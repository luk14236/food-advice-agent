import os
import json
import logging
from uuid import uuid4
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import boto3
import psycopg2
from psycopg2.extras import execute_values
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

# --- Required env vars injected by SAM template ---
# Child lambdas to invoke
LAMBDA_GENERATOR_NAME = os.getenv("LAMBDA_GENERATOR_NAME")  # e.g., "answerbot-generator"
LAMBDA_PARSER_NAME = os.getenv("LAMBDA_PARSER_NAME")        # e.g., "answerbot-parser"

# Secrets (use Secrets Manager ONLY; no PG_HOST/PG_DB envs)
PG_PASSWORD_SECRET_ARN = os.getenv("PG_PASSWORD_SECRET_ARN")  # {"username","password"}
PG_CONN_SECRET_ARN = os.getenv("PG_CONN_SECRET_ARN")          # {"host","port","dbname","username"}

# Optional: default question for the generator
DEFAULT_QUESTION = os.getenv("DEFAULT_QUESTION", "Tell me your three favorite foods.")

# Reuse clients; add small retries for transient AWS API hiccups
_boto_cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
lambda_client = boto3.client("lambda", config=_boto_cfg)
secrets_client = boto3.client("secretsmanager", config=_boto_cfg)


# --------------------------
# Helpers: AWS/Lambda calls
# --------------------------
def _invoke_lambda(function_name: str, payload: Dict[str, Any]) -> Any:
    """
    Invoke a Lambda function synchronously with a JSON payload.
    Expects an API-Gateway-like response:
      { "statusCode": <int>, "body": <JSON string or raw string> }
    Returns the parsed 'body' (Python object if JSON, otherwise raw string).
    Raises for non-2xx status codes or malformed payloads.
    """
    try:
        resp = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except ClientError:
        LOG.exception("Failed to invoke lambda %s", function_name)
        raise

    raw = resp["Payload"].read().decode("utf-8")
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError:
        LOG.error("Invalid JSON envelope from %s: %s", function_name, raw)
        raise

    status = envelope.get("statusCode")
    body = envelope.get("body")

    if status and int(status) >= 400:
        LOG.error("Lambda %s returned error %s: %s", function_name, status, body)
        raise RuntimeError(f"Lambda {function_name} error: {status} {body}")

    # 'body' might be a JSON string or a plain string.
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body  # plain text, return as-is
    return body


def _get_generator_answer(question: str) -> str:
    """
    Call the generator lambda with {"question": "..."}.
    Returns plain text like "Dish A; Dish B; Dish C".
    """
    event = {"body": json.dumps({"question": question})}
    body = _invoke_lambda(LAMBDA_GENERATOR_NAME, event)
    if not isinstance(body, str):
        # Force to string just in case the generator changes behavior.
        body = json.dumps(body, ensure_ascii=False)
    return body.strip()


def _parse_answer(answer_text: str) -> Dict[str, Any]:
    """
    Call the parser lambda with {"answer": "..."}.
    Returns a dict of the form:
      {"favorite_foods": [{"name": "...","possible_ingredients":[...],"diet":"..."} * 3]}
    Raises if the format is not as expected.
    """
    event = {"body": json.dumps({"answer": answer_text})}
    body = _invoke_lambda(LAMBDA_PARSER_NAME, event)

    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"Parser returned unexpected text: {body}")

    foods = body.get("favorite_foods")
    if not isinstance(foods, list):
        raise RuntimeError(f"Unexpected parser format: {body}")
    if len(foods) != 3:
        raise RuntimeError(f"Parser returned {len(foods)} items (expected 3): {foods}")
    return body


# --------------------------
# Helpers: Secrets & Postgres
# --------------------------
def _get_password_creds() -> Tuple[str, str]:
    """
    Read {"username": "...", "password": "..."} from PG_PASSWORD_SECRET_ARN.
    """
    if not PG_PASSWORD_SECRET_ARN:
        raise RuntimeError("PG_PASSWORD_SECRET_ARN is not set.")
    try:
        resp = secrets_client.get_secret_value(SecretId=PG_PASSWORD_SECRET_ARN)
    except ClientError:
        LOG.exception("Failed to read secret %s", PG_PASSWORD_SECRET_ARN)
        raise
    data = json.loads(resp.get("SecretString") or "{}")
    missing = [k for k in ("username", "password") if k not in data]
    if missing:
        raise RuntimeError(f"Password secret missing keys: {missing}")
    return data["username"], data["password"]


def _get_conn_info() -> Dict[str, Any]:
    """
    Read {"host": "...", "port": 5432, "dbname": "...", "username": "..."} from PG_CONN_SECRET_ARN.
    """
    if not PG_CONN_SECRET_ARN:
        raise RuntimeError("PG_CONN_SECRET_ARN is not set.")
    try:
        resp = secrets_client.get_secret_value(SecretId=PG_CONN_SECRET_ARN)
    except ClientError:
        LOG.exception("Failed to read secret %s", PG_CONN_SECRET_ARN)
        raise
    data = json.loads(resp.get("SecretString") or "{}")
    missing = [k for k in ("host", "port", "dbname", "username") if k not in data]
    if missing:
        raise RuntimeError(f"Connection secret missing keys: {missing}")
    return data


def _connect_pg():
    """
    Open a Postgres connection using ONLY Secrets Manager (no PG_* env vars).
    """
    conn_info = _get_conn_info()
    user_from_pwd, password = _get_password_creds()

    # Optional sanity: prefer the username from password secret.
    user = user_from_pwd

    return psycopg2.connect(
        host=conn_info["host"],
        port=int(conn_info["port"]),
        dbname=conn_info["dbname"],
        user=user,
        password=password,
        connect_timeout=10,
    )


def _insert_batch(conn, rows: List[Tuple[Any, ...]]) -> None:
    """
    Bulk insert rows into the 'favorite_foods' table with user linkage.
    rows: list of tuples -> (user_id, name, possible_ingredients(list), diet, created_at)
    """
    if not rows:
        return
    sql = """
    INSERT INTO favorite_foods
      (user_id, name, possible_ingredients, diet, created_at)
    VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


# ---------------
# Lambda handler
# ---------------
def handler(event, context):
    """
    Accepts:
    - API Gateway proxy: event["body"] is a JSON string like '{"runs":10}'
    - Direct invoke / tests: event is already a dict like {"runs":10}
    """
    try:
        LOG.info("Orchestrator start")

        # Robust body parsing
        raw = {}
        if isinstance(event, dict):
            if "body" in event:
                b = event.get("body")
                if isinstance(b, str):
                    raw = json.loads(b or "{}")
                elif isinstance(b, dict):
                    raw = b
            else:
                raw = event
        else:
            raw = {}

        runs = int(raw.get("runs") or 0)
        if runs <= 0:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Missing or invalid 'runs' (>0)"}),
            }

        question = raw.get("question") or DEFAULT_QUESTION

        conn = _connect_pg()

        total_inserted = 0
        pending_rows: List[Tuple[Any, ...]] = []
        now = datetime.now(timezone.utc)

        for i in range(runs):
            # 1) Generate three dish names (plain text)
            answer_text = _get_generator_answer(question)
            LOG.info("Run %d/%d generator -> %s", i + 1, runs, answer_text)

            # 2) Parse into structured JSON
            parsed = _parse_answer(answer_text)
            foods = parsed["favorite_foods"]

            # 3) One user_id per iteration (3 rows per user)
            user_id = str(uuid4())

            for f in foods:
                name = (f.get("name") or "").strip()
                ingredients = f.get("possible_ingredients", []) or []
                diet = f.get("diet", "normal")
                pending_rows.append((user_id, name, ingredients, diet, now))
                total_inserted += 1

            # Flush every 10 runs (3*10 = 30 rows) to keep transactions reasonable
            if (i + 1) % 10 == 0:
                _insert_batch(conn, pending_rows)
                pending_rows.clear()

        # Final flush
        if pending_rows:
            _insert_batch(conn, pending_rows)

        conn.close()

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "runs": runs, "inserted_rows": total_inserted}),
        }

    except Exception as e:
        LOG.exception("orchestrator error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }
