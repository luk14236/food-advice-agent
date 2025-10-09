import os
import json
import uuid
import logging
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

# Postgres connection (host/port/db) + secret for credentials
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_SECRET_ARN = os.getenv("PG_SECRET_ARN")                  # Secrets Manager ARN with {"username","password"}

# Optional: default question to pass to the generator
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
    except ClientError as e:
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

    # 'body' can be a JSON string or plain text
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
        # Force to string to tolerate future changes
        body = json.dumps(body, ensure_ascii=False)
    return body.strip()


def _parse_answer(answer_text: str) -> Dict[str, Any]:
    """
    Call the parser lambda with {"answer": "..."}.
    Returns a dict:
      {"favorite_foods": [{"name": "...","possible_ingredients":[...],"diet":"..."} * 3]}
    Validates the expected shape.
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
        raise RuntimeError(f"Unexpected parser format (missing list): {body}")
    if len(foods) != 3:
        raise RuntimeError(f"Parser returned {len(foods)} items (expected 3): {foods}")

    return body


# --------------------------
# Helpers: Secrets & Postgres
# --------------------------
def _get_db_creds() -> Tuple[str, str]:
    """
    Read {"username": "...", "password": "..."} from Secrets Manager.
    """
    if not PG_SECRET_ARN:
        raise RuntimeError("PG_SECRET_ARN is not set.")
    try:
        resp = secrets_client.get_secret_value(SecretId=PG_SECRET_ARN)
    except ClientError:
        LOG.exception("Failed to read secret %s", PG_SECRET_ARN)
        raise

    blob = resp.get("SecretString") or "{}"
    data = json.loads(blob)
    try:
        return data["username"], data["password"]
    except KeyError:
        raise RuntimeError(f"Secret {PG_SECRET_ARN} missing username/password keys")


def _connect_pg():
    """
    Open a Postgres connection using host/port/db and creds from Secrets Manager.
    """
    if not (PG_HOST and PG_DB):
        raise RuntimeError("PG_HOST and PG_DB must be set via environment variables.")
    user, password = _get_db_creds()
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=user,
        password=password,
        connect_timeout=10,
    )


def _insert_batch(conn, rows: List[Tuple[Any, ...]]) -> None:
    """
    Bulk insert rows into the 'favorite_foods' table.

    rows: list of tuples
      (run_id, dish_index, name, possible_ingredients(list), diet, source_text, created_at)
    """
    sql = """
    INSERT INTO favorite_foods
      (run_id, dish_index, name, possible_ingredients, diet, source_text, created_at)
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
    Orchestrator entry point.

    Expected event:
    {
      "runs": 100,                   # number of iterations (required, >0)
      "question": "optional override of the default question",
      "store_source_text": true      # optional (default True) - whether to store raw generator text
    }

    Behavior per run:
      1) Invoke the generator (plain text with 3 dishes)
      2) Invoke the parser (structured JSON with exactly 3 items)
      3) Insert 3 rows into Postgres

    All rows in this invocation share the same 'run_id' (UUID).
    """
    try:
        LOG.info("Orchestrator start")
        body = event if isinstance(event, dict) else {}
        runs = int(body.get("runs") or 0)
        if runs <= 0:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Missing or invalid 'runs' (>0)"}),
            }

        question = body.get("question") or DEFAULT_QUESTION
        store_source_text = body.get("store_source_text", True)

        conn = _connect_pg()

        total_inserted = 0
        pending_rows: List[Tuple[Any, ...]] = []
        run_uuid = uuid.uuid4()
        now = datetime.now(timezone.utc)

        for i in range(runs):
            # 1) Generate three dish names (plain text)
            answer_text = _get_generator_answer(question)
            LOG.info("Run %d/%d generator -> %s", i + 1, runs, answer_text)

            # 2) Parse into structured JSON
            parsed = _parse_answer(answer_text)
            foods = parsed["favorite_foods"]

            # 3) Accumulate rows for batch insert
            for idx, f in enumerate(foods):
                name = (f.get("name") or "").strip()
                ingredients = f.get("possible_ingredients", []) or []
                diet = f.get("diet", "normal")
                source_text = answer_text if store_source_text else None

                pending_rows.append(
                    (
                        str(run_uuid),  # same run_id for this whole invocation
                        idx,            # 0..2
                        name,
                        ingredients,    # psycopg2 converts list -> text[]
                        diet,
                        source_text,
                        now,
                    )
                )
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
            "body": json.dumps(
                {"ok": True, "runs": runs, "inserted_rows": total_inserted, "run_id": str(run_uuid)}
            ),
        }

    except Exception as e:
        LOG.exception("orchestrator error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }
