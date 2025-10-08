import os
import json
import uuid
import logging
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

# --- REQUIRED ENV VARS ---
# Names of the already-deployed lambdas
LAMBDA_GENERATOR_NAME = os.getenv("LAMBDA_GENERATOR_NAME")  # e.g., "answerbot-generator"
LAMBDA_PARSER_NAME = os.getenv("LAMBDA_PARSER_NAME")        # e.g., "answerbot-parser"

# Postgres (RDS) connection settings
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Optional: default question for the generator
DEFAULT_QUESTION = os.getenv("DEFAULT_QUESTION", "Tell me your three favorite foods.")

lambda_client = boto3.client("lambda")


def _invoke_lambda(function_name: str, payload: dict) -> dict:
    """
    Invoke a Lambda function synchronously with a JSON payload.
    Expects the callee to return an API-Gateway-like response:
      { "statusCode": <int>, "body": <JSON string or raw string> }

    Returns the parsed 'body' (as Python object if JSON, otherwise raw string).
    Raises if the callee reports an error (statusCode >= 400) or if payload cannot be decoded.
    """
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    raw = response["Payload"].read().decode("utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        LOG.error("Failed to decode response from lambda %s: %s", function_name, raw)
        raise

    status_code = data.get("statusCode")
    body = data.get("body")

    if status_code and status_code >= 400:
        LOG.error("Lambda %s returned error %s: %s", function_name, status_code, body)
        raise RuntimeError(f"Lambda {function_name} error: {status_code} {body}")

    # 'body' might be a JSON string or a plain string.
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            # It's plain text, return as-is.
            return body
    return body


def _get_generator_answer(question: str) -> str:
    """
    Calls the generator Lambda (first one).
    It expects an event with 'body' containing JSON: {"question": "..."}.
    Returns the plain text with the three dishes (e.g., "X; Y; Z").
    """
    event = {"body": json.dumps({"question": question})}
    body = _invoke_lambda(LAMBDA_GENERATOR_NAME, event)
    if not isinstance(body, str):
        # Force to string just in case the generator changes behavior.
        body = json.dumps(body, ensure_ascii=False)
    return body.strip()


def _parse_answer(answer_text: str) -> dict:
    """
    Calls the parser Lambda (second one).
    It expects an event with 'body' containing JSON: {"answer": "..."}.
    Returns a dict of the form:
    {
      "favorite_foods": [
        {"name": "...", "possible_ingredients": [...], "diet": "..."},
        ...
      ]
    }
    Raises if the format is not as expected.
    """
    event = {"body": json.dumps({"answer": answer_text})}
    body = _invoke_lambda(LAMBDA_PARSER_NAME, event)

    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"Parser returned unexpected text: {body}")

    if "favorite_foods" not in body or not isinstance(body["favorite_foods"], list):
        raise RuntimeError(f"Unexpected parser format: {body}")
    return body


def _connect_pg():
    """
    Open a connection to Postgres using env vars.
    """
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def _insert_batch(conn, rows):
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


def handler(event, context):
    """
    Orchestrator entry point.

    Expected event:
    {
      "runs": 100,                   # number of iterations (required, >0)
      "question": "optional override of the default question",
      "store_source_text": true      # optional (default True) - whether to store the generator's raw text
    }

    Behavior:
    - For each run:
        1) Invoke the generator Lambda to produce 3 dishes (plain text).
        2) Invoke the parser Lambda to convert text -> structured JSON.
        3) Insert 3 rows (one per dish) into Postgres.
    - All rows for this batch share the same 'run_id' (UUID).
    """
    try:
        LOG.info("Orchestrator start")
        body = event if isinstance(event, dict) else {}
        runs = int(body.get("runs") or 0)
        if runs <= 0:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Missing or invalid 'runs' (>0)"}),
            }

        question = body.get("question") or DEFAULT_QUESTION
        store_source_text = body.get("store_source_text", True)

        conn = _connect_pg()

        total_inserted = 0
        all_rows = []
        run_uuid = uuid.uuid4()
        now = datetime.now(timezone.utc)

        for i in range(runs):
            # 1) Generate dishes (plain text)
            answer_text = _get_generator_answer(question)
            LOG.info("Run %d/%d generator -> %s", i + 1, runs, answer_text)

            # 2) Parse into structured JSON
            parsed = _parse_answer(answer_text)
            foods = parsed["favorite_foods"]

            # 3) Prepare rows
            for idx, f in enumerate(foods):
                name = f.get("name", "").strip()
                ingredients = f.get("possible_ingredients", []) or []
                diet = f.get("diet", "normal")
                source_text = answer_text if store_source_text else None

                all_rows.append(
                    (
                        str(run_uuid),   # run_id shared by the whole batch
                        idx,             # 0..2
                        name,
                        ingredients,     # psycopg2 handles list->array
                        diet,
                        source_text,
                        now,
                    )
                )
                total_inserted += 1

            # Flush every 10 runs (3*10 = 30 rows) to keep transactions manageable
            if (i + 1) % 10 == 0:
                _insert_batch(conn, all_rows)
                all_rows.clear()

        # Insert any remaining rows
        if all_rows:
            _insert_batch(conn, all_rows)

        conn.close()

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "ok": True,
                "runs": runs,
                "inserted_rows": total_inserted,
                "run_id": str(run_uuid)
            }),
        }

    except Exception as e:
        LOG.exception("orchestrator error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }