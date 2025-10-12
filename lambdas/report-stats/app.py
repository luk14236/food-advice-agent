import os
import json
import logging
from typing import Any, Dict, Tuple

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

PG_PASSWORD_SECRET_ARN = os.getenv("PG_PASSWORD_SECRET_ARN")
PG_CONN_SECRET_ARN = os.getenv("PG_CONN_SECRET_ARN")

_boto_cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
secrets_client = boto3.client("secretsmanager", config=_boto_cfg)

def _get_password_creds() -> Tuple[str, str]:
    """Return (username, password) from the master password secret."""
    if not PG_PASSWORD_SECRET_ARN:
        raise RuntimeError("PG_PASSWORD_SECRET_ARN is not set.")
    try:
        resp = secrets_client.get_secret_value(SecretId=PG_PASSWORD_SECRET_ARN)
    except ClientError:
        LOG.exception("Failed to read password secret")
        raise
    data = json.loads(resp.get("SecretString") or "{}")
    user = data.get("username")
    pwd = data.get("password")
    if not user or not pwd:
        raise RuntimeError("Password secret missing username/password")
    return user, pwd

def _get_conn_info() -> Dict[str, Any]:
    """Return {"host","port","dbname","username"} from the connection secret."""
    if not PG_CONN_SECRET_ARN:
        raise RuntimeError("PG_CONN_SECRET_ARN is not set.")
    try:
        resp = secrets_client.get_secret_value(SecretId=PG_CONN_SECRET_ARN)
    except ClientError:
        LOG.exception("Failed to read connection secret")
        raise
    data = json.loads(resp.get("SecretString") or "{}")
    for k in ("host","port","dbname","username"):
        if k not in data:
            raise RuntimeError(f"Connection secret missing key: {k}")
    return data

def _connect_pg():
    """Connect using only Secrets Manager (no PG_* env vars)."""
    conn_info = _get_conn_info()
    user_from_pwd, password = _get_password_creds()
    return psycopg2.connect(
        host=conn_info["host"],
        port=int(conn_info["port"]),
        dbname=conn_info["dbname"],
        user=user_from_pwd,
        password=password,
        connect_timeout=10,
    )

def _parse_rows(qs: Dict[str, Any]) -> int:
    if "rows" not in qs:
        raise ValueError("Missing required query parameter 'rows'")
    try:
        n = int(qs["rows"])
    except Exception:
        raise ValueError("'rows' must be an integer")
    if n <= 0:
        raise ValueError("'rows' must be > 0")
    return min(n, 10000)  # safety cap

def handler(event, context):
    """
    GET /reports/stats?rows=N[&strictVeg=true]
    - Take latest N rows (created_at DESC, id DESC).
    - Return:
        1) top_5: most frequent dishes (normalized)
        2) veg_users_count: number of users that match veg criteria
        3) veg_users: list of users (user_id, created_at, foods[3])
    """
    try:
        qs = (event.get("queryStringParameters") or {}) if isinstance(event, dict) else {}
        rows_n = _parse_rows(qs)
        strict_veg = str(qs.get("strictVeg", "false")).lower() == "true"

        conn = _connect_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
               WITH slice AS (
                SELECT favorite_food_id, user_id, name, diet
                FROM favorite_foods
                ORDER BY created_at DESC, favorite_food_id DESC
                LIMIT %s
              ),
              b AS (
                SELECT
                  user_id,
                  LOWER(TRIM(name)) AS norm_name,
                  name,
                  diet
                FROM slice
              ),
              top3 AS (
                SELECT
                  MIN(name) AS name,
                  COUNT(*)  AS count
                FROM b
                GROUP BY norm_name
                ORDER BY count DESC, name ASC
                LIMIT 3
              ),
              veg_users AS (
                SELECT COUNT(DISTINCT user_id) AS n
                FROM b
                WHERE diet IN ('vegetarian','vegan')
              )
              SELECT
                (SELECT JSON_AGG(JSON_BUILD_OBJECT('name', name, 'count', count)
                                ORDER BY count DESC, name ASC)
                  FROM top3) AS top_3,
                (SELECT n FROM veg_users) AS vegetarian_users_count;
                """,
                (rows_n, strict_veg, strict_veg),
            )
            res = cur.fetchone()

        conn.close()

        top5 = res["top5_json"] or []
        veg_users_count = res["veg_users_count"] or 0
        users = res["users_json"] or []

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "rows_input": rows_n,
                "top_5": top5,
                "veg_users_count": veg_users_count,
                "veg_users": users
            }, default=str),
        }

    except ValueError as ve:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(ve)}),
        }
    except Exception as e:
        LOG.exception("report-stats error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }
