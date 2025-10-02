import json, os, random, logging
from llm import chat

LOG = logging.getLogger()
LOG.setLevel(os.getenv("LOG_LEVEL", "INFO"))

SYSTEM = """You are AnswerBot. You read a question about favorite foods and reply ONLY with a list of exactly three dish names (world cuisines), optionally with 1 short descriptive clause for each. Avoid drinks. No commentary."""
USER_HINT = """Examples of valid outputs:
- "Feijoada — Brazilian stew; Sushi — assorted nigiri and maki; Bibimbap — Korean rice bowl"
- "Biryani; Paella; Moussaka"
Strictly three distinct dishes, preferably from different regions.
"""


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        question = body.get("question") or "Tell me your three favorite foods."
        # Nudge variety a bit via temperature in llm.chat (set in env)
        content = chat([
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": question + "\n" + USER_HINT}
        ], temperature=0.9)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"answer": content})
        }
    except Exception as e:
        LOG.exception("answer-bot error")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
