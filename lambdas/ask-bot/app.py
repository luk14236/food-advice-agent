import json, os, logging
from llm import chat

LOG = logging.getLogger()
LOG.setLevel(os.getenv("LOG_LEVEL", "INFO"))

SYSTEM = """You are AskBot, a strict parser for favorite foods.
Input: a short free-text list of dishes.
Task:
1) If the input is not foods (e.g., places, drinks, jokes), return:
   {"error":"Invalid answer. Provide only food dishes."}
2) Otherwise, extract EXACTLY 3 foods.
3) For each, output:
   - name (string)
   - possible_ingredients (array of 4-10 strings, lowercase, generic/comma-free)
   - diet (enum: "vegetarian" | "vegan" | "normal") — classify typical version.
Return ONLY valid JSON, no commentary.
"""

SCHEMA_HINT = """Return this shape:
{
  "favorite_foods": [
    {
      "name": "string",
      "possible_ingredients": ["string", "..."],
      "diet": "vegetarian|vegan|normal"
    },
    { ... (total 3) }
  ]
}"""


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        answer_text = body.get("answer", "").strip()
        if not answer_text:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'answer' in body"})}

        content = chat([
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": f"Input:\n{answer_text}\n\n{SCHEMA_HINT}"}
        ], model=os.getenv("OPENAI_MODEL_PARSE", "gpt-4o-mini"), temperature=0)

        # Best-effort strict JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            data = {"error": "Parser failed to produce valid JSON", "raw": content}

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(data)
        }
    except Exception as e:
        LOG.exception("ask-bot error")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
