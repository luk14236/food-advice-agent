import os, json, logging

from openai import OpenAI

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

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

Return this shape:
{
  "favorite_foods": [
    {
      "name": "string",
      "possible_ingredients": ["string", "..."],
      "diet": "vegetarian|vegan|normal"
    },
    { ... (total 3) }
  ]
}
"""


def get_response(messages):
    general_messages = [
        {
            "role": "system",
            "content": SYSTEM
        }
    ]

    general_messages.extend(messages)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=json.loads(json.dumps(general_messages)),
        response_format={"type": "json_object"},
        max_tokens=300,
        n=1,
        stop=None,
        top_p=1,
        temperature=0
    )

    return response.choices[0].message.content


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")

        answer_text = body.get("answer", "").strip()
        if not answer_text:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Missing 'answer' in body"})
            }

        content = get_response([
            {"role": "user", "content": answer_text}
        ])

        # Best-effort strict JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            data = {"error": "Parser failed to produce valid JSON", "raw": content}

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(data)
        }
    except Exception as e:
        LOG.exception("ask-bot error")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
