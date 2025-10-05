import os, json, logging

from openai import OpenAI

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM = """You are AnswerBot. You read a question about favorite foods and reply ONLY with a list of exactly three dish names (world cuisines), optionally with 1 short descriptive clause for each. Avoid drinks. No commentary."""
USER_HINT = """Examples of valid outputs:
- "Feijoada — Brazilian stew; Sushi — assorted nigiri and maki; Bibimbap — Korean rice bowl"
- "Biryani; Paella; Moussaka"
Strictly three distinct dishes, preferably from different regions.
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
        messages = json.loads(json.dumps(general_messages)),
        max_tokens=300,
        n=1,
        stop=None,
        top_p = 1,
        temperature=0
    )

    return response.choices[0].message.content


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")

        question = body.get("question") or "Tell me your three favorite foods."

        content = get_response([
            {"role": "user", "content": question}
        ])

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
