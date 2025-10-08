import os, json, logging

from openai import OpenAI

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM = """You are AnswerBot.
Return exactly three distinct world dishes (no drinks). Prefer different regions each time.
Vary your choices across calls; avoid commonly overused examples like Sushi, Pizza, Pasta, Biryani, Paella, Moussaka, Feijoada, Bibimbap.
Reply as plain text with dishes separated by semicolons. No commentary."""


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
        max_tokens=120,
        n=1,
        stop=None,
        top_p=0.95,
        temperature=0.8
    )

    return response.choices[0].message.content


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")

        question = body.get("question") or "Tell me your three favorite foods."

        content = get_response([
            {"role": "user", "content": question}
        ])

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(content)
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
