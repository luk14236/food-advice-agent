import json, os, logging, urllib.request, urllib.parse

LOG = logging.getLogger()
LOG.setLevel(os.getenv("LOG_LEVEL", "INFO"))

ANSWER_URL = os.getenv("ANSWER_URL")
ASK_URL = os.getenv("ASK_URL")

QUESTION_TEXT = "Please list your three favorite foods. Only food dishes (no drinks), can be very complex dishes."


def _post(url, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def handler(event, context):
    try:
        # read rounds from querystring or body
        rounds = 1
        if "queryStringParameters" in event and event["queryStringParameters"]:
            rounds = int(event["queryStringParameters"].get("n", "1"))
        if event.get("body"):
            body = json.loads(event["body"])
            rounds = int(body.get("n", rounds))
        rounds = max(1, min(rounds, 10))  # cap to 10

        transcript = []
        last_structured = None

        for i in range(rounds):
            # AnswerBot
            a = _post(ANSWER_URL, {"question": QUESTION_TEXT})
            answer_text = a.get("answer", "").strip()
            transcript.append({"role": "answer-bot", "content": answer_text})

            # AskBot parser
            s = _post(ASK_URL, {"answer": answer_text})
            transcript.append({"role": "ask-bot", "content": s})
            last_structured = s

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "rounds": rounds,
                "result": last_structured,
                "transcript": transcript
            })
        }
    except Exception as e:
        LOG.exception("orchestrator error")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
