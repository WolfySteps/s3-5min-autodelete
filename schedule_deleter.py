import os, json, datetime, re
from urllib.parse import unquote_plus
import boto3

scheduler = boto3.client("scheduler")

DELETER_FUNCTION_NAME = os.environ["DELETER_FUNCTION_NAME"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]

def _safe_name(s):
    return re.sub(r'[^A-Za-z0-9.\-_/#]', '-', s)[:256]

def _deleter_arn():
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    account = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:aws:lambda:{region}:{account}:function:{DELETER_FUNCTION_NAME}"

def handler(event, context):
    target_arn = _deleter_arn()

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        fire_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=3)).replace(microsecond=0).isoformat() + "Z"

        cleaned_key = _safe_name(key.replace("/", "-"))
        schedule_name = _safe_name(f"del-{bucket}-{cleaned_key}-{int(datetime.datetime.utcnow().timestamp())}")

        scheduler.create_schedule(
            Name=schedule_name,
            ScheduleExpression=f"at({fire_at})",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": target_arn,
                "RoleArn": SCHEDULER_ROLE_ARN,
                "Input": json.dumps({"bucket": bucket, "key": key})
            }
        )

    return {"ok": True, "count": len(event.get("Records", []))}
