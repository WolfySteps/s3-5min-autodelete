
import os, json, datetime, re
from urllib.parse import unquote_plus

import boto3

scheduler = boto3.client("scheduler")
sts = boto3.client("sts")

DELETER_FUNCTION_NAME = os.environ["DELETER_FUNCTION_NAME"]

def _account_region():
    # region from environment, account via STS
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    acct = sts.get_caller_identity()["Account"]
    return acct, region

def _deleter_arn():
    acct, region = _account_region()
    return f"arn:aws:lambda:{region}:{acct}:function:{DELETER_FUNCTION_NAME}"

def _scheduler_role_arn():
    # CloudFormation output name defined in serverless.yml Outputs
    # Easier: build it directly since we know its name pattern
    acct, region = _account_region()
    role_name = f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME','s3-5min-autodelete')}".split('-scheduleDeleter')[0]  # stack base; not robust
    # Instead, read from SSM for robustness (optional). For simplicity, we reconstruct:
    # service-stage-scheduler-invoke
    service_stage = "-".join(DELETER_FUNCTION_NAME.split("-")[:-1])  # service-stage
    return f"arn:aws:iam::{acct}:role/{service_stage}-scheduler-invoke"

def _safe_name(s):
    # valid schedule name chars: [\.\-_/#A-Za-z0-9]+
    s = re.sub(r'[^A-Za-z0-9\.\-_/#]', '-', s)
    return s[:256]

def handler(event, context):
    # S3 Put event may contain multiple Records
    role_arn = _scheduler_role_arn()
    target_arn = _deleter_arn()

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        # fire time: now + 5 minutes, RFC3339 "at()"
        fire_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=5)).replace(microsecond=0).isoformat() + "Z"

        # Ensure schedule name uniqueness and validity
        cleaned_key = _safe_name(key.replace("/", "-"))
        schedule_name = _safe_name(f"del-{bucket}-{cleaned_key}-{int(datetime.datetime.utcnow().timestamp())}")

        scheduler.create_schedule(
            Name=schedule_name,
            ScheduleExpression=f"at({fire_at})",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": target_arn,
                "RoleArn": role_arn,
                "Input": json.dumps({"bucket": bucket, "key": key})
            }
        )

    return {"ok": True, "count": len(event.get("Records", []))}
