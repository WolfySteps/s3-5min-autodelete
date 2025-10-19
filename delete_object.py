import boto3
s3 = boto3.client("s3")

def handler(event, context):
    bucket = event["bucket"]
    key = event["key"]
    s3.delete_object(Bucket=bucket, Key=key)
    return {"deleted": f"s3://{bucket}/uploads/{key}"}

