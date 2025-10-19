
import boto3
s3 = boto3.client("s3")

def handler(event, context):
    bucket = event["bucket"]
    key = event["key"]
    # If bucket has versioning and you only want to delete the latest version,
    # this is enough. To hard-delete all versions, you'd list_object_versions and loop.
    s3.delete_object(Bucket=bucket, Key=key)
    return {"deleted": f"s3://{bucket}/{key}"}
