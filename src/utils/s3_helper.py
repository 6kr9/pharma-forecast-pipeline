import boto3
import os
import sys
sys.path.append(r"D:\pharma_pipeline")

# LocalStack endpoint — in prod we cam use real AWS
LOCALSTACK_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "pharma-pipeline"

def get_s3_client():
    """Returns S3 client pointing to LocalStack or real AWS."""
    if os.environ.get("USE_LOCALSTACK", "true") == "true":
        return boto3.client(
            "s3",
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
    else:
        # Real AWS — uses environment credentials
        return boto3.client("s3")


def upload_file_to_s3(local_path: str, s3_key: str) -> None:
    """Upload a single file to S3."""
    s3 = get_s3_client()
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"Uploaded: {local_path} → s3://{BUCKET_NAME}/{s3_key}")


def upload_forecast_to_s3() -> None:
    """Upload forecast CSV output to S3."""
    local_path = r"D:\pharma_pipeline\outputs\sales_forecast.csv"
    s3_key = "outputs/sales_forecast.csv"
    upload_file_to_s3(local_path, s3_key)


def list_s3_files(prefix: str = "") -> None:
    """List all files in the bucket."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f"  {obj['Key']} ({obj['Size']} bytes)")
    else:
        print("No files found")


if __name__ == "__main__":
    print("Testing S3 connection...")
    print("\nFiles in bucket before upload:")
    list_s3_files()

    print("\nUploading forecast...")
    upload_forecast_to_s3()

    print("\nFiles in bucket after upload:")
    list_s3_files()