from os import getenv
import boto3

class AWSUtils:
    def __init__(self):
        self.aws_access_key_id = getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = getenv("AWS_DEFAULT_REGION")
        self.bucket_name = getenv("BUCKET_NAME")

    def get_credentials(self):
        return {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
            "AWS_REGION": self.aws_region,
            "BUCKET_NAME": self.bucket_name
        }

    def create_s3_client(self):
        return boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )