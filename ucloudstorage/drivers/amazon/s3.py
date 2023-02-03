import logging
import os

from aiobotocore.session import get_session

from ucloudstorage.base import Driver
from ucloudstorage.drivers.amazon.exceptions import (
    S3FileUploadError,
    S3FileDeleteError,
)

logger = logging.getLogger(__name__)


class AmazonS3(Driver):
    upload_error_msg = "It was not possible to upload the file on S3"
    delete_error_msg = "It was not possible to delete the file on S3"

    def __init__(
        self,
        key: str = None,
        secret: str = None,
        region: str = None,
        bucket: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(key, secret, region, *args, **kwargs)
        self.bucket = bucket
        self.client_params = {
            "region_name": self.region,
            "aws_secret_access_key": self.secret,
            "aws_access_key_id": self.key,
        }
        if os.getenv("LOCALSTACK", True):
            self.client_params["endpoint_url"] = "http://localhost:4566"
        self.base_url = f"https://{self.bucket}.s3.{self.region}.amazonaws.com/"

    async def upload_file(self, file: bytes, file_path: str) -> str:
        session = get_session()
        try:
            async with session.create_client(
                "s3", **self.client_params
            ) as client:
                file_upload_response = await client.put_object(
                    ACL="public-read",
                    Bucket=self.bucket,
                    Key=file_path,
                    Body=file,
                )
                if (
                    file_upload_response["ResponseMetadata"]["HTTPStatusCode"]
                    != 200
                ):
                    raise S3FileUploadError(self.upload_error_msg)
                return f"{self.base_url}{file_path}"
        except Exception:
            raise S3FileUploadError(self.upload_error_msg)

    async def delete_file(self, file_path: str) -> None:
        session = get_session()
        try:
            async with session.create_client(
                "s3", **self.client_params
            ) as client:
                response = await client.delete_object(
                    Bucket=self.bucket, Key=file_path
                )
                if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                    raise S3FileDeleteError(self.delete_error_msg)
        except Exception:
            raise S3FileDeleteError(self.delete_error_msg)
