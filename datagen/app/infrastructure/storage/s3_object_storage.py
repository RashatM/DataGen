import json
from typing import Any, Dict

from botocore.exceptions import ClientError
from botocore.client import BaseClient


from app.infrastructure.errors import ObjectNotFoundError, ObjectPayloadFormatError
from app.infrastructure.ports.object_storage_port import IObjectStorage


class S3StorageAdapter(IObjectStorage):

    def __init__(self, bucket: str, prefix: str, s3_client: BaseClient):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3_client = s3_client

    def build_key(self, relative_key: str) -> str:
        normalized = relative_key.strip("/")
        if self.prefix:
            return f"{self.prefix}/{normalized}"
        return normalized

    def generate_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def put_text(self, key: str, content: str) -> str:
        full_key = self.build_key(key)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
        return self.generate_uri(full_key)

    def put_json(self, key: str, payload: Dict[str, Any]) -> str:
        content = json.dumps(payload, ensure_ascii=True, indent=2)
        full_key = self.build_key(key)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=content.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
        return self.generate_uri(full_key)

    def put_bytes(self, key: str, body: bytes) -> str:
        full_key = self.build_key(key)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=body,
            ContentType="application/octet-stream",
        )
        return self.generate_uri(full_key)

    def build_uri(self, key: str) -> str:
        full_key = self.build_key(key)
        return self.generate_uri(full_key)

    @staticmethod
    def is_not_found_error(error: ClientError) -> bool:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        return error_code in {"NoSuchKey", "404", "NotFound"}

    def get_bytes(self, key: str) -> bytes:
        full_key = self.build_key(key)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=full_key)
        except ClientError as error:
            if self.is_not_found_error(error):
                raise ObjectNotFoundError(f"Object not found for key={full_key}") from error
            raise
        body = response["Body"].read()
        return body

    def get_json(self, key: str) -> Dict[str, Any]:
        body = self.get_bytes(key)
        loaded = json.loads(body.decode("utf-8"))
        if not isinstance(loaded, dict):
            raise ObjectPayloadFormatError(
                f"Object payload must be a JSON object for key={key}"
            )
        return loaded
