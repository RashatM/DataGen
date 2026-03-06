import json
from typing import Any, Dict

from botocore.exceptions import ClientError
from botocore.client import BaseClient


from app.infrastructure.errors import ObjectNotFoundError, ObjectPayloadFormatError
from app.core.application.ports.object_storage_port import IObjectStorage


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

    def delete_prefix(self, prefix: str) -> int:
        full_prefix = self.build_key(prefix)
        paginator = self.s3_client.get_paginator("list_objects_v2")
        deleted_total = 0

        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue

            objects = [{"Key": item["Key"]} for item in contents if "Key" in item]
            if not objects:
                continue

            for offset in range(0, len(objects), 1000):
                chunk = objects[offset: offset + 1000]
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": chunk, "Quiet": True},
                )

                errors = response.get("Errors", [])
                if errors:
                    first_error = errors[0]
                    key = first_error.get("Key", "")
                    message = first_error.get("Message", "")
                    raise RuntimeError(f"Failed to delete object key={key}: {message}")

                deleted_total += len(response.get("Deleted", []))

        return deleted_total
