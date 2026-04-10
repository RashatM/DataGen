from contextlib import closing
import json
from typing import Any, IO
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import ObjectIdentifierTypeDef

from app.infrastructure.errors import ObjectNotFoundError, ObjectPayloadFormatError


class S3StorageAdapter:
    """Тонкий адаптер над boto3 S3 client для типовых операций чтения и записи объектов проекта."""

    def __init__(self, bucket: str, s3_client: S3Client):
        self.bucket = bucket
        self.s3_client = s3_client

    def build_uri(self, key: str) -> str:
        return f's3a://{self.bucket}/{key.strip("/")}'

    def put_text(self, key: str, content: str) -> str:
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content.encode(),
            ContentType="text/plain; charset=utf-8",
        )
        return self.build_uri(key)

    def put_json(self, key: str, payload: dict[str, Any]) -> str:
        content = json.dumps(payload, indent=2)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content.encode(),
            ContentType="application/json; charset=utf-8",
        )
        return self.build_uri(key)

    def put_bytes(self, key: str, body: bytes) -> str:
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/octet-stream",
        )
        return self.build_uri(key)

    def upload_stream(
        self,
        key: str,
        stream: IO[bytes],
        content_type: str = "application/octet-stream",
    ) -> str:
        """Загружает файловый поток в S3 без промежуточной материализации в bytes."""
        stream.seek(0)
        self.s3_client.upload_fileobj(
            Fileobj=stream,
            Bucket=self.bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
        )
        return self.build_uri(key)

    @staticmethod
    def is_not_found_error(error: ClientError) -> bool:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        return error_code in {"NoSuchKey", "404", "NotFound"}

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if self.is_not_found_error(error):
                raise ObjectNotFoundError(f"Object not found for key={key}") from error
            raise
        with closing(response["Body"]) as stream:
            return stream.read()

    def get_json(self, key: str) -> dict[str, Any]:
        """Читает объект как JSON и гарантирует, что верхний уровень — именно объект, а не список или строка."""
        body = self.get_bytes(key)
        loaded = json.loads(body.decode())
        if not isinstance(loaded, dict):
            raise ObjectPayloadFormatError(
                f"Object payload must be a JSON object for key={key}"
            )
        return loaded

    def delete_prefix(self, prefix: str) -> int:
        """Удаляет все объекты под prefix батчами по 1000 ключей и возвращает число реально удалённых объектов."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        deleted_total = 0

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue

            objects: list[ObjectIdentifierTypeDef] = [{"Key": item["Key"]} for item in contents if "Key" in item]
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

    def close(self) -> None:
        self.s3_client.close()
