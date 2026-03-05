from enum import Enum


class StorageType(Enum):
    S3 = "s3"
    HADOOP = "hadoop"

    def __str__(self):
        return self.value
