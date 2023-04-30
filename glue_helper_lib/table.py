import dataclasses
import typing
import pyspark.sql


class S3Uri:
    def __init__(self, path: str) -> None:
        self._path = path
        if not path.startswith("s3://"):
            raise ValueError("path shouldstart with 's3://'")

    def __str__(self) -> str:
        return self._path


class GlueTable(typing.Protocol):
    def write(self, df: pyspark.sql.DataFrame):
        ...

    def read(
        self, spark_session: pyspark.sql.SparkSession
    ) -> pyspark.sql.DataFrame:
        ...


@dataclasses.dataclass
class GlueCatalogArguments:
    database: str
    table: str


class StorageLocation:
    def __init__(self, path: S3Uri):
        self._path = path

    def __str__(self) -> str:
        return str(self._path)
