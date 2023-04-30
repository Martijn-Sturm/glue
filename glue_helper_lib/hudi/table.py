import dataclasses
from glue_helper_lib import table
from glue_helper_lib.hudi import config
import typing
import pyspark.sql
import enum


@dataclasses.dataclass
class Partitioning:
    partition_column: typing.Optional[str]
    is_datetime: bool


@dataclasses.dataclass
class HudiTableArguments:
    storage_location: table.StorageLocation
    catalog: table.GlueCatalogArguments
    index_type: config.IndexType
    table_type: config.TableType
    record_key_colums: typing.List[str]
    precombine_column: str
    partitioning: Partitioning


class WriteMode(enum.Enum):
    OVERWRITE = "overwrite"
    UPSERT = "upsert"


class HudiGlueTable:
    _write_mode_to_mode_string = {
        WriteMode.OVERWRITE: "overwrite",
        WriteMode.UPSERT: "append",
    }

    def __init__(self, table_arguments: HudiTableArguments) -> None:
        self._args = table_arguments
        self._hudi_config = config.get_hudi_options(
            database_name=self._args.catalog.database,
            table_name=self._args.catalog.table,
            hudi_table_path=str(self._args.storage_location),
            table_type=self._args.table_type,
            index_type=self._args.index_type,
            record_key_columns=self._args.record_key_colums,
            precombine_column_name=self._args.precombine_column,
            partition_key_column_name=self._args.partitioning.partition_column,
            partitioned_on_datetime=self._args.partitioning.is_datetime,
        )

    def write(self, df: pyspark.sql.DataFrame, write_mode: WriteMode):
        df.write.format("hudi").options(**self._hudi_config).mode(
            self._write_mode_to_mode_string[write_mode]
        ).save()

    def read(
        self, spark_session: pyspark.sql.SparkSession
    ) -> pyspark.sql.DataFrame:
        return spark_session.read.format("hudi").load(
            str(self._args.storage_location)
        )
