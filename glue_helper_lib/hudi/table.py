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


class WriteMode(enum.Enum):
    OVERWRITE = "overwrite"
    UPSERT = "upsert"


@dataclasses.dataclass
class WriteHudiTableArguments:
    catalog: table.GlueCatalogArguments
    index_type: config.IndexType
    table_type: config.TableType
    record_key_colums: typing.List[str]
    precombine_column: str
    partitioning: Partitioning
    write_mode: WriteMode


class HudiGlueTable:
    _write_mode_to_mode_string = {
        WriteMode.OVERWRITE: "overwrite",
        WriteMode.UPSERT: "append",
    }

    def __init__(self, storage_location: table.StorageLocation) -> None:
        self._storage_location = storage_location

    def _get_hudi_config(self, write_args: WriteHudiTableArguments):
        return config.get_hudi_options(
            database_name=write_args.catalog.database,
            table_name=write_args.catalog.table,
            hudi_table_path=str(self._storage_location),
            table_type=write_args.table_type,
            index_type=write_args.index_type,
            record_key_columns=write_args.record_key_colums,
            precombine_column_name=write_args.precombine_column,
            partition_key_column_name=write_args.partitioning.partition_column,
            partitioned_on_datetime=write_args.partitioning.is_datetime,
        )

    def write(
        self, df: pyspark.sql.DataFrame, write_args: WriteHudiTableArguments
    ):
        df.write.format("hudi").options(
            **self._get_hudi_config(write_args)
        ).mode(self._write_mode_to_mode_string[write_args.write_mode]).save()

    def read(
        self, spark_session: pyspark.sql.SparkSession
    ) -> pyspark.sql.DataFrame:
        return spark_session.read.format("hudi").load(
            str(self._storage_location)
        )
