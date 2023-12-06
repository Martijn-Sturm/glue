import pyspark.sql.types as spark_types
import pyspark.sql.functions as spark_funcs
from glue_helper_lib.hudi.table import (
    HudiGlueTable,
    WriteHudiTableArguments,
    WriteMode,
)
from glue_helper_lib.hudi.config import IndexType, TableType
from glue_helper_lib.table import StorageLocation, S3Uri, GlueCatalogArguments
from glue_helper_lib.hudi import session
from glue_helper_lib import arguments
from glue_helper_lib import logging
import dataclasses
import datetime

logger = logging.Logger("create-table", logging.LogLevel.DEBUG)

glue_session = session.HudiGlueSession()

SCHEMA = spark_types.StructType(
    [
        spark_types.StructField("id1", spark_types.StringType(), True),
        spark_types.StructField("id2", spark_types.StringType(), True),
        spark_types.StructField("col_string", spark_types.StringType(), True),
        # spark_types.StructField(
        #     "unique_record_identifier", spark_types.StringType(), True
        # ),
    ]
)

date_time = datetime.datetime.now()

df = glue_session.get_spark_session().createDataFrame(
    [
        [1, 1, "a"],
        [2, 1, "b"],
    ],
    SCHEMA,
)

timestamp_colname = "timestamp"

df = df.withColumn(timestamp_colname, spark_funcs.lit(date_time))
df = df.withColumn(
    timestamp_colname,
    spark_funcs.to_timestamp(timestamp_colname),
)


@dataclasses.dataclass
class JobArguments(arguments.Arguments):
    database_name: str
    table_name: str
    storage_path: str


job_arguments = JobArguments.from_glue_arguments()
logger.info("job arguments: %s", job_arguments)

table_uri = S3Uri(job_arguments.storage_path)
logger.info("table uri / path: %s", table_uri)

catalog = GlueCatalogArguments(
    database=job_arguments.database_name, table=job_arguments.table_name
)

demo_table = HudiGlueTable(
    storage_location=StorageLocation(table_uri),
)

write_args = WriteHudiTableArguments(
    catalog=catalog,
    index_type=IndexType.GLOBAL_SIMPLE,
    table_type=TableType.COPY_ON_WRITE,
    record_key_colums=["id1", "id2"],
    precombine_column=timestamp_colname,
    partitioning=None,
    write_mode=WriteMode.UPSERT,
)
logger.info("going to write dataframe with write args: %s", write_args)
demo_table.write(df, write_args)

logger.info("written dataframe")
