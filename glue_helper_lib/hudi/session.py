from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from awsglue.context import GlueContext  # type: ignore


class HudiGlueSession:
    def __init__(self) -> None:
        self.spark_session: SparkSession = self._set_spark()
        self.spark_context = self.spark_session.sparkContext
        self.glue_context = GlueContext(self.spark_context)

    def _set_spark(self) -> SparkSession:
        conf_list = self._get_conf_list()
        spark_conf = SparkConf().setAll(conf_list)
        return (
            SparkSession.builder.config(conf=spark_conf)
            .enableHiveSupport()
            .getOrCreate()
        )

    def _get_conf_list(self):
        return [
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            (
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            ),
            (
                "spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            ),
        ]
