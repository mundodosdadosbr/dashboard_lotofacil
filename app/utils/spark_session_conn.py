import pyspark.sql
from pyspark.sql import SparkSession
from delta import *


class SparkSessionConn:

    @staticmethod
    def session_spark(appName):
        spark = (
            SparkSession.builder
            .master("spark://spark-master:7077")
            .appName(appName)
            .config('spark.sql.debug.maxToStringFields', 5000)
            .config('spark.debug.maxToStringFields', 5000)
            .config("delta.autoOptimize.optimizeWrite", "true")
            .config("delta.autoOptimize.autoCompact", "true")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "datalake")
            .config("spark.hadoop.fs.s3a.secret.key", "datalake")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark

    @staticmethod
    def build_delta_minio(appName):
        builder = pyspark.sql.SparkSession.builder.appName(appName).master("spark://spark-master:7077") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("delta.autoOptimize.optimizeWrite", "true") \
            .config("delta.autoOptimize.autoCompact", "true") \
            .config("spark.hadoop.fs.s3a.access.key", "datalake") \
            .config("spark.hadoop.fs.s3a.secret.key", "datalake") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
        return spark
