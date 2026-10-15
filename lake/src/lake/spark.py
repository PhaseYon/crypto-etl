"""SparkSession factory: Delta Lake + S3A configured from LakeConfig.

Switching from local MinIO/RustFS to AWS S3 only changes environment variables:
leave S3_ENDPOINT empty and either set AWS keys or rely on the default AWS
credentials chain (IAM role, instance profile, ...).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from lake.config import LakeConfig


def spark_conf(config: LakeConfig, master: str = "local[*]") -> dict[str, str]:
    conf = {
        "spark.master": master,
        "spark.driver.memory": config.driver_memory,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(config.shuffle_partitions),
        # Size Delta log replay and file listing (used by VACUUM) for one node; the defaults
        # (50 and 10000 tasks) are meant for clusters and dominate runtime in local mode
        "spark.databricks.delta.snapshotPartitions": str(config.shuffle_partitions),
        "spark.sql.sources.parallelPartitionDiscovery.parallelism": str(config.shuffle_partitions * 4),
        "spark.ui.showConsoleProgress": "false",
        # Compact small files right after each write, inside the writing job (safe on S3,
        # unlike a second process committing to the same table)
        "spark.databricks.delta.autoCompact.enabled": "true",
        # S3A: retries and timeouts for every object store call
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint.region": config.aws_region,
        "spark.hadoop.fs.s3a.retry.limit": "7",
        "spark.hadoop.fs.s3a.retry.interval": "500ms",
        "spark.hadoop.fs.s3a.attempts.maximum": "5",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "30s",
        "spark.hadoop.fs.s3a.connection.timeout": "200s",
        "spark.hadoop.fs.s3a.fast.upload": "true",
    }
    if config.s3_endpoint:
        # S3-compatible server (RustFS/MinIO): custom endpoint and path-style bucket URLs
        conf["spark.hadoop.fs.s3a.endpoint"] = config.s3_endpoint
        conf["spark.hadoop.fs.s3a.path.style.access"] = "true"
        conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(config.s3_endpoint.startswith("https")).lower()
    if config.aws_access_key_id and config.aws_secret_access_key:
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        conf["spark.hadoop.fs.s3a.access.key"] = config.aws_access_key_id
        conf["spark.hadoop.fs.s3a.secret.key"] = config.aws_secret_access_key
    return conf


def build_spark(
    app_name: str, config: LakeConfig, master: str = "local[*]", extra: dict[str, str] | None = None
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in {**spark_conf(config, master), **(extra or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
