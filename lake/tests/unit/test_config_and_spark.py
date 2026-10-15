from lake.config import LakeConfig
from lake.spark import spark_conf


def test_paths_follow_contract():
    config = LakeConfig.from_env({"LAKE_BUCKET": "my-bucket"})
    assert config.bronze_trades == "s3a://my-bucket/bronze/trades"
    assert config.silver_trades == "s3a://my-bucket/silver/trades"
    assert config.silver_quarantine == "s3a://my-bucket/silver/trades_quarantine"
    assert config.gold_ohlcv_1m == "s3a://my-bucket/gold/ohlcv_1m"
    assert config.ops_trade_gaps == "s3a://my-bucket/ops/trade_gaps"
    assert config.checkpoint("bronze_trades") == "s3a://my-bucket/_checkpoints/bronze_trades"


def test_lake_root_override_wins():
    config = LakeConfig.from_env({"LAKE_BUCKET": "ignored", "LAKE_ROOT": "/tmp/lake/"})
    assert config.bronze_trades == "/tmp/lake/bronze/trades"


def test_s3_compatible_endpoint_uses_path_style_and_keys():
    config = LakeConfig.from_env(
        {"S3_ENDPOINT": "http://minio:9000", "AWS_ACCESS_KEY_ID": "id", "AWS_SECRET_ACCESS_KEY": "secret"}
    )
    conf = spark_conf(config)
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "id"


def test_real_s3_uses_default_endpoint_and_credentials_chain():
    conf = spark_conf(LakeConfig.from_env({"AWS_REGION": "eu-west-1"}))
    assert "spark.hadoop.fs.s3a.endpoint" not in conf
    assert "spark.hadoop.fs.s3a.access.key" not in conf
    assert "spark.hadoop.fs.s3a.aws.credentials.provider" not in conf
    assert conf["spark.hadoop.fs.s3a.endpoint.region"] == "eu-west-1"


def test_delta_round_trip(spark, local_config):
    path = local_config.path("smoke")
    spark.range(5).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 5
