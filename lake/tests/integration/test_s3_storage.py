import pytest

pytestmark = pytest.mark.integration


def test_delta_round_trip_on_object_store(spark, s3_config):
    path = s3_config.path("smoke")
    spark.range(10).write.format("delta").save(path)
    spark.range(10, 15).write.format("delta").mode("append").save(path)
    assert spark.read.format("delta").load(path).count() == 15
