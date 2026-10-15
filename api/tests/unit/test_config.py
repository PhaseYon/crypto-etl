from crypto_api.config import Settings
from pydantic import SecretStr


def test_minio_storage_options():
    settings = Settings(
        s3_endpoint="http://minio:9000",
        aws_access_key_id=SecretStr("user"),
        aws_secret_access_key=SecretStr("secret"),
        source_timeout_seconds=4,
    )
    options = settings.storage_options()
    assert settings.lake_root == "s3://crypto-lake"
    assert options["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert options["AWS_ALLOW_HTTP"] == "true"
    assert options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] == "false"
    assert options["AWS_ACCESS_KEY_ID"] == "user"
    assert options["timeout"] == "4s"


def test_real_aws_has_no_endpoint_override():
    options = Settings(s3_endpoint=None).storage_options()
    assert "AWS_ENDPOINT_URL" not in options
    assert "AWS_ACCESS_KEY_ID" not in options  # falls back to the default AWS credential chain


def test_local_lake_uri_needs_no_options():
    settings = Settings(lake_uri="/tmp/lake/")
    assert settings.lake_root == "/tmp/lake"
    assert settings.storage_options() == {}


def test_products_and_cors_from_env(monkeypatch):
    monkeypatch.setenv("PRODUCTS", " BTC-USD, ETH-USD ,")
    monkeypatch.setenv("CORS_ORIGINS", "http://localhost:15173")
    settings = Settings()
    assert settings.products == ["BTC-USD", "ETH-USD"]
    assert settings.cors_origins == ["http://localhost:15173"]


def test_secrets_not_in_repr():
    settings = Settings(aws_secret_access_key=SecretStr("hunter22"))
    assert "hunter22" not in repr(settings)
