import pytest
from crypto_producer.config import Settings


def test_defaults_when_env_is_empty():
    s = Settings.from_env({})
    assert s == Settings()
    assert s.products == ("BTC-USD", "ETH-USD", "SOL-USD")
    assert s.topic == "raw.trades"


def test_reads_and_casts_env_vars():
    s = Settings.from_env(
        {
            "KAFKA_BOOTSTRAP": "kafka:29092",
            "KAFKA_TOPIC": "t",
            "PRODUCTS": " BTC-USD , ,ETH-USD ",
            "COINBASE_REST_URL": "http://rest/",
            "MAX_BACKFILL_TRADES": "50",
            "SHUTDOWN_BACKFILL_TIMEOUT": "2.5",
            "log_level": "ignored",
            "LOG_LEVEL": "debug",
        }
    )
    assert s.kafka_bootstrap == "kafka:29092"
    assert s.topic == "t"
    assert s.products == ("BTC-USD", "ETH-USD")
    assert s.rest_url == "http://rest"
    assert s.max_backfill_trades == 50
    assert s.shutdown_backfill_timeout == 2.5
    assert s.log_level == "DEBUG"


def test_blank_values_fall_back_to_defaults():
    s = Settings.from_env({"MAX_BACKFILL_TRADES": "  ", "PRODUCTS": ""})
    assert s.max_backfill_trades == Settings().max_backfill_trades
    assert s.products == Settings().products


@pytest.mark.parametrize(
    ("name", "value"),
    [("MAX_BACKFILL_TRADES", "lots"), ("REST_TIMEOUT", "1s"), ("REST_RETRIES", "-1")],
)
def test_invalid_values_name_the_variable(name, value):
    with pytest.raises(ValueError, match=name):
        Settings.from_env({name: value})
