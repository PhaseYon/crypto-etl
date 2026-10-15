import json

import pytest
from crypto_producer import backfill as cli
from crypto_producer.config import Settings
from crypto_producer.gaps import BackfillJob
from fakes import FakeProducer, make_fetch


def test_explicit_range_args():
    args = cli.parse_args(["--product", "BTC-USD", "--from-id", "5", "--to-id", "9"])
    assert (args.product, args.from_id, args.to_id, args.gaps_file) == ("BTC-USD", 5, 9, None)


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--product", "BTC-USD", "--from-id", "5"],
        ["--product", "BTC-USD", "--from-id", "9", "--to-id", "5"],
        ["--gaps-file", "g.csv", "--product", "BTC-USD"],
    ],
)
def test_invalid_args_exit_with_usage_error(argv):
    with pytest.raises(SystemExit) as exc:
        cli.parse_args(argv)
    assert exc.value.code == 2


ROWS = [
    {"product_id": "BTC-USD", "gap_start_id": 101, "gap_end_id": 110, "missing_count": 10},
    {"product_id": "ETH-USD", "gap_start_id": 5, "gap_end_id": 5, "missing_count": 1},
]


def expected_jobs():
    return [BackfillJob.for_range("BTC-USD", 101, 110), BackfillJob.for_range("ETH-USD", 5, 5)]


def test_gaps_file_csv(tmp_path):
    path = tmp_path / "gaps.csv"
    path.write_text("product_id,gap_start_id,gap_end_id,missing_count\nBTC-USD,101,110,10\nETH-USD,5,5,1\n")
    assert cli.jobs_from_gaps_file(path) == expected_jobs()


def test_gaps_file_json_array(tmp_path):
    path = tmp_path / "gaps.json"
    path.write_text(json.dumps(ROWS))
    assert cli.jobs_from_gaps_file(path) == expected_jobs()


def test_gaps_file_json_lines(tmp_path):
    path = tmp_path / "part-0000.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in ROWS) + "\n")
    assert cli.jobs_from_gaps_file(path) == expected_jobs()


def test_gaps_file_bad_row_names_the_row(tmp_path):
    path = tmp_path / "gaps.csv"
    path.write_text("product_id,gap_start_id,gap_end_id\nBTC-USD,1,2\nBTC-USD,9,oops\n")
    with pytest.raises(ValueError, match="row 2"):
        cli.jobs_from_gaps_file(path)


def test_main_returns_2_for_missing_file(tmp_path):
    assert cli.main(["--gaps-file", str(tmp_path / "nope.csv")]) == 2


@pytest.mark.asyncio
async def test_unit_tests_cannot_reach_a_real_broker():
    with pytest.raises(RuntimeError, match="real Kafka producer"):
        await cli.backfill_jobs(Settings(), [])


@pytest.mark.asyncio
async def test_backfill_jobs_fills_every_range(monkeypatch):
    fetch = make_fetch(range(1, 1000))
    monkeypatch.setattr(cli, "fetch_trades_before", lambda url, product, before, timeout: fetch(product, before))
    producer = FakeProducer()
    ok = await cli.backfill_jobs(Settings(), expected_jobs(), producer=producer)
    assert ok
    produced = sorted((key, json.loads(value)["trade_id"]) for _, key, value in producer.messages)
    assert produced == [("BTC-USD", i) for i in range(101, 111)] + [("ETH-USD", 5)]


@pytest.mark.asyncio
async def test_backfill_jobs_reports_incomplete_ranges(monkeypatch):
    fetch = make_fetch(range(108, 1000))  # 101..107 no longer available
    monkeypatch.setattr(cli, "fetch_trades_before", lambda url, product, before, timeout: fetch(product, before))
    ok = await cli.backfill_jobs(Settings(), [BackfillJob.for_range("BTC-USD", 101, 110)], producer=FakeProducer())
    assert not ok
