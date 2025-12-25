import datetime as dt
import os
from typing import List, Optional

import httpx
import pandas as pd
import pytest

from cbr_usdrub_fetcher import (
    FetchError,
    RateRecord,
    build_date_range,
    build_output_filename,
    fetch_rates,
    save_parquet,
    validate_rate,
)


def test_build_date_range_has_7_days_and_includes_today():
    today = dt.date(2025, 12, 24)
    dr = build_date_range(today)
    assert len(dr) == 7
    assert dr[0] == dt.date(2025, 12, 18)
    assert dr[-1] == today


def test_validate_rate_positive_number():
    validate_rate(75.12)
    validate_rate(1)
    with pytest.raises(ValueError):
        validate_rate(0)
    with pytest.raises(ValueError):
        validate_rate(-1)
    with pytest.raises(ValueError):
        validate_rate("bad")  # type: ignore[arg-type]


def test_save_parquet_writes_file(tmp_path):
    records = [
        RateRecord(
            date=dt.date(2025, 12, 20),
            pair="USD/RUB",
            rate=100.5,
            source="CBR",
            retrieved_at=dt.datetime(2025, 12, 21, 10, 11, 12),
        )
    ]
    outfile = tmp_path / "out.parquet"
    save_parquet(records, outfile.as_posix())
    assert outfile.exists()
    df = pd.read_parquet(outfile)
    assert list(df.columns) == ["date", "pair", "rate", "source", "retrieved_at"]
    assert df.iloc[0]["date"] == "2025-12-20"
    assert df.iloc[0]["pair"] == "USD/RUB"
    assert df.iloc[0]["rate"] == 100.5


def test_build_output_filename_contains_date_and_time():
    now = dt.datetime(2025, 12, 24, 15, 4, 5)
    name = build_output_filename(now)
    assert name == "cbr_usdrub_2025-12-24_150405.parquet"


class DummyClient:
    def __init__(self, responses: List[Optional[float]]):
        self.responses = responses
        self.calls = 0

    def get(self, *args, **kwargs):
        # Simulate httpx.Response-like object with status_code/json/raise_for_status
        class Resp:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def raise_for_status(self):
                if 400 <= self.status_code < 600:
                    raise httpx.HTTPStatusError("error", request=None, response=self)

            def json(self):
                return self._payload

        if self.calls >= len(self.responses):
            raise httpx.TransportError("no more responses")
        value = self.responses[self.calls]
        self.calls += 1
        if isinstance(value, Exception):
            raise value
        if value is None:
            return Resp(404, {})
        payload = {"Valute": {"USD": {"Value": value}}}
        return Resp(200, payload)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fetch_rates_retries_then_succeeds(monkeypatch):
    # first call raises transport error, second returns valid value
    client = DummyClient(responses=[httpx.TransportError("tmp"), 90.0])
    date_range = [dt.date(2025, 12, 24)]

    def fake_fetch_daily_rate(target_date, client):
        # use dummy client get
        resp = client.get("url")
        if resp is None:
            return None
        return resp.json()["Valute"]["USD"]["Value"]

    monkeypatch.setattr("cbr_usdrub_fetcher.fetch_daily_rate", fake_fetch_daily_rate)

    records = fetch_rates(date_range, client)  # type: ignore[arg-type]
    assert len(records) == 1
    assert records[0].rate == 90.0


def test_fetch_rates_raises_after_retries(monkeypatch):
    client = DummyClient(
        responses=[
            httpx.TimeoutException("t"),
            httpx.TimeoutException("t"),
            httpx.TimeoutException("t"),
            httpx.TimeoutException("t"),
        ]
    )
    date_range = [dt.date(2025, 12, 24)]

    def fake_fetch_daily_rate(target_date, client):
        resp = client.get("url")
        if resp is None:
            return None
        return resp.json()["Valute"]["USD"]["Value"]

    monkeypatch.setattr("cbr_usdrub_fetcher.fetch_daily_rate", fake_fetch_daily_rate)

    with pytest.raises(FetchError):
        fetch_rates(date_range, client)  # type: ignore[arg-type]


def test_fetch_rates_skips_missing_today(monkeypatch):
    client = DummyClient(responses=[None])
    date_range = [dt.date(2025, 12, 24)]

    def fake_fetch_daily_rate(target_date, client):
        resp = client.get("url")
        if resp is None:
            return None
        payload = resp.json()
        valute = payload.get("Valute", {})
        usd = valute.get("USD")
        if usd is None:
            return None
        return usd.get("Value")

    monkeypatch.setattr("cbr_usdrub_fetcher.fetch_daily_rate", fake_fetch_daily_rate)
    records = fetch_rates(date_range, client)  # type: ignore[arg-type]
    assert records == []


@pytest.mark.integration
def test_integration_real_api(tmp_path, monkeypatch):
    # change cwd to temp to avoid polluting repo
    monkeypatch.chdir(tmp_path)
    from cbr_usdrub_fetcher import run

    output = run(".")
    assert os.path.exists(output)
    df = pd.read_parquet(output)
    assert not df.empty
    assert set(["date", "pair", "rate", "source", "retrieved_at"]).issubset(df.columns)
    assert (df["pair"] == "USD/RUB").all()
