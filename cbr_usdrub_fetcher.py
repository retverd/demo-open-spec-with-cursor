import argparse
import datetime as dt
import os
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import httpx
import pandas as pd

CBR_ARCHIVE_URL = (
    "https://www.cbr-xml-daily.ru/archive/{year}/{month:02d}/{day:02d}/daily_json.js"
)
PAIR = "USD/RUB"
SOURCE = "CBR"
TIMEOUT_SECONDS = 10.0
MAX_RETRIES = 3


@dataclass
class RateRecord:
    date: dt.date
    pair: str
    rate: float
    source: str
    retrieved_at: dt.datetime


class FetchError(Exception):
    pass


def build_date_range(today: Optional[dt.date] = None) -> List[dt.date]:
    today = today or dt.date.today()
    return [today - dt.timedelta(days=i) for i in range(6, -1, -1)]


def fetch_daily_rate(
    target_date: dt.date,
    client: httpx.Client,
) -> Optional[float]:
    url = CBR_ARCHIVE_URL.format(
        year=target_date.year, month=target_date.month, day=target_date.day
    )
    resp = client.get(url, timeout=TIMEOUT_SECONDS)
    if resp.status_code == 404:
        return None  # нет данных за этот день — частичный успех допустим
    resp.raise_for_status()
    data = resp.json()
    valute = data.get("Valute", {})
    usd = valute.get("USD")
    if not usd:
        return None
    rate = usd.get("Value")
    return rate


def fetch_with_retries(
    fn: Callable[[], Optional[float]],
    max_retries: int = MAX_RETRIES,
) -> Optional[float]:
    attempts = 0
    last_error: Optional[Exception] = None
    while attempts <= max_retries:
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            # временные ошибки источника (5xx) — пробуем повторные попытки
            if 500 <= e.response.status_code < 600:
                last_error = e
                attempts += 1
                continue
            raise
        except (httpx.TransportError, httpx.TimeoutException) as e:
            last_error = e
            attempts += 1
            continue
        except Exception:  # noqa: BLE001 - оставляем общий catch чтобы пробросить ниже
            raise
    if last_error:
        raise FetchError(f"Failed after retries: {last_error}") from last_error
    return None


def fetch_rates(
    date_range: Iterable[dt.date],
    client: httpx.Client,
) -> List[RateRecord]:
    now = dt.datetime.now()
    records: List[RateRecord] = []
    for d in date_range:

        def _call() -> Optional[float]:
            return fetch_daily_rate(d, client)

        rate = fetch_with_retries(_call, max_retries=MAX_RETRIES)
        if rate is None:
            continue
        validate_rate(rate)
        records.append(
            RateRecord(
                date=d,
                pair=PAIR,
                rate=float(rate),
                source=SOURCE,
                retrieved_at=now,
            )
        )
    return records


def validate_rate(rate: float) -> None:
    if not isinstance(rate, (int, float)):
        raise ValueError("rate must be a number")
    if rate <= 0:
        raise ValueError("rate must be > 0")


def save_parquet(records: List[RateRecord], output_path: str) -> None:
    if not records:
        raise ValueError("no records to save")
    df = pd.DataFrame(
        [
            {
                "date": r.date.isoformat(),
                "pair": r.pair,
                "rate": r.rate,
                "source": r.source,
                "retrieved_at": r.retrieved_at.isoformat(timespec="seconds"),
            }
            for r in records
        ]
    )
    df.to_parquet(output_path, index=False)


def build_output_filename(now: Optional[dt.datetime] = None) -> str:
    now = now or dt.datetime.now()
    today_str = now.date().isoformat()
    time_str = now.strftime("%H%M%S")
    return f"cbr_usdrub_{today_str}_{time_str}.parquet"


def run(out_dir: str = ".") -> str:
    today = dt.date.today()
    date_range = build_date_range(today)
    with httpx.Client() as client:
        records = fetch_rates(date_range, client)
    if not records:
        raise FetchError("No data fetched for the requested date range")
    filename = build_output_filename()
    output_path = os.path.join(out_dir, filename)
    save_parquet(records, output_path)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch USD/RUB rates from CBR for last 7 days and save to Parquet."
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Выходная папка (по умолчанию текущая).",
    )
    args = parser.parse_args()
    output_path = run(out_dir=args.out_dir)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
