from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from portfolio_tool import data as market_data


def test_xpar_session_override_forces_0900_1730_local_time() -> None:
    schedule = pd.DataFrame(
        {
            "market_open": [
                pd.Timestamp("2026-01-06T08:05:00Z"),
                pd.Timestamp("2026-07-06T07:11:00Z"),
            ],
            "market_close": [
                pd.Timestamp("2026-01-06T16:29:00Z"),
                pd.Timestamp("2026-07-06T15:24:00Z"),
            ],
        }
    )

    out = market_data._apply_exchange_session_overrides(schedule, "XPAR")
    open_local = pd.to_datetime(out["market_open"], utc=True).dt.tz_convert("Europe/Paris")
    close_local = pd.to_datetime(out["market_close"], utc=True).dt.tz_convert("Europe/Paris")

    assert list(open_local.dt.hour) == [9, 9]
    assert list(open_local.dt.minute) == [0, 0]
    assert list(close_local.dt.hour) == [17, 17]
    assert list(close_local.dt.minute) == [30, 30]


def test_non_overridden_exchange_keeps_schedule() -> None:
    schedule = pd.DataFrame(
        {
            "market_open": [pd.Timestamp("2026-01-06T14:30:00Z")],
            "market_close": [pd.Timestamp("2026-01-06T21:00:00Z")],
        }
    )
    out = market_data._apply_exchange_session_overrides(schedule, "XNYS")
    assert out.equals(schedule)


def test_trls_session_override_forces_0730_2300_local_time() -> None:
    schedule = pd.DataFrame(
        {
            "market_open": [
                pd.Timestamp("2026-01-06T08:05:00Z"),
                pd.Timestamp("2026-07-06T07:11:00Z"),
            ],
            "market_close": [
                pd.Timestamp("2026-01-06T16:29:00Z"),
                pd.Timestamp("2026-07-06T15:24:00Z"),
            ],
        }
    )

    out = market_data._apply_exchange_session_overrides(schedule, "TRLS")
    open_local = pd.to_datetime(out["market_open"], utc=True).dt.tz_convert("Europe/Berlin")
    close_local = pd.to_datetime(out["market_close"], utc=True).dt.tz_convert("Europe/Berlin")

    assert list(open_local.dt.hour) == [7, 7]
    assert list(open_local.dt.minute) == [30, 30]
    assert list(close_local.dt.hour) == [23, 23]
    assert list(close_local.dt.minute) == [0, 0]


def test_trls_calendar_alias_points_to_xetr() -> None:
    assert market_data.EXCHANGE_CALENDAR_ALIASES["TRLS"] == "XETR"
