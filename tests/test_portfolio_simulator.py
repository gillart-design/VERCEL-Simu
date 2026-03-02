from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import portfolio_simulator_app as app


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    app.init_db(conn)
    return conn


def test_compute_positions_fifo_vs_lifo() -> None:
    tx = pd.DataFrame(
        [
            {
                "id": 1,
                "executed_at_utc": pd.Timestamp("2026-01-01", tz="UTC"),
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 10.0,
                "price": 100.0,
                "fees": 0.0,
                "currency": "USD",
                "fx_to_base": 1.0,
                "strategy_tag": "test",
                "exchange": "XNYS",
                "note": None,
            },
            {
                "id": 2,
                "executed_at_utc": pd.Timestamp("2026-01-02", tz="UTC"),
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 10.0,
                "price": 120.0,
                "fees": 0.0,
                "currency": "USD",
                "fx_to_base": 1.0,
                "strategy_tag": "test",
                "exchange": "XNYS",
                "note": None,
            },
            {
                "id": 3,
                "executed_at_utc": pd.Timestamp("2026-01-03", tz="UTC"),
                "symbol": "AAPL",
                "side": "SELL",
                "quantity": 10.0,
                "price": 130.0,
                "fees": 0.0,
                "currency": "USD",
                "fx_to_base": 1.0,
                "strategy_tag": "test",
                "exchange": "XNYS",
                "note": None,
            },
        ]
    )
    fifo = app.compute_positions(tx, accounting_method="fifo")
    lifo = app.compute_positions(tx, accounting_method="lifo")
    assert float(fifo.iloc[0]["realized_pnl"]) == 300.0
    assert float(lifo.iloc[0]["realized_pnl"]) == 100.0


def test_upsert_snapshot_respects_interval_and_delta() -> None:
    conn = make_conn()
    snap = {"portfolio_value": 100000.0, "cash": 50000.0, "invested": 50000.0, "pnl": 0.0, "pnl_pct": 0.0}
    app.upsert_snapshot(conn, snap, min_delta_eur=1.0, min_seconds=1)
    app.upsert_snapshot(conn, snap, min_delta_eur=1.0, min_seconds=60)
    rows = conn.execute("SELECT COUNT(*) AS c FROM snapshots").fetchone()["c"]
    assert rows == 1


def test_trade_risk_blocks_cash_shortage() -> None:
    holdings = pd.DataFrame(columns=["symbol", "zone", "secteur", "valeur_marche"])
    errors = app.check_trade_risk(
        side="BUY",
        symbol="AAPL",
        quantity=100.0,
        price=200.0,
        fees=0.0,
        cash=1000.0,
        holdings=holdings,
        base_currency="EUR",
        fx_to_base=1.0,
        max_line_pct=50.0,
        max_sector_pct=60.0,
        max_zone_pct=70.0,
    )
    assert any("Cash insuffisant" in e for e in errors)


def test_polygon_symbol_supported() -> None:
    assert app.polygon_symbol_supported("AAPL")
    assert not app.polygon_symbol_supported("MC.PA")
