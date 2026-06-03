from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import Boolean, Column, Float, Integer, MetaData, String, Table, Text, create_engine, event, select, update
from sqlalchemy.engine import Engine

from common import json_dumps, utc_now_iso

SCHEMA_VERSION = 2


def build_metadata() -> MetaData:
    md = MetaData()

    Table(
        "schema_info",
        md,
        Column("name", String(64), primary_key=True),
        Column("value", String(256), nullable=False),
        Column("updated_at", String(32), nullable=False),
    )

    Table(
        "orders",
        md,
        Column("client_order_id", String(64), primary_key=True),
        Column("exchange_order_id", String(128), nullable=True, index=True),
        Column("coin", String(32), nullable=False, index=True),
        Column("symbol", String(64), nullable=False, index=True),
        Column("side", String(16), nullable=False),
        Column("order_type", String(16), nullable=False),
        Column("time_in_force", String(16), nullable=True),
        Column("post_only", Boolean, nullable=False, default=True),
        Column("reduce_only", Boolean, nullable=False, default=False),
        Column("price", Float, nullable=True),
        Column("size", Float, nullable=False),
        Column("notional", Float, nullable=False),
        Column("status", String(32), nullable=False, index=True),
        Column("status_reason", Text, nullable=True),
        Column("model", String(64), nullable=False),
        Column("reservation_price", Float, nullable=True),
        Column("half_spread", Float, nullable=True),
        Column("mid_price", Float, nullable=True),
        Column("volatility", Float, nullable=True),
        Column("inventory", Float, nullable=True),
        Column("gamma", Float, nullable=True),
        Column("k", Float, nullable=True),
        Column("horizon_seconds", Float, nullable=True),
        Column("raw_json", Text, nullable=True),
        Column("created_at", String(32), nullable=False),
        Column("updated_at", String(32), nullable=False),
        Column("last_seen_at", String(32), nullable=True),
    )

    Table(
        "fills",
        md,
        Column("fill_id", String(128), primary_key=True),
        Column("client_order_id", String(64), nullable=True, index=True),
        Column("exchange_order_id", String(128), nullable=True, index=True),
        Column("exchange_trade_id", String(128), nullable=True, index=True),
        Column("coin", String(32), nullable=False, index=True),
        Column("symbol", String(64), nullable=False, index=True),
        Column("side", String(16), nullable=False),
        Column("price", Float, nullable=False),
        Column("size", Float, nullable=False),
        Column("notional", Float, nullable=False),
        Column("fee", Float, nullable=True),
        Column("fee_currency", String(32), nullable=True),
        Column("timestamp_ms", Integer, nullable=True),
        Column("raw_json", Text, nullable=True),
        Column("created_at", String(32), nullable=False),
    )

    Table(
        "positions",
        md,
        Column("coin", String(32), primary_key=True),
        Column("symbol", String(64), nullable=False),
        Column("base_qty", Float, nullable=False),
        Column("mark_price", Float, nullable=True),
        Column("notional", Float, nullable=True),
        Column("raw_json", Text, nullable=True),
        Column("updated_at", String(32), nullable=False),
    )

    Table(
        "market_leverage",
        md,
        Column("coin", String(32), primary_key=True),
        Column("symbol", String(64), nullable=False),
        Column("max_leverage", Integer, nullable=False),
        Column("configured_leverage", Integer, nullable=False),
        Column("margin_mode", String(16), nullable=False),
        Column("target_order_exposure_usd", Float, nullable=False),
        Column("target_order_margin_usd", Float, nullable=False),
        Column("raw_json", Text, nullable=True),
        Column("updated_at", String(32), nullable=False),
    )

    Table(
        "model_quotes",
        md,
        Column("quote_id", String(64), primary_key=True),
        Column("coin", String(32), nullable=False, index=True),
        Column("symbol", String(64), nullable=False, index=True),
        Column("mid_price", Float, nullable=False),
        Column("best_bid", Float, nullable=True),
        Column("best_ask", Float, nullable=True),
        Column("bid_price", Float, nullable=True),
        Column("ask_price", Float, nullable=True),
        Column("reservation_price", Float, nullable=False),
        Column("half_spread", Float, nullable=False),
        Column("inventory", Float, nullable=False),
        Column("volatility", Float, nullable=False),
        Column("gamma", Float, nullable=False),
        Column("k", Float, nullable=False),
        Column("horizon_seconds", Float, nullable=False),
        Column("created_at", String(32), nullable=False, index=True),
    )

    Table(
        "audit_events",
        md,
        Column("event_id", String(64), primary_key=True),
        Column("level", String(16), nullable=False),
        Column("event_type", String(64), nullable=False, index=True),
        Column("coin", String(32), nullable=True, index=True),
        Column("client_order_id", String(64), nullable=True, index=True),
        Column("message", Text, nullable=False),
        Column("payload_json", Text, nullable=True),
        Column("created_at", String(32), nullable=False, index=True),
    )

    return md


class DualDB:
    def __init__(self, primary_url: str, sqlite_url: str, spool_file: str):
        self.log = logging.getLogger("DualDB")
        self.metadata = build_metadata()
        self.spool_file = Path(spool_file)
        self.engines: Dict[str, Engine] = {}
        for name, url in {"primary": primary_url, "sqlite": sqlite_url}.items():
            if not url:
                continue
            engine = create_engine(url, future=True, pool_pre_ping=True)
            if url.startswith("sqlite"):
                self._setup_sqlite(engine)
            self.engines[name] = engine

    @staticmethod
    def _setup_sqlite(engine: Engine) -> None:
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):  # type: ignore[no-untyped-def]
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.close()

    def table(self, name: str) -> Table:
        return self.metadata.tables[name]

    def create_all(self) -> None:
        for name, engine in self.engines.items():
            self.metadata.create_all(engine)
            self.upsert_one_engine(engine, "schema_info", "name", {
                "name": "schema_version", "value": str(SCHEMA_VERSION), "updated_at": utc_now_iso(),
            })
            self.log.info("database ready: %s", name)

    def upsert_one_engine(self, engine: Engine, table_name: str, pk_col: str, row: Dict[str, Any]) -> None:
        table = self.table(table_name)
        clean = {k: v for k, v in row.items() if k in table.c}
        pk_value = clean[pk_col]
        with engine.begin() as conn:
            existing = conn.execute(select(table.c[pk_col]).where(table.c[pk_col] == pk_value)).first()
            if existing:
                conn.execute(update(table).where(table.c[pk_col] == pk_value).values(**clean))
            else:
                conn.execute(table.insert().values(**clean))

    def upsert(self, table_name: str, pk_col: str, row: Dict[str, Any]) -> None:
        failures: Dict[str, str] = {}
        for name, engine in self.engines.items():
            try:
                self.upsert_one_engine(engine, table_name, pk_col, row)
            except Exception as exc:
                failures[name] = repr(exc)
                self.log.error("DB write failed db=%s table=%s pk=%s error=%s", name, table_name, row.get(pk_col), exc)
        if failures:
            self.spool_failure(table_name, pk_col, row, failures)

    def spool_failure(self, table_name: str, pk_col: str, row: Dict[str, Any], failures: Dict[str, str]) -> None:
        self.spool_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.spool_file, "a", encoding="utf-8") as f:
            f.write(json_dumps({
                "created_at": utc_now_iso(), "table": table_name, "pk_col": pk_col, "row": row, "failures": failures,
            }) + "\n")

    def audit(self, level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None, coin: Optional[str] = None, client_order_id: Optional[str] = None) -> None:
        import uuid
        self.upsert("audit_events", "event_id", {
            "event_id": uuid.uuid4().hex,
            "level": level.upper(),
            "event_type": event_type,
            "coin": coin,
            "client_order_id": client_order_id,
            "message": message,
            "payload_json": json_dumps(payload or {}),
            "created_at": utc_now_iso(),
        })

    def reconcile_all(self) -> None:
        if len(self.engines) < 2:
            return
        names = list(self.engines.keys())
        a, b = self.engines[names[0]], self.engines[names[1]]
        for table_name, pk_col in [
            ("schema_info", "name"), ("orders", "client_order_id"), ("fills", "fill_id"),
            ("positions", "coin"), ("market_leverage", "coin"),
            ("model_quotes", "quote_id"), ("audit_events", "event_id"),
        ]:
            try:
                self._reconcile_table(table_name, pk_col, a, b)
                self._reconcile_table(table_name, pk_col, b, a)
            except Exception as exc:
                self.log.warning("reconcile failed table=%s error=%s", table_name, exc)

    def _reconcile_table(self, table_name: str, pk_col: str, source: Engine, dest: Engine) -> None:
        table = self.table(table_name)
        with source.begin() as src, dest.begin() as dst:
            src_rows = src.execute(select(table)).mappings().all()
            if not src_rows:
                return
            dest_keys = {r[0] for r in dst.execute(select(table.c[pk_col])).all()}
            for row in src_rows:
                if row[pk_col] not in dest_keys:
                    dst.execute(table.insert().values(**dict(row)))
