"""AlloyDB-backed store.

Connects to AlloyDB (Postgres-compatible) using credentials from the project
`.env` file. All application tables live under the schema named by
`TARGET_SCHEMA`; the search_path is set on every pooled connection so the rest
of the codebase can keep writing unqualified table names.

A `HybridRow` row factory preserves the prior `sqlite3.Row` ergonomics: results
support `row["col"]`, positional indexing, iteration-yields-values (for
`(count,) = row` unpacking) and `dict(row)`, so call sites don't need to change.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg_pool import ConnectionPool


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _strip_quotes(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1]
    return s


PG_HOST = _strip_quotes(os.environ.get("ALLOYDB_HOST", "localhost"))
PG_PORT = int(_strip_quotes(os.environ.get("ALLOYDB_PORT", "5432")))
PG_DBNAME = _strip_quotes(os.environ.get("ALLOYDB_DBNAME", "postgres"))
PG_USER = _strip_quotes(os.environ.get("ALLOYDB_USER", "postgres"))
PG_PASSWORD = _strip_quotes(os.environ.get("ALLOYDB_PASSWORD", ""))
PG_SSLMODE = _strip_quotes(os.environ.get("ALLOYDB_SSLMODE", "disable"))
TARGET_SCHEMA = _strip_quotes(os.environ.get("TARGET_SCHEMA", "public")) or "public"

CONNINFO = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
    f"user={PG_USER} password={PG_PASSWORD} sslmode={PG_SSLMODE} "
    f"application_name=iris-poc"
)

DB_DSN = f"postgresql://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}?schema={TARGET_SCHEMA}"


class HybridRow(dict):
    """Dict row that also behaves like ``sqlite3.Row``.

    * ``row["col"]`` — keyed access (inherits from dict).
    * ``row[i]``     — positional access.
    * ``list(row)``  — yields *values* (not keys).
    * ``(x,) = row`` — unpacks values, matching the prior sqlite behavior.
    * ``dict(row)``  — works because ``keys()`` still returns the column names.
    """

    __slots__ = ()

    def __iter__(self):
        return iter(super().values())

    def __getitem__(self, key):
        if isinstance(key, int):
            values = list(super().values())
            return values[key]
        return super().__getitem__(key)

    def keys(self):  # noqa: D401 - match dict.keys semantics
        return list(super().keys())


def _hybrid_row_factory(cursor):
    names = [c.name for c in (cursor.description or [])]

    def _make(values):
        return HybridRow(zip(names, values))

    return _make


_POOL: ConnectionPool | None = None
_POOL_LOCK = threading.Lock()


def _configure(conn: psycopg.Connection) -> None:
    """Runs once per newly-opened pool connection.

    The pool requires the callback to leave the connection in an idle (no open
    transaction) state. ``SET`` opens an implicit transaction under the default
    ``autocommit=False``, so toggle autocommit for the duration of the setup.
    """
    conn.row_factory = _hybrid_row_factory
    prev = conn.autocommit
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{TARGET_SCHEMA}", public')
    finally:
        conn.autocommit = prev


def _pool() -> ConnectionPool:
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                _POOL = ConnectionPool(
                    conninfo=CONNINFO,
                    min_size=1,
                    max_size=8,
                    configure=_configure,
                    open=True,
                )
    return _POOL


def get_conn():
    """Return a connection context manager from the pool."""
    return _pool().connection()


@contextmanager
def tx():
    """Transactional pool connection. Commits on clean exit, rolls back on error."""
    with _pool().connection() as conn:
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise


TABLES = [
    "entities",
    "humans",
    "contacts",
    "relationships",
    "markets",
    "audit_log",
    "review_queue",
    "mapping_issues",
    "dedup_clusters",
    "meta",
]

DDL = """
CREATE TABLE IF NOT EXISTS entities (
    entity_identifier TEXT PRIMARY KEY,
    name TEXT,
    legal_entity_type TEXT,
    date_of_incorporation TEXT,
    entity_description TEXT,
    annual_revenue DOUBLE PRECISION,
    is_closed INTEGER,
    doing_business_as TEXT,
    fein TEXT,
    ssn TEXT,
    allow_agency_participation INTEGER,
    merged_from TEXT
);

CREATE TABLE IF NOT EXISTS humans (
    human_identifier TEXT PRIMARY KEY,
    prefix TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    preferred_name TEXT,
    pronoun TEXT,
    date_of_birth TEXT,
    education_level TEXT,
    occupation TEXT,
    occupation_industry TEXT,
    year_occupation_started INTEGER,
    is_deceased INTEGER,
    gender TEXT,
    marital_status TEXT,
    ssn TEXT,
    license_number TEXT,
    license_state TEXT,
    first_licensed_date TEXT,
    allow_agency_participation INTEGER,
    merged_from TEXT
);

CREATE TABLE IF NOT EXISTS contacts (
    contact_id SERIAL PRIMARY KEY,
    parent_type TEXT,
    parent_identifier TEXT,
    contact_type TEXT,
    physical_line1 TEXT,
    physical_city TEXT,
    physical_state TEXT,
    physical_country TEXT,
    mailing_line1 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    primary_phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS relationships (
    rel_id SERIAL PRIMARY KEY,
    src_type TEXT,
    src_name TEXT,
    rel_type TEXT,
    dst_type TEXT,
    dst_name TEXT,
    title TEXT
);

CREATE TABLE IF NOT EXISTS markets (
    market_id SERIAL PRIMARY KEY,
    name TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    ts TEXT DEFAULT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'),
    actor TEXT,
    action TEXT,
    field TEXT,
    target TEXT,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS review_queue (
    review_id SERIAL PRIMARY KEY,
    kind TEXT,
    title TEXT,
    status TEXT DEFAULT 'open',
    payload_json TEXT,
    created_ts TEXT DEFAULT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'),
    decided_ts TEXT,
    decision TEXT,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS mapping_issues (
    issue_id SERIAL PRIMARY KEY,
    source_field TEXT,
    canonical_field TEXT,
    sample_value TEXT,
    status TEXT,
    note TEXT,
    suggested_fix TEXT,
    confidence DOUBLE PRECISION,
    record_ref TEXT
);

CREATE TABLE IF NOT EXISTS dedup_clusters (
    cluster_id SERIAL PRIMARY KEY,
    kind TEXT,
    winner_ref TEXT,
    members_json TEXT,
    signals_json TEXT,
    confidence DOUBLE PRECISION,
    auto_merged INTEGER,
    status TEXT DEFAULT 'auto'
);

CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT
);
"""


def _ensure_schema(cur: psycopg.Cursor) -> None:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}"')
    cur.execute(f'SET search_path TO "{TARGET_SCHEMA}", public')


def init_db() -> None:
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(DDL)


def reset_db() -> None:
    with _pool().connection() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            for t in TABLES:
                cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
            cur.execute(DDL)


def write_audit(
    actor: str,
    action: str,
    field: str | None,
    target: str | None,
    reason: str | None,
) -> None:
    with tx() as conn:
        conn.execute(
            "INSERT INTO audit_log(actor, action, field, target, reason) VALUES (%s,%s,%s,%s,%s)",
            (actor, action, field, target, reason),
        )
