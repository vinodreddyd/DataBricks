# ============================================================================
# schema_introspection.py
# ----------------------------------------------------------------------------
# "Browse the source DB" feature — Lakehouse Federation edition.
#
# When a source database is registered as a Foreign Catalog in Unity Catalog
# (via Lakehouse Federation), every schema/table/column appears in the
# standard system.information_schema views. We just SELECT against them
# through the same SQL Warehouse the App already uses for run history.
#
# Why federation instead of a notebook job?
#   - Sub-second response time (vs 30-60s cluster boot + JDBC fetch)
#   - No JDBC drivers / Secret Scopes needed in the introspection path
#   - Unity Catalog enforces row/column-level access controls automatically
#   - Zero extra moving parts to deploy & monitor
#
# When does this NOT work?
#   - Source DB hasn't been federated yet (no foreign catalog created)
#   - User's UC permissions don't include USE CATALOG / USE SCHEMA grants
# In those cases the API returns a clear error and the UI surfaces it.
#
# Public functions:
#   introspect_schemas(ref)
#   introspect_tables(ref, schema)
#   introspect_columns(ref, schema, table)
#   suggest_watermark_columns(columns)         (pure helper, no IO)
# ============================================================================

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

logger = logging.getLogger(__name__)


# ── Configuration (env-driven) ──────────────────────────────────────────────
WAREHOUSE_ID = os.getenv("DBX_WAREHOUSE_ID", "")
CACHE_TTL_S  = int(os.getenv("DBX_INTROSPECT_CACHE_TTL_S", "300"))


# ── Identifier hardening ───────────────────────────────────────────────────
# Catalog/schema/table names go into IDENTIFIER positions in SQL — parameter
# binding doesn't help there, so we hard-validate against an allow-list
# pattern before backticking.
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-\$]*$")


def _safe_ident(name: str, kind: str = "identifier") -> str:
    if not name:
        raise ValueError(f"{kind} is required")
    if not _SAFE_IDENT.match(name):
        raise ValueError(
            f"{kind!r} must match [A-Za-z_][A-Za-z0-9_\\-\\$]* (got {name!r})"
        )
    return f"`{name}`"      # backtick-wrap so reserved words still work


# ── Tiny TTL cache ─────────────────────────────────────────────────────────
_cache: Dict[str, Tuple[float, list]] = {}
_cache_lock = Lock()


def _cache_key(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:24]


def _cache_get(key: str) -> Optional[list]:
    with _cache_lock:
        hit = _cache.get(key)
        if not hit:
            return None
        ts, val = hit
        if time.time() - ts > CACHE_TTL_S:
            _cache.pop(key, None)
            return None
        return val


def _cache_put(key: str, value: list) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), value)
        if len(_cache) > 500:
            for k, _ in sorted(_cache.items(), key=lambda kv: kv[1][0])[:50]:
                _cache.pop(k, None)


def cache_clear() -> int:
    """For the 'Refresh' button in the UI."""
    with _cache_lock:
        n = len(_cache)
        _cache.clear()
    return n


# ── Warehouse connection (mirrors run_history._connect) ────────────────────
def _connect():
    """Open a SQL Warehouse connection using the App's OAuth token."""
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DBX_WAREHOUSE_ID is not configured. The schema browser uses a "
            "SQL Warehouse to read system.information_schema."
        )
    w     = WorkspaceClient()
    host  = w.config.host.replace("https://", "").rstrip("/")
    token = w.config.token
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    )


def _query(sql: str, params: Optional[list] = None) -> List[dict]:
    """Run a SELECT against the warehouse, return list of {col: val} dicts."""
    rows: List[dict] = []
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            rows.append({c: v for c, v in zip(cols, r)})
    return rows


# ── Federation reference (what the UI passes us) ───────────────────────────
@dataclass(frozen=True)
class FederationRef:
    """Identifies a Lakehouse-Federated source by its UC foreign catalog."""
    foreign_catalog: str

    def validate(self) -> Optional[str]:
        if not self.foreign_catalog:
            return "foreign_catalog is required"
        if not _SAFE_IDENT.match(self.foreign_catalog):
            return f"foreign_catalog has invalid characters: {self.foreign_catalog!r}"
        return None


# ── Error humanization ─────────────────────────────────────────────────────
def _humanize_error(e: Exception) -> str:
    """
    Convert raw SQL errors into something a user can act on. The most common
    failures here are "catalog not found" (forgot to register federation)
    and permission errors (UC grants).
    """
    msg = str(e)
    low = msg.lower()
    if "catalog" in low and ("not found" in low or "does not exist" in low):
        return ("Foreign catalog not found. Make sure the source database has "
                "been registered via Lakehouse Federation and the catalog name "
                "is spelled correctly. Ask a workspace admin if unsure.")
    if "schema" in low and ("not found" in low or "does not exist" in low):
        return ("Schema not found in the foreign catalog, or you don't have "
                "USE SCHEMA permission on it.")
    if any(k in low for k in ("permission", "denied", "does not have", "unauthorized")):
        return ("You don't have Unity Catalog permissions to browse this "
                "object. You'll need at least USE CATALOG + USE SCHEMA, plus "
                "SELECT on the table.")
    if "warehouse" in low and ("stopped" in low or "not running" in low):
        return ("SQL Warehouse is stopped. Start it from the SQL Warehouses "
                "page and retry.")
    if "could not connect" in low:
        return "Could not reach the SQL Warehouse. Check it's running and try again."
    return f"Introspection failed: {msg}"


# ── Public API: schemas ────────────────────────────────────────────────────
def introspect_schemas(ref: FederationRef, force: bool = False) -> List[Dict]:
    """
    List schemas under a foreign catalog.
    Returns: [{'schema_name': 'public'}, ...]
    """
    err = ref.validate()
    if err:
        raise ValueError(err)

    key = _cache_key("schemas", ref.foreign_catalog)
    if not force:
        hit = _cache_get(key)
        if hit is not None:
            return hit

    cat = _safe_ident(ref.foreign_catalog, "foreign_catalog")
    sql = (
        f"SELECT schema_name "
        f"FROM   {cat}.information_schema.schemata "
        f"WHERE  schema_name <> 'information_schema' "
        f"ORDER  BY schema_name"
    )
    try:
        rows = _query(sql)
    except Exception as e:                           # noqa: BLE001
        raise RuntimeError(_humanize_error(e)) from e

    _cache_put(key, rows)
    return rows


# ── Public API: tables ─────────────────────────────────────────────────────
def introspect_tables(ref: FederationRef, schema: str,
                      force: bool = False) -> List[Dict]:
    """
    List tables and views inside a schema.
    Returns: [{table_name, table_type, row_estimate?}, ...]
    """
    err = ref.validate()
    if err:
        raise ValueError(err)
    if not schema:
        raise ValueError("schema is required")

    key = _cache_key("tables", ref.foreign_catalog, schema)
    if not force:
        hit = _cache_get(key)
        if hit is not None:
            return hit

    cat = _safe_ident(ref.foreign_catalog, "foreign_catalog")
    sql = (
        f"SELECT table_name, "
        f"       table_type, "
        f"       CAST(NULL AS BIGINT) AS row_estimate "
        f"FROM   {cat}.information_schema.tables "
        f"WHERE  table_schema = ? "
        f"  AND  table_type IN ('BASE TABLE', 'VIEW', 'TABLE') "
        f"ORDER  BY table_name"
    )
    try:
        rows = _query(sql, [schema])
    except Exception as e:                           # noqa: BLE001
        raise RuntimeError(_humanize_error(e)) from e

    _cache_put(key, rows)
    return rows


# ── Public API: columns ────────────────────────────────────────────────────
def introspect_columns(ref: FederationRef, schema: str, table: str,
                       force: bool = False) -> List[Dict]:
    """
    List columns for a table.
    Returns: [{column_name, data_type, is_nullable, ordinal_position}, ...]
    """
    err = ref.validate()
    if err:
        raise ValueError(err)
    if not (schema and table):
        raise ValueError("schema and table are required")

    key = _cache_key("columns", ref.foreign_catalog, schema, table)
    if not force:
        hit = _cache_get(key)
        if hit is not None:
            return hit

    cat = _safe_ident(ref.foreign_catalog, "foreign_catalog")
    sql = (
        f"SELECT column_name, "
        f"       data_type, "
        f"       is_nullable, "
        f"       ordinal_position "
        f"FROM   {cat}.information_schema.columns "
        f"WHERE  table_schema = ? "
        f"  AND  table_name   = ? "
        f"ORDER  BY ordinal_position"
    )
    try:
        rows = _query(sql, [schema, table])
    except Exception as e:                           # noqa: BLE001
        raise RuntimeError(_humanize_error(e)) from e

    _cache_put(key, rows)
    return rows


# ── Watermark heuristic ────────────────────────────────────────────────────
# Pure helper; no IO. The UI uses this to pre-check likely watermark cols
# in the column picker.
WATERMARK_NAME_HINTS = (
    "updated_at", "update_ts", "last_updated", "lst_updt",
    "modified_at", "modified_ts", "mod_ts",
    "created_at", "create_ts", "creation_ts",
    "load_ts", "load_dt", "load_date",
    "etl_ts", "ingest_ts", "_ts", "_dt",
    "version", "rowversion", "scn",
)
WATERMARK_TYPE_HINTS = (
    "timestamp", "datetime", "date", "time",
    "bigint", "int", "long", "numeric", "decimal", "rowversion",
)


def suggest_watermark_columns(columns: List[Dict]) -> List[str]:
    """
    Return column names ranked by likelihood of being a good incremental
    watermark. Cheap heuristic — name match weighed more than type match.
    """
    scored = []
    for c in columns:
        name = (c.get("column_name") or "").lower()
        typ  = (c.get("data_type")   or "").lower()
        score = 0
        for hint in WATERMARK_NAME_HINTS:
            if hint in name:
                score += 3
                break
        for hint in WATERMARK_TYPE_HINTS:
            if hint in typ:
                score += 1
                break
        if name in ("id", "pk") or name.endswith("_id"):
            score += 1
        if score > 0:
            scored.append((score, c.get("column_name")))
    scored.sort(reverse=True)
    return [name for _, name in scored][:5]
