# ============================================================================
# run_history.py
# ----------------------------------------------------------------------------
# Reads run metrics from the platform's existing audit table:
#
#     {catalog}.platform_meta.pipeline_runs
#
# This table is populated by metadata_framework.MetadataFramework on every
# Bronze pipeline run — the UI just surfaces it. We use the Databricks SQL
# Connector against a SQL Warehouse so we don't need to spin up a cluster
# just to read a few rows.
#
# Catalog & warehouse id come from app.yaml at deploy time.
# ============================================================================

import logging
import os
from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

logger = logging.getLogger(__name__)


CATALOG       = os.getenv("DBX_CATALOG",       "ops_dev")
WAREHOUSE_ID  = os.getenv("DBX_WAREHOUSE_ID",  "")
META_SCHEMA   = os.getenv("DBX_META_SCHEMA",   "platform_meta")


def _connect():
    """
    Open a SQL Warehouse connection using the Databricks App's OAuth token.
    The host comes from DATABRICKS_HOST (auto-injected by the App runtime).
    """
    if not WAREHOUSE_ID:
        raise RuntimeError("DBX_WAREHOUSE_ID is not configured in app.yaml")

    w     = WorkspaceClient()
    host  = w.config.host.replace("https://", "").rstrip("/")
    token = w.config.token         # OAuth token in App context

    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    )


def list_recent_runs(limit: int = 50,
                     pipeline_name_filter: Optional[str] = None) -> List[dict]:
    """
    Return the N most recent rows from pipeline_runs.

    Schema (from metadata_framework):
        pipeline_name, status, start_time, end_time, duration_seconds,
        row_count, config_version, error_message
    """
    where = ""
    params = []
    if pipeline_name_filter:
        where = "WHERE pipeline_name LIKE ?"
        params.append(f"%{pipeline_name_filter}%")

    query = f"""
        SELECT pipeline_name,
               status,
               CAST(start_time AS STRING) AS start_time,
               duration_seconds,
               row_count,
               config_version
        FROM   {CATALOG}.{META_SCHEMA}.pipeline_runs
        {where}
        ORDER  BY start_time DESC
        LIMIT  ?
    """
    params.append(limit)

    rows: List[dict] = []
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            for r in cur.fetchall():
                rows.append({c: v for c, v in zip(cols, r)})
    except Exception as e:                       # noqa: BLE001
        # The UI shouldn't crash if the meta table doesn't exist yet
        # (e.g. fresh workspace before the first run).
        logger.warning(f"pipeline_runs query failed: {e}")
        return []
    return rows


def get_run_metrics(pipeline_name: str) -> Optional[dict]:
    """
    Pull the LATEST metrics for a given pipeline_name.
    Used by /api/runs/{run_id} once the run reaches TERMINATED so the UI
    can show "1.2M rows ingested in 4m 20s".
    """
    query = f"""
        SELECT pipeline_name, status, row_count, duration_seconds,
               CAST(start_time AS STRING) AS start_time
        FROM   {CATALOG}.{META_SCHEMA}.pipeline_runs
        WHERE  pipeline_name = ?
        ORDER  BY start_time DESC
        LIMIT  1
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(query, [pipeline_name])
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return {c: v for c, v in zip(cols, row)}
    except Exception as e:                       # noqa: BLE001
        logger.warning(f"pipeline_runs lookup for {pipeline_name} failed: {e}")
        return None
