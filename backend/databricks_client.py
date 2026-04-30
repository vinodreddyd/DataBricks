# ============================================================================
# databricks_client.py
# ----------------------------------------------------------------------------
# Thin wrapper around the Databricks Python SDK for the two side-effects
# this app performs:
#
#   1. Write the generated YAML to Workspace Files
#       → /Workspace/Shared/dbx_ingestion/domain-configs/sources/generated/
#
#   2. Trigger the existing NoteBook_Main_Param.py via Jobs 2.1 runs/submit
#       → returns a run_id we can poll
#
# Auth model:
#   When deployed as a Databricks App, the SDK's default credential chain
#   picks up the on-behalf-of-user OAuth token automatically. Locally we
#   fall back to DATABRICKS_HOST / DATABRICKS_TOKEN env vars for dev.
# ============================================================================

import base64
import logging
import os
from datetime import datetime
from pathlib import PurePosixPath
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace, jobs

logger = logging.getLogger(__name__)


# ── Configurable paths (read from app.yaml env section at deploy time) ─────
WORKSPACE_GENERATED_DIR = os.getenv(
    "DBX_GENERATED_DIR",
    "/Workspace/Shared/dbx_ingestion/domain-configs/sources/generated",
)
WORKSPACE_ENV_DIR = os.getenv(
    "DBX_ENV_DIR",
    "/Workspace/Shared/dbx_ingestion/domain-configs/env-overrides",
)
NOTEBOOK_PATH = os.getenv(
    "DBX_NOTEBOOK_PATH",
    "/Workspace/Shared/dbx_ingestion/Data-Platform/NoteBook_Main_Param",
)
DEFAULT_CLUSTER_ID = os.getenv("DBX_DEFAULT_CLUSTER_ID", "")


def _client() -> WorkspaceClient:
    """
    Build a WorkspaceClient. In an App context the SDK auto-picks up the
    user's OAuth token via the DATABRICKS_HOST / X-Forwarded-Auth headers
    injected by the App runtime — no kwargs needed.
    """
    return WorkspaceClient()


# ============================================================================
# 1. Workspace Files — save generated YAML
# ============================================================================

def _ensure_dir(w: WorkspaceClient, path: str) -> None:
    """Create the parent directory if it doesn't already exist."""
    try:
        w.workspace.mkdirs(path)
    except Exception as e:                       # noqa: BLE001
        # mkdirs is idempotent in practice; swallow "already exists"
        logger.debug(f"mkdirs({path}) note: {e}")


def save_yaml_to_workspace(
    yaml_text: str,
    source_name: str,
    overwrite: bool = False,
) -> Dict[str, int | str]:
    """
    Save a YAML config to /Workspace/.../sources/generated/<source>_<ts>.yaml.

    Returns:
        {"workspace_path": <str>, "bytes_written": <int>}
    """
    w = _client()
    _ensure_dir(w, WORKSPACE_GENERATED_DIR)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename  = f"{source_name}_{timestamp}.yaml"
    full_path = str(PurePosixPath(WORKSPACE_GENERATED_DIR) / filename)

    raw = yaml_text.encode("utf-8")
    w.workspace.upload(
        path=full_path,
        content=raw,
        format=workspace.ImportFormat.AUTO,
        overwrite=overwrite,
    )
    logger.info(f"Wrote {len(raw)} bytes → {full_path}")
    return {"workspace_path": full_path, "bytes_written": len(raw)}


def read_yaml_from_workspace(workspace_path: str) -> str:
    """Read back a YAML we wrote — used by the 'Re-run' flow in the UI."""
    w = _client()
    resp = w.workspace.export(path=workspace_path, format=workspace.ExportFormat.AUTO)
    return base64.b64decode(resp.content).decode("utf-8")


def list_generated_configs(limit: int = 50) -> List[Dict[str, str]]:
    """List the most recent generated YAMLs (newest first)."""
    w = _client()
    try:
        objs = list(w.workspace.list(WORKSPACE_GENERATED_DIR))
    except Exception as e:                       # noqa: BLE001
        logger.warning(f"list({WORKSPACE_GENERATED_DIR}) failed: {e}")
        return []
    files = [o for o in objs if o.object_type == workspace.ObjectType.FILE]
    files.sort(key=lambda o: getattr(o, "modified_at", 0) or 0, reverse=True)
    return [
        {"path": o.path, "name": PurePosixPath(o.path).name}
        for o in files[:limit]
    ]


# ============================================================================
# 2. Jobs API — runs/submit (one-off)
# ============================================================================

def trigger_run(
    config_workspace_path: str,
    env: str = "dev",
    cluster_id: Optional[str] = None,
    notify_emails: Optional[List[str]] = None,
    run_name_suffix: str = "",
) -> Dict[str, str | int]:
    """
    Submit a one-off run of NoteBook_Main_Param with the given config path.

    Args:
        config_workspace_path : full Workspace path to the generated YAML.
        env                   : 'dev' | 'prod' — picks env-overrides/{env}.yaml.
        cluster_id            : optional override; falls back to DEFAULT_CLUSTER_ID.
        notify_emails         : on_failure recipients.
        run_name_suffix       : appended to the run name for easy filtering.

    Returns: {"run_id": int, "run_page_url": str}
    """
    w = _client()
    target_cluster = cluster_id or DEFAULT_CLUSTER_ID
    if not target_cluster:
        raise RuntimeError(
            "No cluster_id provided and DBX_DEFAULT_CLUSTER_ID is not set in app config."
        )

    env_yaml_path = str(PurePosixPath(WORKSPACE_ENV_DIR) / f"{env}.yaml")

    run_name = f"dbx-ingestion-ui-{run_name_suffix or 'run'}-{datetime.utcnow().strftime('%H%M%S')}"

    notebook_task = jobs.NotebookTask(
        notebook_path=NOTEBOOK_PATH,
        base_parameters={
            "config_path": config_workspace_path,
            "env_path":    env_yaml_path,
        },
    )

    submitted = w.jobs.submit(
        run_name=run_name,
        tasks=[
            jobs.SubmitTask(
                task_key="bronze_ingestion",
                notebook_task=notebook_task,
                existing_cluster_id=target_cluster,
                # Per-task email_notifications keeps blast radius small
                email_notifications=jobs.JobEmailNotifications(
                    on_failure=notify_emails or [],
                ) if notify_emails else None,
            )
        ],
    )
    run_id = submitted.run_id

    # Build a deep link to the Runs UI
    run_page_url = f"{w.config.host.rstrip('/')}/#job/runs/{run_id}"
    logger.info(f"Submitted run_id={run_id} → {run_page_url}")
    return {"run_id": int(run_id), "run_page_url": run_page_url}


# ============================================================================
# 3. Run polling
# ============================================================================

def get_run(run_id: int) -> Dict:
    """Return state, result_state, timing, and the deep-link URL for a run."""
    w = _client()
    r = w.jobs.get_run(run_id=run_id)

    state         = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "UNKNOWN"
    result_state  = r.state.result_state.value if r.state and r.state.result_state else None
    run_page_url  = f"{w.config.host.rstrip('/')}/#job/runs/{run_id}"

    return {
        "run_id":       int(run_id),
        "state":        state,
        "result_state": result_state,
        "start_time":   r.start_time,
        "end_time":     r.end_time,
        "run_page_url": run_page_url,
    }


def cancel_run(run_id: int) -> None:
    """Cancel a run that's still RUNNING / PENDING."""
    _client().jobs.cancel_run(run_id=run_id)
