# ============================================================================
# app.py
# ----------------------------------------------------------------------------
# FastAPI entry point for the Databricks Ingestion UI.
#
# Endpoints (all under /api):
#   GET  /db-types            list of supported RDBMS for the dropdown
#   GET  /templates/{db_type} per-DB defaults (port, driver, secret keys)
#   POST /validate            run yaml_builder.validate_config()
#   POST /generate-yaml       form payload → YAML string preview
#   POST /save-config         write YAML to Workspace Files
#   POST /run                 save + trigger Jobs runs/submit
#   GET  /runs/{run_id}       poll run state
#   POST /runs/{run_id}/cancel  cancel an in-flight run
#   GET  /runs                run history from platform_meta.pipeline_runs
#   GET  /configs             list previously generated YAMLs
#   GET  /configs/{name}      read a saved YAML back into the form
#   GET  /healthz             liveness probe
#
# Static files (the React build) are served from /static and the SPA is
# returned at the root path. In Databricks Apps both backend and frontend
# run inside the same container, so this is the single deployable artifact.
# ============================================================================

import logging
import os
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from db_registry import (
    DB_REGISTRY,
    DEFAULT_WATERMARK_COLUMNS,
    INGESTION_MODES,
    WRITE_MODES,
    get_template,
    list_db_types,
)
from databricks_client import (
    get_run,
    cancel_run,
    list_generated_configs,
    read_yaml_from_workspace,
    save_yaml_to_workspace,
    trigger_run,
)
from models import (
    GenerateYamlResponse,
    IngestionConfigForm,
    RunRequest,
    RunResponse,
    RunStatus,
    SaveConfigRequest,
    SaveConfigResponse,
    ValidateResponse,
)
from run_history import get_run_metrics, list_recent_runs
from schema_introspection import (
    FederationRef,
    cache_clear,
    introspect_columns,
    introspect_schemas,
    introspect_tables,
    suggest_watermark_columns,
)
from yaml_builder import build as build_yaml, validate_config


# ── Logging setup ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dbx_ingestion_app")


app = FastAPI(
    title="Databricks Ingestion UI",
    version="1.0.0",
    description="Self-service form for generating & running Bronze ingestion configs.",
)

# CORS — only relevant during local dev (Vite on :5173 calling FastAPI on :8000).
# In Databricks Apps, frontend & backend share an origin so this is a no-op.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5173").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Health
# ============================================================================

@app.get("/api/healthz")
def healthz():
    return {"status": "ok", "ts": datetime.utcnow().isoformat() + "Z"}


# ============================================================================
# Metadata endpoints (drive the form's dropdowns)
# ============================================================================

@app.get("/api/db-types")
def db_types():
    """Populates the 'DB Type' dropdown. Returns [{value, label}, ...]."""
    return {
        "db_types":            list_db_types(),
        "ingestion_modes":     INGESTION_MODES,
        "write_modes":         WRITE_MODES,
        "default_watermarks":  DEFAULT_WATERMARK_COLUMNS,
    }


@app.get("/api/templates/{db_type}")
def db_template(db_type: str):
    """
    Per-DB defaults so the user only types host + DB name.
    Frontend calls this on every dropdown change.
    """
    try:
        return get_template(db_type).model_dump()
    except ValueError as e:
        raise HTTPException(404, str(e))


# ============================================================================
# Validation & YAML preview
# ============================================================================

@app.post("/api/validate", response_model=ValidateResponse)
def validate(cfg: IngestionConfigForm):
    """Live validation. Frontend calls on blur of any required field."""
    ok, issues = validate_config(cfg)
    return ValidateResponse(ok=ok, issues=issues)


@app.post("/api/generate-yaml", response_model=GenerateYamlResponse)
def generate_yaml(cfg: IngestionConfigForm):
    """
    Render the form payload to YAML *without* saving — the UI shows this
    in a Monaco editor for sanity check before clicking Save or Run.
    """
    ok, issues = validate_config(cfg)
    if not ok:
        raise HTTPException(400, {
            "message": "Validation failed",
            "issues":  [i.model_dump() for i in issues],
        })

    filename = f"{cfg.source_name}.yaml"
    yaml_text = build_yaml(
        cfg,
        filename=filename,
        generated_at=datetime.utcnow().isoformat() + "Z",
    )
    return GenerateYamlResponse(yaml_text=yaml_text)


# ============================================================================
# Workspace Files: save / list / read
# ============================================================================

@app.post("/api/save-config", response_model=SaveConfigResponse)
def save_config(req: SaveConfigRequest):
    """Validate → render → upload to /Workspace/.../sources/generated/."""
    ok, issues = validate_config(req.config)
    if not ok:
        raise HTTPException(400, {
            "message": "Validation failed",
            "issues":  [i.model_dump() for i in issues],
        })

    yaml_text = build_yaml(
        req.config,
        filename=f"{req.config.source_name}.yaml",
        generated_at=datetime.utcnow().isoformat() + "Z",
    )
    res = save_yaml_to_workspace(yaml_text, req.config.source_name, overwrite=req.overwrite)
    return SaveConfigResponse(**res)


@app.get("/api/configs")
def configs(limit: int = 50):
    """List recent generated YAML files (for the 'Re-run' dropdown)."""
    return {"configs": list_generated_configs(limit=limit)}


@app.get("/api/configs/raw")
def get_config_raw(path: str):
    """Read a saved YAML by full Workspace path. Used for 'Re-load into form'."""
    try:
        return {"path": path, "yaml_text": read_yaml_from_workspace(path)}
    except Exception as e:                       # noqa: BLE001
        raise HTTPException(404, f"Could not read {path}: {e}")


# ============================================================================
# Pipeline trigger & status
# ============================================================================

@app.post("/api/run", response_model=RunResponse)
def run(req: RunRequest):
    """
    The 'Run' button.
    1. Validate the form.
    2. Render YAML.
    3. Save to Workspace Files.
    4. Submit a one-off Jobs run.
    """
    ok, issues = validate_config(req.config)
    if not ok:
        raise HTTPException(400, {
            "message": "Validation failed",
            "issues":  [i.model_dump() for i in issues],
        })

    # 1+2. Render & save
    yaml_text = build_yaml(
        req.config,
        filename=f"{req.config.source_name}.yaml",
        generated_at=datetime.utcnow().isoformat() + "Z",
    )
    saved = save_yaml_to_workspace(yaml_text, req.config.source_name, overwrite=False)

    # 3. Submit run
    submit = trigger_run(
        config_workspace_path=saved["workspace_path"],
        env=req.env,
        cluster_id=req.cluster_id,
        notify_emails=req.notify_emails,
        run_name_suffix=req.config.source_name,
    )
    return RunResponse(
        run_id=submit["run_id"],
        run_page_url=submit["run_page_url"],
        workspace_config_path=saved["workspace_path"],
    )


@app.get("/api/runs/{run_id}", response_model=RunStatus)
def run_status(run_id: int, pipeline_name: str | None = None):
    """
    Polled by the UI every 3s while a run is in flight.
    Once TERMINATED & SUCCESS, also enriches with row_count/duration
    pulled from platform_meta.pipeline_runs (best-effort).
    """
    s = get_run(run_id)
    metrics = None
    if s["state"] == "TERMINATED" and s["result_state"] == "SUCCESS" and pipeline_name:
        metrics = get_run_metrics(pipeline_name)
    return RunStatus(**s, pipeline_metrics=metrics)


@app.post("/api/runs/{run_id}/cancel")
def cancel(run_id: int):
    cancel_run(run_id)
    return {"ok": True, "run_id": run_id}


@app.get("/api/runs")
def runs_history(limit: int = 50, source: str | None = None):
    """Recent runs from platform_meta.pipeline_runs (newest first)."""
    return {"runs": list_recent_runs(limit=limit, pipeline_name_filter=source)}


# ============================================================================
# Schema introspection — "Browse the source DB" feature
# ----------------------------------------------------------------------------
# Uses Lakehouse Federation: the source DB must already be registered as a
# Foreign Catalog in Unity Catalog. We then read system.information_schema
# through a SQL Warehouse — sub-second, no JDBC drivers, no cluster boot,
# no Secret Scopes touched on the read path.
#
# Caching: results memoised for 5 minutes per (catalog, schema, table)
# tuple. Pass force=true to bust the cache.
# ============================================================================

from pydantic import BaseModel, Field

class IntrospectSchemasRequest(BaseModel):
    foreign_catalog: str = Field(..., min_length=1,
        description="UC foreign catalog registered via Lakehouse Federation.")
    force: bool = False


class IntrospectTablesRequest(BaseModel):
    foreign_catalog: str = Field(..., min_length=1)
    schema: str          = Field(..., min_length=1)
    force: bool = False


class IntrospectColumnsRequest(BaseModel):
    foreign_catalog: str = Field(..., min_length=1)
    schema: str          = Field(..., min_length=1)
    table:  str          = Field(..., min_length=1)
    force: bool = False
    suggest_watermarks: bool = True


@app.post("/api/introspect/schemas")
def api_introspect_schemas(req: IntrospectSchemasRequest):
    """List schemas in a foreign catalog."""
    ref = FederationRef(foreign_catalog=req.foreign_catalog.strip())
    try:
        rows = introspect_schemas(ref, force=req.force)
        return {"schemas": rows, "count": len(rows),
                "foreign_catalog": ref.foreign_catalog}
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:                           # noqa: BLE001
        logger.exception("introspect_schemas failed")
        raise HTTPException(502, str(e))


@app.post("/api/introspect/tables")
def api_introspect_tables(req: IntrospectTablesRequest):
    """List tables and views inside a given schema."""
    ref = FederationRef(foreign_catalog=req.foreign_catalog.strip())
    try:
        rows = introspect_tables(ref, schema=req.schema.strip(), force=req.force)
        return {"tables": rows, "schema": req.schema, "count": len(rows),
                "foreign_catalog": ref.foreign_catalog}
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:                           # noqa: BLE001
        logger.exception("introspect_tables failed")
        raise HTTPException(502, str(e))


@app.post("/api/introspect/columns")
def api_introspect_columns(req: IntrospectColumnsRequest):
    """List columns for a given schema.table — and suggest watermark columns."""
    ref = FederationRef(foreign_catalog=req.foreign_catalog.strip())
    try:
        cols = introspect_columns(
            ref,
            schema=req.schema.strip(),
            table=req.table.strip(),
            force=req.force,
        )
        suggestions = suggest_watermark_columns(cols) if req.suggest_watermarks else []
        return {
            "columns":              cols,
            "schema":               req.schema,
            "table":                req.table,
            "count":                len(cols),
            "watermark_suggestions": suggestions,
            "foreign_catalog":      ref.foreign_catalog,
        }
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:                           # noqa: BLE001
        logger.exception("introspect_columns failed")
        raise HTTPException(502, str(e))


@app.post("/api/introspect/cache/clear")
def api_introspect_cache_clear():
    """Drop the in-memory introspection cache (Refresh button)."""
    return {"cleared": cache_clear()}


# ============================================================================
# Static frontend (React build) — served from the same container in Apps
# ============================================================================

_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

    @app.get("/")
    def spa_root():
        return FileResponse(str(_static_dir / "index.html"))

    @app.get("/{full_path:path}")
    def spa_catch_all(full_path: str, request: Request):
        # Don't shadow the API
        if full_path.startswith("api/"):
            raise HTTPException(404, "Not found")
        # Serve the file if it exists, otherwise fall back to index.html (SPA routing)
        target = _static_dir / full_path
        if target.is_file():
            return FileResponse(str(target))
        return FileResponse(str(_static_dir / "index.html"))


# ============================================================================
# Generic error handler — keeps the UI robust against backend stack traces
# ============================================================================

@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    logger.exception(f"Unhandled error on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": exc.__class__.__name__},
    )
