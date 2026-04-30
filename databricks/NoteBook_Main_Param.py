# Databricks notebook source
# ============================================================================
# NoteBook_Main_Param.py
# ----------------------------------------------------------------------------
# Parameterized version of NoteBook_Main.py. The Databricks Ingestion UI
# triggers this notebook via Jobs runs/submit and passes:
#
#     config_path  → /Workspace/.../sources/generated/<source>_<ts>.yaml
#     env_path     → /Workspace/.../env-overrides/<env>.yaml
#
# This is the only NEW file we add to the existing repo — everything else
# (run_pipeline.py, bronze_rdbms_multi_template.py, etc.) is untouched.
# ============================================================================
import sys

# Same path-bootstrap as the existing notebook so we can import platform-core
sys.path.insert(
    0,
    "/Workspace/Shared/dbx_ingestion/Data-Platform/platform-core",
)

from run_pipeline import run

# COMMAND ----------

# ── Pull params injected by the UI / Jobs API ────────────────────────────────
# dbutils is provided by the Databricks runtime — the linter doesn't see it.
dbutils.widgets.text("config_path", "", "Path to source config YAML")    # noqa: F821
dbutils.widgets.text("env_path",    "", "Path to env-overrides YAML")    # noqa: F821

config_path = dbutils.widgets.get("config_path")    # noqa: F821
env_path    = dbutils.widgets.get("env_path")       # noqa: F821

if not config_path:
    raise ValueError("config_path widget is required (set by the UI / Jobs API).")
if not env_path:
    raise ValueError("env_path widget is required (set by the UI / Jobs API).")

print(f"config_path = {config_path}")
print(f"env_path    = {env_path}")

# COMMAND ----------

# ── Hand off to the existing dispatcher — zero changes downstream ───────────
run(config_path=config_path, env_path=env_path)
