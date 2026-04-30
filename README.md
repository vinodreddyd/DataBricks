# Databricks Ingestion UI

Self-service Databricks App that replaces the manual "edit YAML → run notebook" workflow with a form. Users pick a DB type, fill in connection details, add tables, preview the generated YAML, and click **Run** to trigger your existing Bronze ingestion pipeline.

> **Zero changes to your existing platform code.** The app emits the exact same YAML schema you author by hand today and triggers your existing dispatcher (`run_pipeline.py`) via the Jobs API.

---

## Repo layout

```
dbx_ingestion_app/
├── docs/
│   └── ARCHITECTURE.md          ← deeper design walkthrough
├── backend/                     ← FastAPI app (deployed as the Databricks App)
│   ├── app.py                   ← routes (incl. /api/introspect/*)
│   ├── app.yaml                 ← Databricks App manifest
│   ├── db_registry.py           ← supported RDBMS catalog
│   ├── databricks_client.py     ← Workspace Files + Jobs API
│   ├── schema_introspection.py  ← Lakehouse Federation schema browser
│   ├── models.py                ← Pydantic schemas
│   ├── run_history.py           ← reads platform_meta.pipeline_runs
│   ├── yaml_builder.py          ← form → YAML rendering + validation
│   ├── requirements.txt
│   └── static/                  ← (built frontend lands here)
├── frontend/                    ← React + Vite (compiled into backend/static)
│   ├── package.json
│   ├── vite.config.js
│   ├── tailwind.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx
│       ├── api.js
│       └── components/
│           ├── ConnectionForm.jsx
│           ├── TableBuilder.jsx
│           ├── SchemaBrowser.jsx   ← schema/table browser modal
│           ├── ColumnPicker.jsx    ← watermark/merge-key column picker
│           ├── YamlPreview.jsx
│           ├── RunStatus.jsx
│           └── RunHistory.jsx
└── databricks/
    ├── databricks.yml           ← Asset Bundle for one-command deploy
    └── NoteBook_Main_Param.py   ← the only NEW file added to your existing repo
```

---

## How it fits with your existing code

```
                          (existing, unchanged)
   ┌─────────────────────────────────────────────────────────────┐
   │  Data-Platform/                                             │
   │    NoteBook_Main.py     ← still works for hand-authored YAMLs│
   │    NoteBook_Main_Param.py  ← NEW thin wrapper, 5 lines      │
   │    domain-configs/                                          │
   │      sources/                                               │
   │        redshift_multi_table.yaml      ← hand-authored        │
   │        sqlserver_multi_table.yaml                            │
   │        oracle_multi_table.yaml                               │
   │        generated/                     ← NEW dir, app writes here│
   │          redshift_finance_20260430T120100Z.yaml             │
   │      env-overrides/dev.yaml                                 │
   │    platform-core/                                           │
   │      run_pipeline.py     ← unchanged dispatcher              │
   │      ingestion/                                             │
   │        bronze_rdbms_multi_template.py  ← unchanged           │
   └─────────────────────────────────────────────────────────────┘
```

The UI ALWAYS routes through `NoteBook_Main_Param.py → run_pipeline.run(config_path, env_path)`. Your existing notebook keeps working for cases where you'd rather author YAML by hand.

---

## Local dev

```bash
# Terminal 1 — backend (mocks Databricks SDK against env vars)
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<personal-access-token>
export DBX_DEFAULT_CLUSTER_ID=<dev-cluster-id>
export DBX_WAREHOUSE_ID=<dev-warehouse-id>
uvicorn app:app --reload --port 8000

# Terminal 2 — frontend
cd frontend
npm install
npm run dev      # opens http://localhost:5173, proxies /api → :8000
```

## Production build

```bash
cd frontend
npm run build    # outputs to ../backend/static/
```

Now `backend/` contains everything needed for the App (FastAPI + bundled SPA).

## Deploy as a Databricks App

### Option A — Asset Bundle (recommended)

```bash
# Edit databricks/databricks.yml: set workspace host, cluster id, warehouse id.
cd databricks
databricks bundle deploy --target dev
databricks bundle run dbx-ingestion-app --target dev
```

The bundle:
1. Uploads `NoteBook_Main_Param.py` to `/Workspace/Shared/dbx_ingestion/Data-Platform/`
2. Creates the App pointing at `backend/`
3. Wires the App to the cluster + SQL warehouse you supplied

### Option B — Manual

```bash
# 1. Push the parameterized notebook
databricks workspace import \
   databricks/NoteBook_Main_Param.py \
   /Workspace/Shared/dbx_ingestion/Data-Platform/NoteBook_Main_Param \
   --language PYTHON

# 2. Deploy the app
databricks apps create dbx-ingestion-ui \
   --source-code-path /Workspace/Users/me@company.com/apps/dbx-ingestion-ui
databricks apps deploy dbx-ingestion-ui
```

---

## What the user sees

| Tab | What it does |
|-----|-------------|
| **Build** | Two-pane form. Left: source identity + JDBC connection. Right: dynamic list of tables. DB-type dropdown drives port/driver/secret defaults. |
| **Preview** | Read-only Monaco-style view of the rendered YAML — exactly what gets uploaded. |
| **Run** | Live status of the latest run. Polls Jobs API every 3 s. Surfaces row counts from `platform_meta.pipeline_runs` once the run finishes. |
| **History** | Last 50 runs from `platform_meta.pipeline_runs`. Filter by pipeline name. |

Top-right action bar: **Preview YAML**, **Save** (just writes the YAML), **Run pipeline** (saves + submits a job).

---

## Schema browser (optional but powerful)

Instead of typing fully-qualified table names and watermark column names by hand, users can **browse the source database**. This works through Lakehouse Federation:

1. A workspace admin registers the source DB as a **Foreign Catalog** in Unity Catalog (one-time setup, done outside this app — see [Databricks docs on query federation](https://docs.databricks.com/en/query-federation/index.html)).
2. The user pastes that catalog name into the **Foreign catalog** field on the connection panel.
3. The "Browse" button next to *Source table* and "Pick columns" buttons next to *Watermark columns / Merge keys / partitionColumn* now light up.
4. Clicks read `<catalog>.information_schema.{schemata, tables, columns}` through the App's SQL Warehouse — sub-second response.

**What's NOT collected by the app**: The schema browser uses the SAME SQL Warehouse the History tab uses. It never opens a JDBC connection to the source DB itself, never sees the user/password secret values, and never bypasses Unity Catalog permissions.

**What if the user doesn't set a Foreign catalog?** The app falls back to manual entry — the Browse buttons are disabled with a helpful tooltip, but everything else still works.

**Watermark suggestions**: When the user picks a table, the column picker pre-checks columns that look like good incremental watermarks based on a name + type heuristic (`updated_at`, `*_ts`, monotonic ints, etc.) — they can override the picks before applying.

**Cache**: Introspection results are memoised for 5 minutes (configurable via `DBX_INTROSPECT_CACHE_TTL_S`). The Refresh button in each modal busts the cache for that scope.

---

## Adding a new database

Two-step change in `db_registry.py`:

```python
"snowflake": DbTemplate(
    db_type="snowflake",
    display_name="Snowflake",
    default_port=443,
    jdbc_driver="net.snowflake.client.jdbc.SnowflakeDriver",
    user_secret_default="snowflake_user",
    password_secret_default="snowflake_password",
    suggested_read_options={"fetchsize": "10000"},
),
```

Then add the matching row to `JDBC_DRIVERS` in `bronze_rdbms_multi_template.py`. The UI picks it up immediately.

---

## Security notes

* The app **never sees DB passwords** — only the Databricks secret-scope and key NAMES. Actual credentials are read by the Spark cluster via `dbutils.secrets.get()`.
* Generated YAMLs are written to a single fenced directory (`/Workspace/Shared/dbx_ingestion/domain-configs/sources/generated/`) so the app cannot overwrite hand-authored configs.
* Jobs are submitted with `runs/submit` (one-off) and `run_as` defaults to the calling user — no shared service-principal credentials in the app code.

See `docs/ARCHITECTURE.md` for the full design.
