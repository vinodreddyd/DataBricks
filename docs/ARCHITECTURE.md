# Databricks Ingestion UI — Architecture & Design

## 1. Goal

Replace the current "manually edit YAML → run notebook" workflow with a self-service Databricks App where data engineers fill in a form (DB type, host, tables, etc.), preview the generated YAML, and click **Run** to trigger the existing Bronze pipeline.

No change to your existing `platform-core` code — the app generates the same YAML you write today, drops it into Workspace Files, and triggers your existing notebook via the Databricks Jobs API.

---

## 2. High-level architecture

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │                          BROWSER (user)                              │
 │   React UI — DB-type dropdown, table builder, YAML preview, Run btn  │
 └──────────────────────────────┬───────────────────────────────────────┘
                                │ REST (JSON)
                                ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │              DATABRICKS APP  (FastAPI, Python 3.11)                  │
 │                                                                      │
 │   /api/db-types          → list of supported RDBMS                   │
 │   /api/templates/{db}    → default port, driver, secret keys         │
 │   /api/validate          → YAML schema validation                    │
 │   /api/generate-yaml     → build YAML from form payload              │
 │   /api/save-config       → write YAML to Workspace Files             │
 │   /api/test-connection   → JDBC ping (optional, uses dbutils)        │
 │   /api/run               → trigger Databricks Job (Jobs 2.1 API)     │
 │   /api/runs/{run_id}     → poll job status & logs                    │
 │   /api/runs              → list recent runs (history)                │
 └──────────────────────────────┬───────────────────────────────────────┘
                                │
        ┌───────────────────────┼─────────────────────────┐
        ▼                       ▼                         ▼
 ┌──────────────┐    ┌────────────────────┐    ┌────────────────────┐
 │ Workspace    │    │  Databricks Jobs   │    │  Unity Catalog     │
 │   Files API  │    │       API 2.1      │    │   (run history,    │
 │              │    │                    │    │    pipeline_runs)  │
 │  writes:     │    │  triggers:         │    │                    │
 │  /Workspace/ │    │  NoteBook_Main.py  │    │  ops_dev.platform  │
 │   .../sources│    │  with notebook_    │    │  _meta.pipeline_   │
 │   /<gen>.yaml│    │  params: config_   │    │  runs              │
 │              │    │  path, env_path    │    │                    │
 └──────────────┘    └────────────────────┘    └────────────────────┘
```

### Why a Databricks App (and not just a notebook widget)?

| Concern                   | Notebook Widgets                                  | Databricks App ✅                     |
| ------------------------- | ------------------------------------------------- | ------------------------------------ |
| UX                        | Limited — flat list of widgets, no dynamic forms  | Real React UI, dynamic table builder |
| Persistence               | None — re-fill every run                          | Saves config & run history           |
| Multi-user                | One user per notebook                             | Shared, team-wide                    |
| Auth                      | Uses notebook owner                               | OAuth on-behalf-of-user              |
| Production fit            | Not a deliverable                                 | First-class app surface              |

---

## 3. Data flow — one end-to-end run

1. **User opens the app** in Databricks → `https://<workspace>/apps/dbx-ingestion-ui`
2. **Picks DB type** from dropdown → `redshift`. Frontend calls `GET /api/templates/redshift` and pre-fills port `5439`, driver, default secret keys.
3. **Fills connection details** — host, database, secret scope, secret keys.
4. **Adds tables** one by one. Each table card has its own ingestion mode, watermarks, partition cols, target schema/table, merge keys, tags.
5. **Clicks "Preview YAML"** — UI shows the rendered YAML (read-only) for sanity check.
6. **Clicks "Save Config"** — backend writes YAML to `/Workspace/Shared/dbx_ingestion/domain-configs/sources/<source_name>_<timestamp>.yaml`.
7. **Clicks "Run"** — backend calls Jobs API `runs/submit` with:
   ```json
   {
     "tasks": [{
       "task_key": "bronze_ingestion",
       "notebook_task": {
         "notebook_path": "/Workspace/.../NoteBook_Main_Param.py",
         "base_parameters": {
           "config_path": "/Workspace/.../sources/<file>.yaml",
           "env_path":    "/Workspace/.../env-overrides/dev.yaml"
         }
       },
       "existing_cluster_id": "<configured>"
     }]
   }
   ```
8. UI polls `GET /api/runs/{run_id}` every 3 s for status. Shows progress, log links, final row counts pulled from `platform_meta.pipeline_runs`.

---

## 4. What the app does NOT change

* `platform-core/run_pipeline.py` — untouched
* `platform-core/ingestion/bronze_rdbms_multi_template.py` — untouched
* `domain-configs/env-overrides/dev.yaml` — untouched
* The YAML schema — the app emits *the exact same shape* you write today

The only new artifact in your repo is **`NoteBook_Main_Param.py`**, a thin wrapper of `NoteBook_Main.py` that reads `config_path` and `env_path` from `dbutils.widgets` instead of being hardcoded. Diff is ~5 lines.

---

## 5. Module breakdown

| Path                                          | Purpose                                            |
| --------------------------------------------- | -------------------------------------------------- |
| `backend/app.py`                              | FastAPI entry, routes, CORS                        |
| `backend/db_registry.py`                      | DB-type → port/driver/secrets registry            |
| `backend/yaml_builder.py`                     | Form-payload → YAML (matches your existing schema) |
| `backend/databricks_client.py`                | Workspace Files + Jobs API wrapper                 |
| `backend/run_history.py`                      | Reads `platform_meta.pipeline_runs` via SQL Warehouse |
| `backend/models.py`                           | Pydantic models (request/response)                 |
| `backend/requirements.txt`                    | Dependencies (Databricks App–compatible)           |
| `backend/app.yaml`                            | Databricks App manifest                            |
| `frontend/src/App.jsx`                        | React shell, tabs (Build / Preview / Run / History)|
| `frontend/src/components/ConnectionForm.jsx`  | DB type + connection card                          |
| `frontend/src/components/TableBuilder.jsx`    | Add/remove tables, per-table editor                |
| `frontend/src/components/YamlPreview.jsx`     | Monaco read-only YAML view                         |
| `frontend/src/components/RunStatus.jsx`       | Live run status panel                              |
| `frontend/src/components/RunHistory.jsx`      | Last 50 runs                                       |
| `frontend/src/api.js`                         | Fetch wrappers                                     |
| `databricks/NoteBook_Main_Param.py`           | Parameterized notebook (5-line wrapper)            |
| `databricks/databricks.yml`                   | Asset bundle for app + job deploy                  |

---

## 6. Security model

* **Auth**: Databricks Apps automatically inject the calling user's OAuth token. Backend uses it for all Workspace + Jobs API calls — *no service principal credentials in code*.
* **Secrets**: The app never sees DB passwords. The user types only the *secret-scope name* and *secret-key name*; the actual JDBC fetch happens inside the Spark cluster via `dbutils.secrets.get()` (your existing flow).
* **Workspace Files writes**: scoped to a single folder (`/Workspace/Shared/dbx_ingestion/domain-configs/sources/generated/`). The app cannot overwrite your hand-authored configs.
* **Job runs**: use `runs/submit` (one-off) with `run_as` = calling user.

---

## 7. Deployment

### Option A — Databricks Asset Bundle (recommended)

```bash
cd databricks/
databricks bundle deploy --target dev   # creates app + uploads code
databricks bundle run dbx-ingestion-app --target dev
```

The bundle (`databricks.yml`) defines:
* the app resource (points at `backend/`)
* the parameterized notebook upload
* the existing-cluster reference (or job-cluster spec) used by `runs/submit`

### Option B — Manual

1. `databricks apps create dbx-ingestion-ui --source-code-path /Workspace/.../backend`
2. `databricks apps deploy dbx-ingestion-ui`
3. Open the app URL printed by the CLI.

---

## 8. Roadmap (post-MVP)

* **Silver/Gold layer support** — same UI pattern, new tab. Backend dispatches to a `silver_pipeline` notebook once your team builds it.
* **Schedule tab** — wraps Jobs API `jobs/create` with cron, instead of one-off `runs/submit`.
* **Diff view** — when re-running an existing config, show a YAML diff before save.
* **Connection test** — backend submits a tiny "SELECT 1" notebook against the chosen secret scope; returns latency + driver version.
* **DLT mode** — toggle to emit a Delta Live Tables pipeline definition instead of a Jobs run.
