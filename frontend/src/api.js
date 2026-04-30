// ============================================================================
// api.js — thin fetch wrapper around the FastAPI backend.
// All endpoints documented in backend/app.py.
// ============================================================================

const BASE = "/api";

async function jsonOrThrow(res) {
  const ct = res.headers.get("content-type") || "";
  const body = ct.includes("application/json") ? await res.json() : await res.text();
  if (!res.ok) {
    const msg = typeof body === "string" ? body : (body.detail?.message || body.detail || JSON.stringify(body));
    const err = new Error(msg);
    err.payload = body;
    err.status  = res.status;
    throw err;
  }
  return body;
}

const get  = (p)        => fetch(`${BASE}${p}`).then(jsonOrThrow);
const post = (p, body)  => fetch(`${BASE}${p}`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
}).then(jsonOrThrow);

export const api = {
  // Metadata
  dbTypes:   ()      => get("/db-types"),
  template:  (db)    => get(`/templates/${encodeURIComponent(db)}`),

  // Validate / preview
  validate:    (cfg) => post("/validate", cfg),
  generateYaml:(cfg) => post("/generate-yaml", cfg),

  // Workspace files
  saveConfig:  (cfg, overwrite=false) => post("/save-config", { config: cfg, overwrite }),
  listConfigs: ()    => get("/configs"),
  readConfig:  (path)=> get(`/configs/raw?path=${encodeURIComponent(path)}`),

  // Runs
  run:         (cfg, env="dev", cluster_id=null, notify_emails=[]) =>
                 post("/run", { config: cfg, env, cluster_id, notify_emails }),
  runStatus:   (id, pipelineName) =>
                 get(`/runs/${id}${pipelineName ? `?pipeline_name=bronze_${encodeURIComponent(pipelineName)}` : ""}`),
  cancelRun:   (id) => post(`/runs/${id}/cancel`, {}),
  history:     (limit=50, source=null) =>
                 get(`/runs?limit=${limit}${source ? `&source=${encodeURIComponent(source)}` : ""}`),

  // Schema introspection — "Browse source DB"
  // Uses Lakehouse Federation: source DBs must be registered as Foreign
  // Catalogs in Unity Catalog. The app reads system.information_schema
  // through the SQL Warehouse — no JDBC drivers, no cluster boot.
  // Results cached server-side for 5 minutes.
  introspectSchemas: (foreignCatalog, force=false) =>
    post("/introspect/schemas", { foreign_catalog: foreignCatalog, force }),
  introspectTables:  (foreignCatalog, schema, force=false) =>
    post("/introspect/tables",  { foreign_catalog: foreignCatalog, schema, force }),
  introspectColumns: (foreignCatalog, schema, table, force=false) =>
    post("/introspect/columns", {
      foreign_catalog: foreignCatalog, schema, table, force,
      suggest_watermarks: true,
    }),
  clearIntrospectCache: () => post("/introspect/cache/clear", {}),
};
