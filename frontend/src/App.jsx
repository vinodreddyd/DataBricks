// ============================================================================
// App.jsx
// ----------------------------------------------------------------------------
// Top-level shell. Owns the form state, tab switching, validation, and the
// run lifecycle (save → submit → poll). Delegates UI to per-tab components.
//
// State shape mirrors the backend's IngestionConfigForm exactly so the
// payload to /api/run is just JSON.stringify(state.config).
// ============================================================================

import React, { useEffect, useMemo, useState } from "react";
import {
  Database, Play, Save, FileCode2, History, AlertTriangle,
  CheckCircle2, RefreshCw, X,
} from "lucide-react";
import { api } from "./api";
import ConnectionForm from "./components/ConnectionForm.jsx";
import TableBuilder   from "./components/TableBuilder.jsx";
import YamlPreview    from "./components/YamlPreview.jsx";
import RunStatus      from "./components/RunStatus.jsx";
import RunHistory     from "./components/RunHistory.jsx";

// ── Initial form state — empty values with sensible types ──────────────────
const emptyConfig = () => ({
  version: "1.0.0",
  fail_fast: false,
  source_name: "",
  source_type: "rdbms_multi",
  connection: {
    db_type: "redshift",
    host: "",
    port: 5439,
    database: "",
    secret_scope: "__SECRETS_SCOPE__",
    user_secret: "redshift_user",
    password_secret: "redshift_password",
    // Optional — enables the "Browse" buttons in the table editor when set.
    // Must be a Foreign Catalog registered in Unity Catalog via Lakehouse
    // Federation. Has no impact on the generated YAML or pipeline runtime.
    foreign_catalog: "",
  },
  tables: [],
  target_catalog: "__CATALOG__",
  write_mode: "upsert",
  schema_evolution: "add_columns",
});

const TABS = [
  { key: "build",   label: "Build",   icon: Database },
  { key: "preview", label: "Preview", icon: FileCode2 },
  { key: "run",     label: "Run",     icon: Play },
  { key: "history", label: "History", icon: History },
];

export default function App() {
  const [tab, setTab]               = useState("build");
  const [config, setConfig]         = useState(emptyConfig());
  const [meta, setMeta]             = useState(null);   // /db-types response
  const [issues, setIssues]         = useState([]);
  const [yamlText, setYamlText]     = useState("");
  const [activeRun, setActiveRun]   = useState(null);   // {run_id, pipeline_name, ...}
  const [busy, setBusy]             = useState(false);
  const [toast, setToast]           = useState(null);

  // ── Boot: pull metadata for dropdowns
  useEffect(() => { api.dbTypes().then(setMeta).catch(e => showToast("error", e.message)); }, []);

  // ── Live validation (debounced)
  useEffect(() => {
    const t = setTimeout(async () => {
      try {
        const v = await api.validate(config);
        setIssues(v.issues || []);
      } catch (_) { /* silent — validation errors handled per-action */ }
    }, 400);
    return () => clearTimeout(t);
  }, [config]);

  const errorCount   = useMemo(() => issues.filter(i => i.severity === "error").length, [issues]);
  const warningCount = useMemo(() => issues.filter(i => i.severity === "warning").length, [issues]);

  // ── Toast helper
  function showToast(kind, msg) {
    setToast({ kind, msg, ts: Date.now() });
    setTimeout(() => setToast(t => (t && t.ts === setToast.lastTs ? null : t)), 4000);
  }

  // ── Generate YAML preview (server-side render, identical to deploy output)
  async function onPreview() {
    setBusy(true);
    try {
      const { yaml_text } = await api.generateYaml(config);
      setYamlText(yaml_text);
      setTab("preview");
    } catch (e) {
      showToast("error", e.message);
      // surface server-side validation issues if present
      if (e.payload?.detail?.issues) setIssues(e.payload.detail.issues);
    } finally { setBusy(false); }
  }

  // ── Save without running
  async function onSave() {
    setBusy(true);
    try {
      const res = await api.saveConfig(config);
      showToast("ok", `Saved → ${res.workspace_path}`);
    } catch (e) {
      showToast("error", e.message);
      if (e.payload?.detail?.issues) setIssues(e.payload.detail.issues);
    } finally { setBusy(false); }
  }

  // ── The big red button: save + submit job + start polling
  async function onRun({ env = "dev", cluster_id = null, notify_emails = [] } = {}) {
    if (errorCount > 0) {
      showToast("error", `Fix ${errorCount} validation error(s) first.`);
      return;
    }
    setBusy(true);
    try {
      const res = await api.run(config, env, cluster_id, notify_emails);
      setActiveRun({
        run_id: res.run_id,
        run_page_url: res.run_page_url,
        config_path:  res.workspace_config_path,
        pipeline_name: config.source_name,
      });
      setTab("run");
      showToast("ok", `Submitted run #${res.run_id}`);
    } catch (e) {
      showToast("error", e.message);
      if (e.payload?.detail?.issues) setIssues(e.payload.detail.issues);
    } finally { setBusy(false); }
  }

  // ── Apply DB-template defaults when user changes db_type dropdown
  async function onDbTypeChange(db_type) {
    try {
      const t = await api.template(db_type);
      setConfig(c => ({
        ...c,
        connection: {
          ...c.connection,
          db_type:         t.db_type,
          port:            t.default_port,
          database:        c.connection.database || t.default_database || "",
          user_secret:     t.user_secret_default,
          password_secret: t.password_secret_default,
        },
      }));
    } catch (e) {
      showToast("error", e.message);
    }
  }

  return (
    <div className="min-h-screen flex flex-col">
      <Header
        errorCount={errorCount}
        warningCount={warningCount}
        busy={busy}
        onPreview={onPreview}
        onSave={onSave}
        onRun={() => onRun()}
      />

      <Tabs current={tab} onChange={setTab} runActive={!!activeRun} />

      <main className="flex-1 max-w-7xl w-full mx-auto px-6 py-6">
        {tab === "build" && (
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
            <div className="lg:col-span-5">
              <ConnectionForm
                config={config}
                setConfig={setConfig}
                meta={meta}
                onDbTypeChange={onDbTypeChange}
                issues={issues}
              />
            </div>
            <div className="lg:col-span-7">
              <TableBuilder
                config={config}
                setConfig={setConfig}
                meta={meta}
                issues={issues}
                foreignCatalog={config.connection.foreign_catalog}
              />
            </div>
            {issues.length > 0 && (
              <div className="lg:col-span-12">
                <ValidationPanel issues={issues} />
              </div>
            )}
          </div>
        )}

        {tab === "preview" && (
          <YamlPreview
            yamlText={yamlText}
            onRefresh={onPreview}
            busy={busy}
            sourceName={config.source_name}
          />
        )}

        {tab === "run" && (
          <RunStatus
            activeRun={activeRun}
            onRunAgain={onRun}
            issues={issues}
          />
        )}

        {tab === "history" && (
          <RunHistory />
        )}
      </main>

      {toast && (
        <div className={`fixed bottom-6 right-6 z-50 card px-4 py-3 flex items-center gap-3
                        ${toast.kind === "error" ? "border-bad/50" : "border-ok/50"}`}>
          {toast.kind === "error"
            ? <AlertTriangle size={16} className="text-bad" />
            : <CheckCircle2  size={16} className="text-ok" />}
          <span className="text-sm">{toast.msg}</span>
          <button onClick={() => setToast(null)} className="text-muted hover:text-text">
            <X size={14} />
          </button>
        </div>
      )}
    </div>
  );
}


// ────────────────────────────────────────────────────────────────────────────
// Header — sticky title + top-level action bar
// ────────────────────────────────────────────────────────────────────────────
function Header({ errorCount, warningCount, busy, onPreview, onSave, onRun }) {
  return (
    <header className="border-b border-line bg-panel/60 backdrop-blur sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-md bg-accent flex items-center justify-center">
            <Database size={16} className="text-black" />
          </div>
          <div>
            <h1 className="text-base font-semibold leading-tight">Databricks Ingestion UI</h1>
            <p className="text-xs text-muted">Bronze layer · RDBMS multi-table</p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <ValidationBadges errorCount={errorCount} warningCount={warningCount} />
          <button onClick={onPreview} disabled={busy} className="btn-ghost">
            <FileCode2 size={14} /> Preview YAML
          </button>
          <button onClick={onSave} disabled={busy || errorCount > 0} className="btn-ghost">
            <Save size={14} /> Save
          </button>
          <button onClick={onRun} disabled={busy || errorCount > 0} className="btn-primary">
            {busy ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
            Run pipeline
          </button>
        </div>
      </div>
    </header>
  );
}

function ValidationBadges({ errorCount, warningCount }) {
  if (errorCount === 0 && warningCount === 0) {
    return <span className="chip bg-ok/10 text-ok"><CheckCircle2 size={11} /> valid</span>;
  }
  return (
    <div className="flex items-center gap-1.5">
      {errorCount > 0   && <span className="chip bg-bad/10 text-bad"><AlertTriangle size={11} /> {errorCount} error{errorCount>1?"s":""}</span>}
      {warningCount > 0 && <span className="chip bg-warn/10 text-warn"><AlertTriangle size={11} /> {warningCount} warning{warningCount>1?"s":""}</span>}
    </div>
  );
}


// ────────────────────────────────────────────────────────────────────────────
// Tabs
// ────────────────────────────────────────────────────────────────────────────
function Tabs({ current, onChange, runActive }) {
  return (
    <nav className="border-b border-line bg-ink">
      <div className="max-w-7xl mx-auto px-6 flex gap-1">
        {TABS.map(t => {
          const Icon = t.icon;
          const isActive = current === t.key;
          return (
            <button
              key={t.key}
              onClick={() => onChange(t.key)}
              className={`flex items-center gap-2 px-4 py-3 text-sm border-b-2 transition-colors
                ${isActive
                  ? "border-accent text-text"
                  : "border-transparent text-muted hover:text-text"}`}
            >
              <Icon size={14} />
              {t.label}
              {t.key === "run" && runActive && (
                <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
              )}
            </button>
          );
        })}
      </div>
    </nav>
  );
}


// ────────────────────────────────────────────────────────────────────────────
// ValidationPanel — sticky, collapsible list of errors/warnings
// ────────────────────────────────────────────────────────────────────────────
function ValidationPanel({ issues }) {
  return (
    <div className="card p-4">
      <h3 className="text-sm font-semibold mb-3 flex items-center gap-2">
        <AlertTriangle size={14} className="text-warn" />
        Validation ({issues.length})
      </h3>
      <ul className="space-y-1.5 text-sm">
        {issues.map((iss, i) => (
          <li key={i} className="flex items-start gap-3 font-mono text-xs">
            <span className={`chip ${iss.severity === "error" ? "bg-bad/10 text-bad" : "bg-warn/10 text-warn"}`}>
              {iss.severity}
            </span>
            <code className="text-muted">{iss.path}</code>
            <span className="text-text">{iss.message}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
