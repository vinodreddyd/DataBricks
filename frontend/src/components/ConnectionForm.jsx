// ============================================================================
// ConnectionForm.jsx
// ----------------------------------------------------------------------------
// Left pane on the Build tab. Renders:
//   - source_name + fail_fast
//   - DB type dropdown (calls /api/templates/{db_type} on change to autofill)
//   - host / port / database
//   - secret scope + user/password secret KEYS (not values — never collect creds)
// ============================================================================

import React from "react";
import { Globe, KeyRound, ServerCog, Database } from "lucide-react";

export default function ConnectionForm({ config, setConfig, meta, onDbTypeChange, issues }) {
  const conn = config.connection;
  const setConn  = (patch) => setConfig(c => ({ ...c, connection: { ...c.connection, ...patch } }));
  const setRoot  = (patch) => setConfig(c => ({ ...c, ...patch }));

  // helper to look up an issue for a given path prefix
  const issueFor = (p) => issues.find(i => i.path.startsWith(p));

  return (
    <div className="card p-5">
      <SectionHeader icon={ServerCog} title="Source" subtitle="Logical pipeline name and DB connection" />

      {/* ── Pipeline identity ────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="col-span-2">
          <label className="label">Source name</label>
          <input
            className="field"
            placeholder="e.g. redshift_finance_batch"
            value={config.source_name}
            onChange={e => setRoot({ source_name: e.target.value.trim() })}
          />
          <FieldHint issue={issueFor("source_name")}>
            Used as the prefix for run names, watermark records, and the YAML filename.
          </FieldHint>
        </div>

        <div className="col-span-2 flex items-center gap-3">
          <input
            id="fail_fast"
            type="checkbox"
            className="accent-accent"
            checked={config.fail_fast}
            onChange={e => setRoot({ fail_fast: e.target.checked })}
          />
          <label htmlFor="fail_fast" className="text-sm text-text">
            Fail fast — abort the batch on the first table failure
          </label>
        </div>
      </div>

      <SectionHeader icon={Globe} title="JDBC connection" subtitle="Shared by every table below" />

      {/* ── DB Type dropdown drives all the other defaults ──────────────── */}
      <div className="grid grid-cols-2 gap-4">
        <div className="col-span-2">
          <label className="label">DB type</label>
          <select
            className="field"
            value={conn.db_type}
            onChange={e => onDbTypeChange(e.target.value)}
          >
            {meta?.db_types?.map(dt => (
              <option key={dt.value} value={dt.value}>{dt.label}</option>
            ))}
          </select>
        </div>

        <div className="col-span-2">
          <label className="label">Host</label>
          <input
            className="field"
            placeholder="e.g. edwdev-rs.c4606mhizfi6.us-east-1.redshift.amazonaws.com"
            value={conn.host}
            onChange={e => setConn({ host: e.target.value.trim() })}
          />
          <FieldHint issue={issueFor("connection.host")} />
        </div>

        <div>
          <label className="label">Port</label>
          <input
            className="field"
            type="number"
            value={conn.port}
            onChange={e => setConn({ port: parseInt(e.target.value || "0", 10) })}
          />
          <FieldHint issue={issueFor("connection.port")} />
        </div>

        <div>
          <label className="label">Database</label>
          <input
            className="field"
            placeholder="e.g. edwdev"
            value={conn.database}
            onChange={e => setConn({ database: e.target.value.trim() })}
          />
          <FieldHint issue={issueFor("connection.database")} />
        </div>
      </div>

      <SectionHeader icon={KeyRound} title="Secret references" subtitle="Keys only — passwords stay in the cluster's secret scope" mt />

      <div className="grid grid-cols-2 gap-4">
        <div className="col-span-2">
          <label className="label">Secret scope (placeholder)</label>
          <input
            className="field font-mono"
            value={conn.secret_scope}
            onChange={e => setConn({ secret_scope: e.target.value.trim() })}
          />
          <p className="text-xs text-muted mt-1">
            Default <code className="text-text">__SECRETS_SCOPE__</code> is replaced at runtime by{" "}
            <code className="text-text">env-overrides/dev.yaml</code>.
          </p>
        </div>

        <div>
          <label className="label">User secret key</label>
          <input
            className="field"
            value={conn.user_secret}
            onChange={e => setConn({ user_secret: e.target.value.trim() })}
          />
          <FieldHint issue={issueFor("connection.user_secret")} />
        </div>

        <div>
          <label className="label">Password secret key</label>
          <input
            className="field"
            value={conn.password_secret}
            onChange={e => setConn({ password_secret: e.target.value.trim() })}
          />
          <FieldHint issue={issueFor("connection.password_secret")} />
        </div>
      </div>

      <SectionHeader icon={Database} title="Schema browser (optional)"
                     subtitle="Lakehouse Federation catalog — enables 'Browse' buttons" mt />

      <div className="grid grid-cols-1 gap-4">
        <div>
          <label className="label">Foreign catalog</label>
          <input
            className="field font-mono"
            placeholder="e.g. redshift_prod or postgres_finance"
            value={conn.foreign_catalog || ""}
            onChange={e => setConn({ foreign_catalog: e.target.value.trim() })}
          />
          <p className="text-xs text-muted mt-1">
            If this source is registered in Unity Catalog via{" "}
            <a href="https://docs.databricks.com/en/query-federation/index.html"
               target="_blank" rel="noreferrer"
               className="text-accent hover:underline">Lakehouse Federation</a>,
            paste the foreign catalog name here. The table builder will then let
            you browse schemas/tables and pick columns instead of typing names.
            Leave blank to fall back to manual entry.
          </p>
        </div>
      </div>
    </div>
  );
}

function SectionHeader({ icon: Icon, title, subtitle, mt }) {
  return (
    <div className={`flex items-center gap-3 ${mt ? "mt-6" : ""} mb-4 pb-3 border-b border-line`}>
      <div className="w-7 h-7 rounded bg-line flex items-center justify-center">
        <Icon size={14} className="text-accent" />
      </div>
      <div>
        <h3 className="text-sm font-semibold">{title}</h3>
        {subtitle && <p className="text-xs text-muted">{subtitle}</p>}
      </div>
    </div>
  );
}

function FieldHint({ issue, children }) {
  if (issue) {
    return (
      <p className={`text-xs mt-1 ${issue.severity === "error" ? "text-bad" : "text-warn"}`}>
        {issue.message}
      </p>
    );
  }
  if (children) return <p className="text-xs text-muted mt-1">{children}</p>;
  return null;
}
