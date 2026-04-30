// ============================================================================
// SchemaBrowser.jsx
// ----------------------------------------------------------------------------
// Modal that lets the user browse the source DB instead of typing table
// names. Uses Lakehouse Federation under the hood — the source DB must
// be registered as a Foreign Catalog in Unity Catalog. The app then reads
// system.information_schema through the SQL Warehouse, which is fast
// (sub-second) and respects all UC permissions.
//
// Two-pane layout: left = schemas, right = tables in the selected schema.
// A search box filters tables in real time. Click a table to use it — the
// modal returns the chosen "<schema>.<table>" up to TableBuilder.
// ============================================================================

import React, { useEffect, useMemo, useState } from "react";
import {
  Database, Table2, Search, RefreshCw, X, ChevronRight, AlertCircle,
  CheckCircle2, Loader2,
} from "lucide-react";
import { api } from "../api";

export default function SchemaBrowser({
  open,
  onClose,
  foreignCatalog,    // e.g. "redshift_prod" — UC foreign catalog
  onPick,            // (schema, table, columns?, suggestions?) => void
}) {
  const [schemas,        setSchemas]        = useState([]);
  const [tables,         setTables]         = useState([]);
  const [activeSchema,   setActiveSchema]   = useState(null);
  const [filter,         setFilter]         = useState("");
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables,  setLoadingTables]  = useState(false);
  const [error,          setError]          = useState(null);

  // Reset whenever the modal opens with a new catalog
  useEffect(() => {
    if (!open) return;
    setSchemas([]); setTables([]); setActiveSchema(null);
    setFilter(""); setError(null);
    if (foreignCatalog) fetchSchemas(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, foreignCatalog]);

  async function fetchSchemas(force) {
    setLoadingSchemas(true); setError(null);
    try {
      const r = await api.introspectSchemas(foreignCatalog, force);
      setSchemas(r.schemas || []);
    } catch (e) {
      setError(humanError(e));
    } finally {
      setLoadingSchemas(false);
    }
  }

  async function fetchTables(schema, force) {
    setActiveSchema(schema);
    setLoadingTables(true); setError(null); setTables([]);
    try {
      const r = await api.introspectTables(foreignCatalog, schema, force);
      setTables(r.tables || []);
    } catch (e) {
      setError(humanError(e));
    } finally {
      setLoadingTables(false);
    }
  }

  const filteredTables = useMemo(() => {
    if (!filter.trim()) return tables;
    const f = filter.trim().toLowerCase();
    return tables.filter(t => (t.table_name || "").toLowerCase().includes(f));
  }, [tables, filter]);

  async function pick(schema, table) {
    // Pre-fetch columns so the watermark picker has data the moment the
    // user opens it. Failure here is non-fatal — user can still type.
    let columns = null, suggestions = [];
    try {
      const r = await api.introspectColumns(foreignCatalog, schema, table, false);
      columns = r.columns;
      suggestions = r.watermark_suggestions || [];
    } catch (_) {
      // ignore — TableBuilder will show fallback
    }
    onPick(schema, table, columns, suggestions);
    onClose();
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div className="card w-full max-w-5xl max-h-[85vh] flex flex-col">

        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-line">
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded bg-line flex items-center justify-center">
              <Database size={14} className="text-accent" />
            </div>
            <div>
              <h3 className="text-sm font-semibold">Browse foreign catalog</h3>
              <p className="text-xs text-muted font-mono">
                {foreignCatalog || "(no catalog set)"} · via Lakehouse Federation
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="btn-ghost px-2 py-1.5 text-xs"
              onClick={() => activeSchema ? fetchTables(activeSchema, true) : fetchSchemas(true)}
              disabled={loadingSchemas || loadingTables || !foreignCatalog}
              title="Re-introspect (skip cache)"
            >
              <RefreshCw size={12} className={loadingSchemas || loadingTables ? "animate-spin" : ""} />
              Refresh
            </button>
            <button className="btn-ghost px-2 py-1.5" onClick={onClose} title="Close">
              <X size={14} />
            </button>
          </div>
        </div>

        {/* Catalog-not-set banner */}
        {!foreignCatalog && (
          <div className="m-4 p-3 rounded-md border border-warn/30 bg-warn/5 text-xs text-warn flex items-start gap-2">
            <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
            <div>
              Set a <strong>Foreign catalog</strong> on the connection panel
              before browsing. The catalog must be registered in Unity Catalog
              via <a href="https://docs.databricks.com/en/query-federation/index.html"
                     target="_blank" rel="noreferrer"
                     className="underline">Lakehouse Federation</a> first.
            </div>
          </div>
        )}

        {/* Body — two panes */}
        <div className="flex-1 grid grid-cols-[260px_1fr] min-h-0">

          {/* Left: schemas */}
          <div className="border-r border-line flex flex-col min-h-0">
            <div className="px-4 py-2 text-xs uppercase tracking-wider text-muted font-semibold border-b border-line">
              Schemas {schemas.length > 0 && <span className="text-faint">({schemas.length})</span>}
            </div>
            <div className="flex-1 overflow-y-auto">
              {loadingSchemas && <LoadingRow label="Listing schemas…" detail="reading information_schema" />}
              {!loadingSchemas && schemas.length === 0 && !error && foreignCatalog && (
                <div className="p-4 text-xs text-muted">No schemas returned.</div>
              )}
              {!loadingSchemas && schemas.map((s, i) => {
                const name = s.schema_name || s.SCHEMA_NAME || s.name;
                const isActive = activeSchema === name;
                return (
                  <button
                    key={i}
                    className={`w-full flex items-center justify-between px-4 py-2 text-xs text-left
                                hover:bg-line transition-colors border-b border-line/40
                                ${isActive ? "bg-accent/10 text-accent" : ""}`}
                    onClick={() => fetchTables(name, false)}
                  >
                    <span className="font-mono truncate">{name}</span>
                    <ChevronRight size={12} className={isActive ? "" : "text-faint"} />
                  </button>
                );
              })}
            </div>
          </div>

          {/* Right: tables */}
          <div className="flex flex-col min-h-0">
            <div className="px-4 py-2 border-b border-line flex items-center gap-2">
              <Search size={12} className="text-faint" />
              <input
                className="flex-1 bg-transparent text-xs outline-none placeholder-muted"
                placeholder={activeSchema
                  ? `Filter tables in ${activeSchema}…`
                  : "Pick a schema on the left first"}
                value={filter}
                onChange={e => setFilter(e.target.value)}
                disabled={!activeSchema}
              />
              {tables.length > 0 && (
                <span className="text-xs text-faint">
                  {filteredTables.length} / {tables.length}
                </span>
              )}
            </div>

            <div className="flex-1 overflow-y-auto">
              {!activeSchema && !loadingTables && (
                <div className="p-8 text-center text-xs text-muted">
                  ← Choose a schema to list its tables
                </div>
              )}
              {loadingTables && <LoadingRow label={`Listing tables in ${activeSchema}…`}
                                            detail="usually under a second" />}
              {!loadingTables && activeSchema && filteredTables.length === 0 && tables.length > 0 && (
                <div className="p-4 text-xs text-muted">No tables match the filter.</div>
              )}
              {!loadingTables && activeSchema && tables.length === 0 && !error && (
                <div className="p-4 text-xs text-muted">
                  Schema <code className="font-mono">{activeSchema}</code> has no visible tables.
                </div>
              )}
              {!loadingTables && filteredTables.map((t, i) => {
                const name = t.table_name || t.TABLE_NAME;
                const ttype = (t.table_type || "TABLE").toString().replace("BASE TABLE", "TABLE");
                const rowEst = t.row_estimate;
                return (
                  <div key={i}
                       className="flex items-center justify-between px-4 py-2 border-b border-line/40
                                  hover:bg-line/40 group">
                    <div className="flex items-center gap-3 min-w-0">
                      <Table2 size={12} className="text-faint flex-shrink-0" />
                      <span className="font-mono text-xs truncate">{name}</span>
                      <span className={`chip text-[10px] ${
                        ttype === "VIEW" ? "bg-blue-500/10 text-blue-400" : "bg-line text-muted"}`}>
                        {ttype}
                      </span>
                      {rowEst != null && rowEst !== "" && (
                        <span className="text-[10px] text-faint font-mono">
                          ~{formatRows(rowEst)} rows
                        </span>
                      )}
                    </div>
                    <button
                      className="btn-primary px-3 py-1 text-xs opacity-0 group-hover:opacity-100 transition"
                      onClick={() => pick(activeSchema, name)}
                    >
                      <CheckCircle2 size={12} /> Use
                    </button>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Error footer */}
        {error && (
          <div className="px-4 py-3 border-t border-bad/30 bg-bad/5 text-xs text-bad flex items-start gap-2">
            <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
            <div className="flex-1">{error}</div>
            <button className="btn-ghost px-2 py-0.5 text-[11px]"
                    onClick={() => setError(null)}>Dismiss</button>
          </div>
        )}

      </div>
    </div>
  );
}


// ── Helpers ────────────────────────────────────────────────────────────────

function LoadingRow({ label, detail }) {
  return (
    <div className="p-6 flex items-center gap-3 text-xs text-muted">
      <Loader2 size={14} className="animate-spin text-accent" />
      <div>
        <div className="text-text">{label}</div>
        {detail && <div className="text-faint mt-0.5">{detail}</div>}
      </div>
    </div>
  );
}

function formatRows(n) {
  const x = Number(n);
  if (Number.isNaN(x)) return n;
  if (x >= 1e9) return (x / 1e9).toFixed(1) + "B";
  if (x >= 1e6) return (x / 1e6).toFixed(1) + "M";
  if (x >= 1e3) return (x / 1e3).toFixed(1) + "K";
  return x.toString();
}

function humanError(e) {
  return e?.message || String(e);
}
