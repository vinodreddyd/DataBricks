// ============================================================================
// ColumnPicker.jsx
// ----------------------------------------------------------------------------
// Modal for picking a list of columns from a discovered table — used for
// watermark_columns, merge_keys, and partitionColumn.
//
// The first time a table is selected from SchemaBrowser, we pre-fetch the
// columns and pass them in via the `initialColumns` prop, so the modal
// renders instantly without another round-trip. If the user opens the
// picker on a hand-typed table name (no pre-fetch), we introspect on
// open.
//
// "watermark" mode pre-checks columns flagged by the backend's
// suggest_watermark_columns() heuristic and shows a helpful note.
// ============================================================================

import React, { useEffect, useMemo, useState } from "react";
import {
  Columns3, Search, RefreshCw, X, AlertCircle, CheckCircle2, Loader2,
  Sparkles, Hash, Calendar,
} from "lucide-react";
import { api } from "../api";

export default function ColumnPicker({
  open,
  onClose,
  foreignCatalog,     // UC foreign catalog name
  schema,
  table,
  mode = "watermark", // 'watermark' | 'merge_keys' | 'partition'
  initialColumns,     // optional preloaded columns from SchemaBrowser
  initialSuggestions, // optional preloaded watermark suggestions
  selected = [],      // current selection
  onApply,            // (selected: string[]) => void
}) {
  const [columns,    setColumns]    = useState(initialColumns || []);
  const [suggestions,setSuggestions]= useState(initialSuggestions || []);
  const [loading,    setLoading]    = useState(false);
  const [error,      setError]      = useState(null);
  const [filter,     setFilter]     = useState("");
  const [picked,     setPicked]     = useState(new Set(selected));

  useEffect(() => {
    if (!open) return;
    setPicked(new Set(selected));
    setFilter("");
    setError(null);
    if (initialColumns && initialColumns.length > 0) {
      setColumns(initialColumns);
      setSuggestions(initialSuggestions || []);
      // pre-select watermark suggestions if mode is watermark and no prior
      // selection exists
      if (mode === "watermark" && (selected || []).length === 0
          && (initialSuggestions || []).length > 0) {
        setPicked(new Set(initialSuggestions));
      }
    } else if (foreignCatalog && schema && table) {
      fetchColumns(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, schema, table, foreignCatalog]);

  async function fetchColumns(force) {
    setLoading(true); setError(null);
    try {
      const r = await api.introspectColumns(foreignCatalog, schema, table, force);
      setColumns(r.columns || []);
      setSuggestions(r.watermark_suggestions || []);
      if (mode === "watermark" && picked.size === 0 && (r.watermark_suggestions || []).length > 0) {
        setPicked(new Set(r.watermark_suggestions));
      }
    } catch (e) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  const filtered = useMemo(() => {
    if (!filter.trim()) return columns;
    const f = filter.trim().toLowerCase();
    return columns.filter(c => (c.column_name || "").toLowerCase().includes(f));
  }, [columns, filter]);

  function toggle(name) {
    const next = new Set(picked);
    next.has(name) ? next.delete(name) : next.add(name);
    setPicked(next);
  }

  function apply() {
    // Preserve column order from the table itself (ordinal_position) when
    // emitting back, so YAML reads naturally.
    const order = new Map(columns.map((c, i) => [c.column_name, c.ordinal_position ?? i]));
    const out = Array.from(picked).sort(
      (a, b) => (order.get(a) ?? 9999) - (order.get(b) ?? 9999),
    );
    onApply(out);
    onClose();
  }

  if (!open) return null;

  const title = {
    watermark:  "Pick watermark columns",
    merge_keys: "Pick merge keys",
    partition:  "Pick partition column",
  }[mode] || "Pick columns";

  const hint = {
    watermark:  "Columns used to detect new/updated rows on incremental loads. Best candidates are monotonic timestamps or version numbers.",
    merge_keys: "Columns that uniquely identify a row, used for the MERGE … ON clause.",
    partition:  "A numeric column used to parallelise the JDBC read (Spark splits the range into numPartitions tasks).",
  }[mode];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div className="card w-full max-w-2xl max-h-[80vh] flex flex-col">

        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-line">
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded bg-line flex items-center justify-center">
              <Columns3 size={14} className="text-accent" />
            </div>
            <div>
              <h3 className="text-sm font-semibold">{title}</h3>
              <p className="text-xs text-muted font-mono">{schema}.{table}</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="btn-ghost px-2 py-1.5 text-xs"
              onClick={() => fetchColumns(true)}
              disabled={loading || !foreignCatalog}
            >
              <RefreshCw size={12} className={loading ? "animate-spin" : ""} />
              Refresh
            </button>
            <button className="btn-ghost px-2 py-1.5" onClick={onClose}><X size={14} /></button>
          </div>
        </div>

        {/* Hint banner */}
        {hint && (
          <div className="m-4 mb-2 p-3 rounded-md bg-line/40 border border-line text-xs text-muted">
            {hint}
          </div>
        )}

        {/* Search */}
        <div className="px-4 pb-2 flex items-center gap-2">
          <Search size={12} className="text-faint" />
          <input
            className="flex-1 bg-panel border border-line rounded-md px-3 py-1.5 text-xs outline-none placeholder-muted focus:border-accent"
            placeholder="Filter columns…"
            value={filter}
            onChange={e => setFilter(e.target.value)}
          />
          {columns.length > 0 && (
            <span className="text-xs text-faint">
              {picked.size} selected · {filtered.length} / {columns.length}
            </span>
          )}
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-2 pb-2">
          {loading && (
            <div className="p-6 flex items-center gap-3 text-xs text-muted">
              <Loader2 size={14} className="animate-spin text-accent" />
              <span>Listing columns of {schema}.{table}…</span>
            </div>
          )}
          {!loading && columns.length === 0 && !error && (
            <div className="p-6 text-xs text-muted">No columns yet — click Refresh.</div>
          )}
          {!loading && filtered.map((c, i) => {
            const name = c.column_name;
            const isPicked = picked.has(name);
            const isSuggested = suggestions.includes(name);
            const isPartitionFriendly = looksNumeric(c.data_type);
            const isWatermarkType = looksTimeOrVersion(c.data_type);
            return (
              <label key={i}
                     className={`flex items-center gap-3 px-3 py-2 mx-1 rounded cursor-pointer
                                 hover:bg-line/40 transition-colors
                                 ${isPicked ? "bg-accent/10" : ""}`}>
                <input
                  type={mode === "partition" ? "radio" : "checkbox"}
                  name="colpick"
                  checked={isPicked}
                  onChange={() => mode === "partition"
                    ? setPicked(new Set([name]))
                    : toggle(name)}
                  className="accent-accent"
                />
                <div className="flex-1 min-w-0 flex items-center gap-3">
                  <span className="font-mono text-xs truncate">{name}</span>
                  {isSuggested && mode === "watermark" && (
                    <span className="chip bg-accent/15 text-accent text-[10px]" title="Suggested watermark">
                      <Sparkles size={9} /> suggested
                    </span>
                  )}
                  {mode === "watermark" && isWatermarkType && !isSuggested && (
                    <span className="chip bg-blue-500/10 text-blue-400 text-[10px]" title="Time-shaped column">
                      <Calendar size={9} /> time-like
                    </span>
                  )}
                  {mode === "partition" && isPartitionFriendly && (
                    <span className="chip bg-blue-500/10 text-blue-400 text-[10px]">
                      <Hash size={9} /> numeric
                    </span>
                  )}
                </div>
                <span className="text-[11px] text-faint font-mono">{c.data_type}</span>
                {c.is_nullable === "YES" && (
                  <span className="text-[10px] text-warn">nullable</span>
                )}
              </label>
            );
          })}
        </div>

        {/* Error */}
        {error && (
          <div className="px-4 py-2 border-t border-bad/30 bg-bad/5 text-xs text-bad flex items-start gap-2">
            <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
            <div className="flex-1">{error}</div>
          </div>
        )}

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 p-3 border-t border-line">
          <button className="btn-ghost px-3 py-1.5 text-xs" onClick={onClose}>Cancel</button>
          <button className="btn-primary px-4 py-1.5 text-xs"
                  disabled={picked.size === 0}
                  onClick={apply}>
            <CheckCircle2 size={12} /> Use {picked.size} {picked.size === 1 ? "column" : "columns"}
          </button>
        </div>
      </div>
    </div>
  );
}


// ── Helpers ────────────────────────────────────────────────────────────────

function looksNumeric(t) {
  const s = (t || "").toLowerCase();
  return /int|long|bigint|smallint|number|decimal|numeric|bigserial|serial/.test(s);
}

function looksTimeOrVersion(t) {
  const s = (t || "").toLowerCase();
  return /timestamp|datetime|date|time|rowversion|scn/.test(s);
}
