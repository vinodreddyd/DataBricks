// ============================================================================
// RunHistory.jsx
// ----------------------------------------------------------------------------
// Lists the most recent rows from {catalog}.platform_meta.pipeline_runs.
// Backend hits the SQL Warehouse — see backend/run_history.py.
// ============================================================================

import React, { useEffect, useState } from "react";
import { History, RefreshCw, Search } from "lucide-react";
import { api } from "../api";

export default function RunHistory() {
  const [rows, setRows]   = useState([]);
  const [busy, setBusy]   = useState(false);
  const [err, setErr]     = useState(null);
  const [q, setQ]         = useState("");

  async function load() {
    setBusy(true); setErr(null);
    try {
      const { runs } = await api.history(50, q || null);
      setRows(runs || []);
    } catch (e) { setErr(e.message); }
    finally { setBusy(false); }
  }

  useEffect(() => { load(); }, []);    // initial load

  return (
    <div className="card overflow-hidden">
      <div className="flex items-center justify-between gap-3 px-5 py-4 border-b border-line">
        <div className="flex items-center gap-3">
          <History size={16} className="text-accent" />
          <h3 className="text-sm font-semibold">Recent runs</h3>
          <span className="chip bg-line text-muted">{rows.length}</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search size={12} className="absolute left-2.5 top-2.5 text-muted" />
            <input
              className="field-sm pl-7 w-56"
              placeholder="filter by pipeline name…"
              value={q}
              onChange={e => setQ(e.target.value)}
              onKeyDown={e => { if (e.key === "Enter") load(); }}
            />
          </div>
          <button onClick={load} disabled={busy} className="btn-ghost text-xs">
            <RefreshCw size={12} className={busy ? "animate-spin" : ""} /> Refresh
          </button>
        </div>
      </div>

      {err && (
        <div className="px-5 py-4 border-b border-line text-xs text-warn">
          History unavailable: {err}
          <span className="text-muted ml-2">
            (check that DBX_WAREHOUSE_ID and DBX_CATALOG are set in app.yaml)
          </span>
        </div>
      )}

      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead className="bg-panel/60 text-muted text-[11px] uppercase tracking-wider">
            <tr>
              <th className="text-left px-5 py-2.5">Pipeline</th>
              <th className="text-left px-5 py-2.5">Status</th>
              <th className="text-right px-5 py-2.5">Rows</th>
              <th className="text-right px-5 py-2.5">Duration</th>
              <th className="text-left px-5 py-2.5">Started</th>
              <th className="text-left px-5 py-2.5">Config v.</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && !busy && (
              <tr>
                <td colSpan={6} className="px-5 py-12 text-center text-muted">
                  No runs yet — submit one from the Build tab.
                </td>
              </tr>
            )}
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-line hover:bg-panel/40">
                <td className="px-5 py-2.5 font-mono text-xs">{r.pipeline_name}</td>
                <td className="px-5 py-2.5">
                  <span className={statusChip(r.status)}>{r.status}</span>
                </td>
                <td className="px-5 py-2.5 text-right font-mono text-xs">
                  {r.row_count == null ? "—" : Number(r.row_count).toLocaleString()}
                </td>
                <td className="px-5 py-2.5 text-right font-mono text-xs">
                  {r.duration_seconds == null ? "—" : `${Number(r.duration_seconds).toFixed(1)}s`}
                </td>
                <td className="px-5 py-2.5 text-xs text-muted">{r.start_time}</td>
                <td className="px-5 py-2.5 text-xs text-muted font-mono">{r.config_version || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function statusChip(status) {
  const s = (status || "").toLowerCase();
  if (s === "success")  return "chip bg-ok/10 text-ok";
  if (s === "failed")   return "chip bg-bad/10 text-bad";
  if (s === "running")  return "chip bg-accent/10 text-accent";
  return "chip bg-line text-muted";
}
