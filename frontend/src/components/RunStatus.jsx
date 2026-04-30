// ============================================================================
// RunStatus.jsx
// ----------------------------------------------------------------------------
// Live status of the most recently submitted run.
//   - polls /api/runs/{run_id} every 3 s while state != TERMINATED
//   - shows lifecycle state, result state, deep link to the Databricks Runs UI
//   - once TERMINATED+SUCCESS, surfaces row_count + duration from
//     platform_meta.pipeline_runs (fetched server-side)
// ============================================================================

import React, { useEffect, useState } from "react";
import {
  Activity, ExternalLink, RefreshCw, Square,
  CheckCircle2, AlertTriangle, Clock, Hash,
} from "lucide-react";
import { api } from "../api";

export default function RunStatus({ activeRun, onRunAgain }) {
  const [status, setStatus] = useState(null);
  const [polling, setPolling] = useState(false);
  const [runEnv, setRunEnv]   = useState("dev");

  // Poll while running
  useEffect(() => {
    if (!activeRun) return;
    let cancelled = false;
    setPolling(true);

    async function tick() {
      try {
        const s = await api.runStatus(activeRun.run_id, activeRun.pipeline_name);
        if (cancelled) return;
        setStatus(s);
        if (s.state !== "TERMINATED" && s.state !== "INTERNAL_ERROR" && s.state !== "SKIPPED") {
          setTimeout(tick, 3000);
        } else {
          setPolling(false);
        }
      } catch (e) {
        if (!cancelled) {
          setStatus(s => ({ ...s, _error: e.message }));
          setPolling(false);
        }
      }
    }
    tick();
    return () => { cancelled = true; };
  }, [activeRun]);

  if (!activeRun) {
    return (
      <div className="card p-12 text-center">
        <Activity size={28} className="mx-auto text-muted mb-4" />
        <p className="text-sm text-muted">
          No active run. Go to <span className="text-text">Build</span> and click{" "}
          <span className="text-accent">Run pipeline</span>.
        </p>
      </div>
    );
  }

  const cancel = async () => {
    try {
      await api.cancelRun(activeRun.run_id);
    } catch (e) { alert(e.message); }
  };

  return (
    <div className="space-y-4">
      <div className="card p-5">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <div className="flex items-center gap-3">
              <StateBadge state={status?.state} result={status?.result_state} />
              <h2 className="text-lg font-semibold font-mono">Run #{activeRun.run_id}</h2>
            </div>
            <p className="text-xs text-muted mt-2 font-mono">
              {activeRun.config_path}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <a className="btn-ghost text-xs" href={activeRun.run_page_url} target="_blank" rel="noreferrer">
              <ExternalLink size={12} /> Open in Databricks
            </a>
            {polling && (
              <button onClick={cancel} className="btn-danger text-xs">
                <Square size={12} /> Cancel
              </button>
            )}
            {!polling && (
              <button onClick={() => onRunAgain({ env: runEnv })} className="btn-primary text-xs">
                <RefreshCw size={12} /> Re-run
              </button>
            )}
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6">
          <Metric icon={Activity} label="State" value={status?.state || "—"} />
          <Metric icon={CheckCircle2} label="Result" value={status?.result_state || "—"} />
          <Metric icon={Clock} label="Started" value={fmtTs(status?.start_time)} />
          <Metric icon={Clock} label="Ended" value={fmtTs(status?.end_time)} />
        </div>
      </div>

      {/* ── Pipeline metrics from platform_meta.pipeline_runs ───────────── */}
      {status?.pipeline_metrics && (
        <div className="card p-5">
          <h3 className="text-sm font-semibold mb-4 flex items-center gap-2">
            <Hash size={14} className="text-accent" /> Ingestion metrics
          </h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Metric icon={Hash} label="Rows ingested" value={fmtInt(status.pipeline_metrics.row_count)} />
            <Metric icon={Clock} label="Duration"     value={fmtSeconds(status.pipeline_metrics.duration_seconds)} />
            <Metric icon={Activity} label="Status"    value={status.pipeline_metrics.status || "—"} />
            <Metric icon={Activity} label="Pipeline"  value={status.pipeline_metrics.pipeline_name || "—"} mono />
          </div>
        </div>
      )}

      {status?._error && (
        <div className="card p-4 border-bad/40">
          <div className="flex items-start gap-3">
            <AlertTriangle size={16} className="text-bad mt-0.5" />
            <div>
              <h4 className="text-sm font-semibold text-bad">Polling error</h4>
              <p className="text-xs text-muted mt-1">{status._error}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


// ────────────────────────────────────────────────────────────────────────────
function StateBadge({ state, result }) {
  if (!state) return <span className="chip bg-line text-muted">…</span>;
  if (state === "TERMINATED") {
    if (result === "SUCCESS")  return <span className="chip bg-ok/10 text-ok"><CheckCircle2 size={11} /> success</span>;
    if (result === "FAILED")   return <span className="chip bg-bad/10 text-bad"><AlertTriangle size={11} /> failed</span>;
    if (result === "CANCELED") return <span className="chip bg-warn/10 text-warn">canceled</span>;
    return <span className="chip bg-line text-muted">{result || state}</span>;
  }
  if (state === "RUNNING") return <span className="chip bg-accent/10 text-accent"><Activity size={11} className="animate-pulse" /> running</span>;
  if (state === "PENDING") return <span className="chip bg-warn/10 text-warn">pending</span>;
  return <span className="chip bg-line text-muted">{state}</span>;
}

function Metric({ icon: Icon, label, value, mono }) {
  return (
    <div className="border border-line rounded-md p-3">
      <div className="flex items-center gap-2 text-muted text-[11px] uppercase tracking-wider">
        <Icon size={10} /> {label}
      </div>
      <div className={`mt-1 text-sm ${mono ? "font-mono" : ""}`}>{value || "—"}</div>
    </div>
  );
}

const fmtTs   = (ms) => ms ? new Date(ms).toLocaleString() : "—";
const fmtInt  = (n)  => (n == null ? "—" : Number(n).toLocaleString());
const fmtSeconds = (s) => {
  if (s == null) return "—";
  const sec = Number(s);
  if (sec < 60) return `${sec.toFixed(1)} s`;
  const m = Math.floor(sec / 60);
  const r = Math.round(sec - m * 60);
  return `${m}m ${r}s`;
};
