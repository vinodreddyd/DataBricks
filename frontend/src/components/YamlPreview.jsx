// ============================================================================
// YamlPreview.jsx
// ----------------------------------------------------------------------------
// Read-only YAML viewer with copy + refresh. The YAML shown here is the
// EXACT bytes the backend will write to Workspace Files when the user
// clicks Save or Run — no client-side rendering of the YAML to avoid drift.
// ============================================================================

import React, { useState } from "react";
import { Copy, RefreshCw, FileCheck2, ClipboardCheck } from "lucide-react";

export default function YamlPreview({ yamlText, onRefresh, busy, sourceName }) {
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    if (!yamlText) return;
    await navigator.clipboard.writeText(yamlText);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-line bg-panel/60">
        <div className="flex items-center gap-3">
          <FileCheck2 size={16} className="text-accent" />
          <div>
            <h3 className="text-sm font-semibold">
              Generated YAML
            </h3>
            <p className="text-xs text-muted font-mono">
              {sourceName ? `${sourceName}.yaml` : "untitled.yaml"}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={onRefresh} disabled={busy} className="btn-ghost text-xs">
            <RefreshCw size={12} className={busy ? "animate-spin" : ""} /> Refresh
          </button>
          <button onClick={copy} disabled={!yamlText} className="btn-ghost text-xs">
            {copied ? <ClipboardCheck size={12} className="text-ok" /> : <Copy size={12} />}
            {copied ? "Copied" : "Copy"}
          </button>
        </div>
      </div>

      {yamlText ? (
        <pre className="p-4 text-xs font-mono leading-relaxed overflow-auto max-h-[70vh] text-text whitespace-pre">
          {yamlText}
        </pre>
      ) : (
        <div className="p-12 text-center">
          <p className="text-sm text-muted">
            No preview yet — go to <span className="text-text">Build</span> and click{" "}
            <span className="text-accent">Preview YAML</span>.
          </p>
        </div>
      )}
    </div>
  );
}
