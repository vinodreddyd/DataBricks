// ============================================================================
// TableBuilder.jsx
// ----------------------------------------------------------------------------
// Right pane on the Build tab. Renders the source.tables[] array as
// collapsible cards with an "+ Add table" button.
//
// Each card is fully self-contained: name, ingestion mode + table/query,
// watermark cols, parallel read tuning, target schema/table, write mode,
// merge keys, governance tags.
//
// Schema-aware editing: when the connection panel has a "Foreign catalog"
// set, "Browse" buttons appear next to the source-table input and any
// column-list field (watermarks, merge keys, partitionColumn). These open
// the SchemaBrowser / ColumnPicker modals which read from the federated
// catalog's information_schema via the SQL Warehouse.
// ============================================================================

import React, { useState } from "react";
import {
  Plus, Table2, Trash2, ChevronDown, ChevronRight, Sparkles, X, Search,
  Database, Columns3,
} from "lucide-react";
import SchemaBrowser from "./SchemaBrowser";
import ColumnPicker  from "./ColumnPicker";

const blankTable = (defaults = {}) => ({
  name: "",
  ingestion: {
    mode: "incremental",
    table: "",
    query: "",
    watermark_columns: ["create_ts", "lst_updt_ts"],
  },
  read_options: {
    fetchsize: 50000,
    partitionColumn: null,
    lowerBound: null,
    upperBound: null,
    numPartitions: null,
    extra: defaults.extra || {},
  },
  target: { schema: "bronze_edw", table: "" },
  write_mode: "upsert",
  merge_keys: [],
  tags: { data_domain: "", data_owner: "", pii: false },
  // Stash columns we already fetched so the picker is instant. Not emitted
  // to YAML — yaml_builder ignores unknown keys.
  _cachedColumns: null,
  _cachedSuggestions: null,
});

export default function TableBuilder({ config, setConfig, meta, issues, foreignCatalog }) {
  const [openIdx, setOpenIdx] = useState(0);

  // Modal state — kept at the top level so only one modal is open at a time.
  const [browserOpen, setBrowserOpen] = useState(false);
  const [browserTargetIdx, setBrowserTargetIdx] = useState(null);

  const [pickerOpen, setPickerOpen] = useState(false);
  const [pickerCtx, setPickerCtx]   = useState(null);   // { idx, mode, schema, table }

  const canBrowse = !!(foreignCatalog && foreignCatalog.trim());

  const addTable = () => {
    setConfig(c => ({
      ...c,
      tables: [...c.tables, blankTable()],
    }));
    setOpenIdx(config.tables.length);
  };

  const removeTable = (idx) => {
    setConfig(c => ({ ...c, tables: c.tables.filter((_, i) => i !== idx) }));
    setOpenIdx(0);
  };

  const updateTable = (idx, patch) => {
    setConfig(c => ({
      ...c,
      tables: c.tables.map((t, i) => i === idx ? { ...t, ...patch } : t),
    }));
  };

  const updateNested = (idx, key, patch) => {
    setConfig(c => ({
      ...c,
      tables: c.tables.map((t, i) =>
        i === idx ? { ...t, [key]: { ...t[key], ...patch } } : t),
    }));
  };

  // ── Modal openers ────────────────────────────────────────────────────────
  // Open the schema/table browser for a specific table card.
  const openBrowser = (idx) => {
    setBrowserTargetIdx(idx);
    setBrowserOpen(true);
  };

  // SchemaBrowser returned a (schema, table) pick. Apply to the card and
  // cache the columns it pre-fetched so the watermark picker is instant.
  const applyBrowserPick = (schema, tableName, columns, suggestions) => {
    if (browserTargetIdx == null) return;
    setConfig(c => ({
      ...c,
      tables: c.tables.map((t, i) => i === browserTargetIdx ? {
        ...t,
        // Default the table identifier to the table name if blank
        name: t.name || tableName,
        ingestion: { ...t.ingestion, table: `${schema}.${tableName}`, query: "" },
        // Default target.table to source name if blank
        target: { ...t.target, table: t.target.table || tableName },
        _cachedColumns: columns,
        _cachedSuggestions: suggestions,
      } : t),
    }));
  };

  // Open the column picker for watermarks / merge_keys / partitionColumn.
  const openPicker = (idx, mode) => {
    const t = config.tables[idx];
    const fq = (t.ingestion.table || "").split(".");
    if (fq.length < 2) return;             // no source table set yet — guard
    setPickerCtx({
      idx,
      mode,
      schema: fq.slice(0, -1).join("."),
      table:  fq[fq.length - 1],
      initialColumns:    t._cachedColumns,
      initialSuggestions:t._cachedSuggestions,
      selected:
        mode === "watermark"  ? t.ingestion.watermark_columns :
        mode === "merge_keys" ? t.merge_keys :
        mode === "partition"  ? (t.read_options.partitionColumn ? [t.read_options.partitionColumn] : []) :
        [],
    });
    setPickerOpen(true);
  };

  const applyPicker = (selected) => {
    if (!pickerCtx) return;
    const { idx, mode } = pickerCtx;
    if (mode === "watermark") {
      updateNested(idx, "ingestion", { watermark_columns: selected });
    } else if (mode === "merge_keys") {
      setConfig(c => ({
        ...c,
        tables: c.tables.map((t, i) => i === idx ? { ...t, merge_keys: selected } : t),
      }));
    } else if (mode === "partition") {
      updateNested(idx, "read_options", { partitionColumn: selected[0] || null });
    }
  };

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-4 pb-3 border-b border-line">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded bg-line flex items-center justify-center">
            <Table2 size={14} className="text-accent" />
          </div>
          <div>
            <h3 className="text-sm font-semibold">Tables ({config.tables.length})</h3>
            <p className="text-xs text-muted">Each entry becomes one Bronze Delta table</p>
          </div>
        </div>
        <button onClick={addTable} className="btn-primary">
          <Plus size={14} /> Add table
        </button>
      </div>

      {config.tables.length === 0 && (
        <div className="text-center py-12 border border-dashed border-line rounded-md">
          <Sparkles size={24} className="mx-auto text-muted mb-3" />
          <p className="text-sm text-text">No tables configured yet.</p>
          <p className="text-xs text-muted mt-1">Click <span className="text-accent">+ Add table</span> to start.</p>
        </div>
      )}

      <div className="space-y-3">
        {config.tables.map((t, idx) => {
          const open = openIdx === idx;
          const issuesForTable = issues.filter(i => i.path.startsWith(`tables[${idx}]`));
          const errCount = issuesForTable.filter(i => i.severity === "error").length;

          return (
            <div key={idx} className="border border-line rounded-md overflow-hidden">
              <button
                onClick={() => setOpenIdx(open ? -1 : idx)}
                className="w-full flex items-center gap-3 px-4 py-3 bg-ink/40 hover:bg-ink/70 transition-colors text-left"
              >
                {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                <span className="font-mono text-xs text-muted">#{idx + 1}</span>
                <span className="text-sm font-medium flex-1 truncate">
                  {t.name || <span className="text-muted italic">unnamed table</span>}
                </span>
                {t.target.table && (
                  <code className="text-[11px] text-muted">→ {t.target.schema}.{t.target.table}</code>
                )}
                <span className="chip bg-line text-muted">{t.ingestion.mode}</span>
                <span className="chip bg-line text-muted">{t.write_mode}</span>
                {errCount > 0 && (
                  <span className="chip bg-bad/10 text-bad">{errCount} err</span>
                )}
                <span
                  role="button"
                  className="text-muted hover:text-bad cursor-pointer"
                  onClick={(e) => { e.stopPropagation(); removeTable(idx); }}
                >
                  <Trash2 size={14} />
                </span>
              </button>

              {open && (
                <div className="p-4 bg-panel space-y-4">
                  <TableEditor
                    table={t}
                    update={(patch) => updateTable(idx, patch)}
                    updateNested={(k, p) => updateNested(idx, k, p)}
                    meta={meta}
                    issues={issuesForTable}
                    idx={idx}
                    canBrowse={canBrowse}
                    onBrowseTable={() => openBrowser(idx)}
                    onPickColumns={(mode) => openPicker(idx, mode)}
                  />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* ── Modals ─────────────────────────────────────────────────────── */}
      {browserOpen && (
        <SchemaBrowser
          open={browserOpen}
          onClose={() => setBrowserOpen(false)}
          foreignCatalog={foreignCatalog}
          onPick={applyBrowserPick}
        />
      )}
      {pickerOpen && pickerCtx && (
        <ColumnPicker
          open={pickerOpen}
          onClose={() => setPickerOpen(false)}
          foreignCatalog={foreignCatalog}
          schema={pickerCtx.schema}
          table={pickerCtx.table}
          mode={pickerCtx.mode}
          initialColumns={pickerCtx.initialColumns}
          initialSuggestions={pickerCtx.initialSuggestions}
          selected={pickerCtx.selected}
          onApply={applyPicker}
        />
      )}
    </div>
  );
}


// ────────────────────────────────────────────────────────────────────────────
// TableEditor — the body of an expanded card
// ────────────────────────────────────────────────────────────────────────────
function TableEditor({ table, update, updateNested, meta, issues, idx,
                       canBrowse, onBrowseTable, onPickColumns }) {
  const issueFor = (p) => issues.find(i => i.path.startsWith(`tables[${idx}].${p}`));
  // The column pickers only make sense once a fully-qualified table is set,
  // because we need (schema, table) to introspect.
  const haveSourceTable = (table.ingestion.table || "").includes(".");
  const canPick = canBrowse && haveSourceTable;

  return (
    <div className="space-y-5">
      {/* ── Identity ─────────────────────────────────────────────────────── */}
      <Row>
        <Field label="Table identifier" hint={issueFor("name")}>
          <input
            className="field"
            placeholder="e.g. f_customer_comment"
            value={table.name}
            onChange={e => update({ name: e.target.value.trim() })}
          />
        </Field>
      </Row>

      {/* ── Ingestion ────────────────────────────────────────────────────── */}
      <SubHeader title="Ingestion" />
      <Row>
        <Field label="Mode">
          <select
            className="field"
            value={table.ingestion.mode}
            onChange={e => updateNested("ingestion", { mode: e.target.value })}
          >
            {(meta?.ingestion_modes || ["incremental", "full", "append"]).map(m =>
              <option key={m} value={m}>{m}</option>
            )}
          </select>
        </Field>
        <Field label="Source table (fully qualified)" hint={issueFor("ingestion")}>
          <div className="flex items-center gap-2">
            <input
              className="field flex-1"
              placeholder="e.g. dbo.TrainDefinitions or public.station_events"
              value={table.ingestion.table}
              onChange={e => updateNested("ingestion", {
                table: e.target.value.trim(),
                query: "",
              })}
            />
            <button
              type="button"
              onClick={onBrowseTable}
              disabled={!canBrowse}
              title={canBrowse
                ? "Browse schemas & tables in the foreign catalog"
                : "Set a Foreign catalog on the connection panel first"}
              className={`btn-ghost px-3 py-2 text-xs flex-shrink-0 ${
                canBrowse ? "" : "opacity-40 cursor-not-allowed"}`}
            >
              <Database size={12} />
              Browse
            </button>
          </div>
        </Field>
      </Row>

      <Field label="Custom query (optional — overrides 'table')">
        <textarea
          className="field font-mono text-xs"
          rows={3}
          placeholder="SELECT a.*, b.region_name FROM d_orders a JOIN d_regions b ON ..."
          value={table.ingestion.query}
          onChange={e => updateNested("ingestion", { query: e.target.value, table: "" })}
        />
      </Field>

      {table.ingestion.mode === "incremental" && (
        <Field label="Watermark columns">
          <div className="flex items-start gap-2">
            <div className="flex-1">
              <ChipList
                items={table.ingestion.watermark_columns}
                onChange={(arr) => updateNested("ingestion", { watermark_columns: arr })}
                placeholder="e.g. create_ts"
              />
            </div>
            <button
              type="button"
              onClick={() => onPickColumns("watermark")}
              disabled={!canPick}
              title={canPick
                ? "Pick from the actual columns of this table"
                : "Set the source table first"}
              className={`btn-ghost px-3 py-2 text-xs flex-shrink-0 ${
                canPick ? "" : "opacity-40 cursor-not-allowed"}`}
            >
              <Columns3 size={12} />
              Pick columns
            </button>
          </div>
        </Field>
      )}

      {/* ── Read options ────────────────────────────────────────────────── */}
      <SubHeader title="Read options" subtitle="JDBC tuning. Set all four parallel-read fields together." />
      <Row>
        <Field label="fetchsize">
          <input
            className="field"
            type="number"
            value={table.read_options.fetchsize}
            onChange={e => updateNested("read_options", { fetchsize: parseInt(e.target.value || "0", 10) })}
          />
        </Field>
        <Field label="partitionColumn">
          <div className="flex items-center gap-2">
            <input
              className="field flex-1"
              placeholder="e.g. cust_cmnt_id"
              value={table.read_options.partitionColumn || ""}
              onChange={e => updateNested("read_options", { partitionColumn: e.target.value.trim() || null })}
            />
            <button
              type="button"
              onClick={() => onPickColumns("partition")}
              disabled={!canPick}
              title="Pick a numeric column from the table"
              className={`btn-ghost px-2 py-2 text-xs flex-shrink-0 ${
                canPick ? "" : "opacity-40 cursor-not-allowed"}`}
            >
              <Columns3 size={12} />
            </button>
          </div>
        </Field>
      </Row>
      <Row>
        <Field label="lowerBound">
          <input
            className="field"
            type="number"
            value={table.read_options.lowerBound ?? ""}
            onChange={e => updateNested("read_options", { lowerBound: e.target.value ? parseInt(e.target.value, 10) : null })}
          />
        </Field>
        <Field label="upperBound">
          <input
            className="field"
            type="number"
            value={table.read_options.upperBound ?? ""}
            onChange={e => updateNested("read_options", { upperBound: e.target.value ? parseInt(e.target.value, 10) : null })}
          />
        </Field>
        <Field label="numPartitions">
          <input
            className="field"
            type="number"
            value={table.read_options.numPartitions ?? ""}
            onChange={e => updateNested("read_options", { numPartitions: e.target.value ? parseInt(e.target.value, 10) : null })}
          />
        </Field>
      </Row>

      <ExtraOptions
        extra={table.read_options.extra || {}}
        onChange={(extra) => updateNested("read_options", { extra })}
      />

      {/* ── Target ──────────────────────────────────────────────────────── */}
      <SubHeader title="Target" />
      <Row>
        <Field label="Schema">
          <input
            className="field"
            placeholder="bronze_edw"
            value={table.target.schema}
            onChange={e => updateNested("target", { schema: e.target.value.trim() })}
          />
        </Field>
        <Field label="Table" hint={issueFor("target")}>
          <input
            className="field"
            placeholder="bronze_f_customer_comment"
            value={table.target.table}
            onChange={e => updateNested("target", { table: e.target.value.trim() })}
          />
        </Field>
      </Row>

      {/* ── Write strategy ──────────────────────────────────────────────── */}
      <SubHeader title="Write strategy" />
      <Row>
        <Field label="Write mode">
          <select
            className="field"
            value={table.write_mode}
            onChange={e => update({ write_mode: e.target.value })}
          >
            {(meta?.write_modes || ["upsert", "append", "overwrite"]).map(m =>
              <option key={m} value={m}>{m}</option>
            )}
          </select>
        </Field>
        {table.write_mode === "upsert" && (
          <Field label="Merge keys" hint={issueFor("merge_keys")}>
            <div className="flex items-start gap-2">
              <div className="flex-1">
                <ChipList
                  items={table.merge_keys}
                  onChange={(arr) => update({ merge_keys: arr })}
                  placeholder="e.g. cust_cmnt_id"
                />
              </div>
              <button
                type="button"
                onClick={() => onPickColumns("merge_keys")}
                disabled={!canPick}
                title={canPick
                  ? "Pick the primary-key columns from this table"
                  : "Set the source table first"}
                className={`btn-ghost px-3 py-2 text-xs flex-shrink-0 ${
                  canPick ? "" : "opacity-40 cursor-not-allowed"}`}
              >
                <Columns3 size={12} />
                Pick
              </button>
            </div>
          </Field>
        )}
      </Row>

      {/* ── Tags ────────────────────────────────────────────────────────── */}
      <SubHeader title="Governance tags" />
      <Row>
        <Field label="data_domain">
          <input className="field"
            value={table.tags.data_domain}
            onChange={e => updateNested("tags", { data_domain: e.target.value.trim() })} />
        </Field>
        <Field label="data_owner" hint={issueFor("tags.data_owner")}>
          <input className="field" placeholder="team@company.com"
            value={table.tags.data_owner}
            onChange={e => updateNested("tags", { data_owner: e.target.value.trim() })} />
        </Field>
        <Field label="">
          <label className="flex items-center gap-2 mt-7 text-sm">
            <input type="checkbox" className="accent-accent"
              checked={table.tags.pii}
              onChange={e => updateNested("tags", { pii: e.target.checked })} />
            Contains PII
          </label>
        </Field>
      </Row>
    </div>
  );
}


// ── Small layout primitives ───────────────────────────────────────────────

const Row = ({ children }) => <div className="grid grid-cols-3 gap-3">{children}</div>;

function Field({ label, hint, children }) {
  return (
    <div className={children ? "" : "hidden"}>
      <label className="label">{label}</label>
      {children}
      {hint && (
        <p className={`text-xs mt-1 ${hint.severity === "error" ? "text-bad" : "text-warn"}`}>
          {hint.message}
        </p>
      )}
    </div>
  );
}

const SubHeader = ({ title, subtitle }) => (
  <div className="pt-2 border-t border-line">
    <h4 className="text-xs font-semibold uppercase tracking-wider text-muted mt-3">{title}</h4>
    {subtitle && <p className="text-xs text-muted/70">{subtitle}</p>}
  </div>
);


// ── ChipList — multi-string editor (watermarks, merge_keys) ───────────────
function ChipList({ items, onChange, placeholder }) {
  const [val, setVal] = useState("");
  const add = () => {
    const v = val.trim();
    if (v && !items.includes(v)) onChange([...items, v]);
    setVal("");
  };
  return (
    <div className="flex flex-wrap items-center gap-1.5 p-1.5 bg-panel border border-line rounded-md">
      {items.map((it, i) => (
        <span key={i} className="chip bg-line text-text">
          {it}
          <button className="text-muted hover:text-bad ml-1"
            onClick={() => onChange(items.filter((_, j) => j !== i))}>
            <X size={10} />
          </button>
        </span>
      ))}
      <input
        className="flex-1 min-w-[120px] bg-transparent text-xs px-1 py-1 outline-none placeholder-muted"
        placeholder={placeholder}
        value={val}
        onChange={e => setVal(e.target.value)}
        onKeyDown={e => { if (e.key === "Enter" || e.key === ",") { e.preventDefault(); add(); } }}
        onBlur={add}
      />
    </div>
  );
}


// ── ExtraOptions — free-form key/value pairs for db-specific JDBC opts ────
function ExtraOptions({ extra, onChange }) {
  const entries = Object.entries(extra);
  const [k, setK] = useState("");
  const [v, setV] = useState("");
  const add = () => {
    if (!k.trim()) return;
    onChange({ ...extra, [k.trim()]: v });
    setK(""); setV("");
  };
  const remove = (key) => {
    const { [key]: _, ...rest } = extra;
    onChange(rest);
  };
  return (
    <div>
      <label className="label">Extra read_options (db-specific)</label>
      <div className="space-y-1.5">
        {entries.map(([key, val]) => (
          <div key={key} className="flex items-center gap-2 text-xs font-mono">
            <code className="px-2 py-1 bg-panel border border-line rounded">{key}</code>
            <span className="text-muted">:</span>
            <input
              className="field-sm flex-1"
              value={val}
              onChange={e => onChange({ ...extra, [key]: e.target.value })}
            />
            <button className="text-muted hover:text-bad" onClick={() => remove(key)}>
              <X size={12} />
            </button>
          </div>
        ))}
        <div className="flex items-center gap-2">
          <input
            className="field-sm flex-1"
            placeholder="key (e.g. encrypt)"
            value={k}
            onChange={e => setK(e.target.value)}
          />
          <input
            className="field-sm flex-1"
            placeholder="value (e.g. true)"
            value={v}
            onChange={e => setV(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter") { e.preventDefault(); add(); } }}
          />
          <button className="btn-ghost px-3 py-1.5 text-xs" onClick={add}>
            <Plus size={12} /> Add
          </button>
        </div>
      </div>
    </div>
  );
}


