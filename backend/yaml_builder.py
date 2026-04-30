# ============================================================================
# yaml_builder.py
# ----------------------------------------------------------------------------
# Convert an `IngestionConfigForm` (UI payload) into a YAML string whose
# shape is byte-identical to the hand-authored configs in
# domain-configs/sources/*.yaml.
#
# Why a custom builder and not just yaml.safe_dump(model.model_dump())?
#   1. We need the ordered, commented format so generated files diff cleanly
#      against existing ones in code review.
#   2. We need to preserve placeholder strings like "__SECRETS_SCOPE__" and
#      "__CATALOG__" *unquoted* so run_pipeline.inject_env_values() sees them.
#   3. We need to drop empty / None fields so the template's defaults kick in.
#
# Validation lives in validate_config() — called both server-side before
# save/run and from the /api/validate endpoint for live UI feedback.
# ============================================================================

from io import StringIO
from typing import List, Tuple
import yaml

from models import (
    IngestionConfigForm,
    TableForm,
    ValidationIssue,
)


# ── Custom Dumper: preserve key order, no aliases, sane indenting ──────────
class _OrderedDumper(yaml.SafeDumper):
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())


_OrderedDumper.add_representer(dict, _dict_representer)


# ============================================================================
# 1. Build dict in canonical order (matches your sample YAMLs)
# ============================================================================

def _build_dict(cfg: IngestionConfigForm) -> dict:
    """
    Mirror the layout of redshift_multi_table.yaml exactly:
        version
        fail_fast
        source:
          name
          source_type
          connection: { db_type, host, port, database, secret_scope, user_secret, password_secret }
          tables: [ ... ]
        target:
          catalog
        write_mode
        schema_evolution
    """
    tables_out = []
    for t in cfg.tables:
        # ── ingestion block: drop empty (table | query) so YAML stays clean
        ingestion: dict = {"mode": t.ingestion.mode}
        if t.ingestion.table:
            ingestion["table"] = t.ingestion.table
        if t.ingestion.query:
            ingestion["query"] = t.ingestion.query.strip()
        if t.ingestion.mode == "incremental" and t.ingestion.watermark_columns:
            ingestion["watermark_columns"] = list(t.ingestion.watermark_columns)

        # ── read_options block: numeric tuning + free-form extras
        ro = t.read_options
        read_options: dict = {"fetchsize": ro.fetchsize}
        # JDBC parallel-read quartet — only emit if all four are set
        if all(v is not None for v in [ro.partitionColumn, ro.lowerBound,
                                        ro.upperBound, ro.numPartitions]):
            read_options["partitionColumn"] = ro.partitionColumn
            read_options["lowerBound"]      = ro.lowerBound
            read_options["upperBound"]      = ro.upperBound
            read_options["numPartitions"]   = ro.numPartitions
        for k, v in ro.extra.items():
            read_options[k] = v

        # ── target block: catalog is set ONCE at top level (matches your YAMLs)
        target = {"schema": t.target.schema_, "table": t.target.table}

        table_block = {
            "name":         t.name,
            "ingestion":    ingestion,
            "read_options": read_options,
            "target":       target,
            "write_mode":   t.write_mode,
        }
        if t.write_mode == "upsert":
            table_block["merge_keys"] = list(t.merge_keys)

        table_block["tags"] = {
            "data_domain": t.tags.data_domain,
            "data_owner":  t.tags.data_owner,
            "pii":         t.tags.pii,
        }
        tables_out.append(table_block)

    return {
        "version":   cfg.version,
        "fail_fast": cfg.fail_fast,
        "source": {
            "name":        cfg.source_name,
            "source_type": cfg.source_type,
            "connection": {
                "db_type":         cfg.connection.db_type,
                "host":            cfg.connection.host,
                "port":            cfg.connection.port,
                "database":        cfg.connection.database,
                "secret_scope":    cfg.connection.secret_scope,
                "user_secret":     cfg.connection.user_secret,
                "password_secret": cfg.connection.password_secret,
            },
            "tables": tables_out,
        },
        "target": {"catalog": cfg.target_catalog},
        "write_mode":       cfg.write_mode,
        "schema_evolution": cfg.schema_evolution,
    }


# ============================================================================
# 2. Dump to YAML with a top-of-file header banner (matches your style)
# ============================================================================

_HEADER_TEMPLATE = """# ============================================================================
# {filename}
# ----------------------------------------------------------------------------
# Auto-generated by the Databricks Ingestion UI on {generated_at}.
# Source name: {source_name}   |   DB type: {db_type}   |   {n_tables} table(s)
#
# Edits made by hand will be overwritten the next time this config is
# re-saved from the UI. Prefer editing the source via the UI.
#
# Placeholders __CATALOG__, __SECRETS_SCOPE__ are rendered at runtime by
# run_pipeline.inject_env_values() from env-overrides/{{dev,prod}}.yaml.
# ============================================================================
"""


def build(cfg: IngestionConfigForm, filename: str = "generated.yaml",
          generated_at: str = "") -> str:
    """Render an IngestionConfigForm to a YAML string with a comment banner."""
    header = _HEADER_TEMPLATE.format(
        filename=filename,
        generated_at=generated_at or "",
        source_name=cfg.source_name,
        db_type=cfg.connection.db_type,
        n_tables=len(cfg.tables),
    )
    body_dict = _build_dict(cfg)

    buf = StringIO()
    buf.write(header)
    yaml.dump(
        body_dict,
        buf,
        Dumper=_OrderedDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        indent=2,
    )
    return buf.getvalue()


# ============================================================================
# 3. Validation (run before save & run, also exposed at /api/validate)
# ============================================================================

def _err(path: str, msg: str) -> ValidationIssue:
    return ValidationIssue(path=path, severity="error", message=msg)


def _warn(path: str, msg: str) -> ValidationIssue:
    return ValidationIssue(path=path, severity="warning", message=msg)


def validate_config(cfg: IngestionConfigForm) -> Tuple[bool, List[ValidationIssue]]:
    """
    Apply business rules that Pydantic alone cannot express.

    Mirrors the runtime checks performed inside
    bronze_rdbms_multi_template.py (REQUIRED_CONNECTION_FIELDS, merge_keys
    when write_mode=upsert, watermark_columns when mode=incremental, …).
    Catching them here means the user gets a red banner BEFORE submitting
    a 30-minute Spark job.
    """
    issues: List[ValidationIssue] = []

    # ── Source-level checks
    if not cfg.source_name.strip():
        issues.append(_err("source_name", "source_name is required"))
    if not cfg.tables:
        issues.append(_err("tables", "Add at least one table to ingest"))

    # ── Connection checks — fields are non-empty strings
    conn = cfg.connection
    for field in ("host", "database", "user_secret", "password_secret", "secret_scope"):
        if not str(getattr(conn, field, "")).strip():
            issues.append(_err(f"connection.{field}", f"{field} is required"))
    if conn.port <= 0 or conn.port > 65535:
        issues.append(_err("connection.port",
                           f"port {conn.port} is not in 1–65535"))

    # ── Per-table checks
    seen_targets: dict = {}
    for i, t in enumerate(cfg.tables):
        path = f"tables[{i}]"
        if not t.name.strip():
            issues.append(_err(f"{path}.name", "Table name is required"))

        # Exactly one of (table, query) must be present
        has_table = bool(t.ingestion.table and t.ingestion.table.strip())
        has_query = bool(t.ingestion.query and t.ingestion.query.strip())
        if not has_table and not has_query:
            issues.append(_err(
                f"{path}.ingestion",
                "Provide either 'table' (fully-qualified name) or 'query' (custom SELECT)",
            ))
        if has_table and has_query:
            issues.append(_err(
                f"{path}.ingestion",
                "'table' and 'query' are mutually exclusive — pick one",
            ))

        # Incremental mode needs at least one watermark column
        if t.ingestion.mode == "incremental" and not t.ingestion.watermark_columns:
            issues.append(_err(
                f"{path}.ingestion.watermark_columns",
                "Incremental mode requires at least one watermark column "
                "(commonly create_ts and lst_updt_ts)",
            ))

        # Upsert needs merge_keys
        if t.write_mode == "upsert" and not t.merge_keys:
            issues.append(_err(
                f"{path}.merge_keys",
                "write_mode=upsert requires at least one merge_key",
            ))

        # Read-options parallel-read quartet must be all-or-nothing
        ro = t.read_options
        quartet = [ro.partitionColumn, ro.lowerBound, ro.upperBound, ro.numPartitions]
        n_set = sum(1 for v in quartet if v is not None)
        if 0 < n_set < 4:
            issues.append(_warn(
                f"{path}.read_options",
                "For parallel JDBC reads, set ALL of partitionColumn, lowerBound, "
                "upperBound, numPartitions — partial config falls back to single reader",
            ))

        # Target must be unique within the batch
        target_key = f"{t.target.schema_}.{t.target.table}"
        if target_key in seen_targets:
            issues.append(_err(
                f"{path}.target",
                f"Duplicate target {target_key} — also used by tables[{seen_targets[target_key]}]",
            ))
        seen_targets[target_key] = i

        # Tag warnings (non-blocking)
        if not t.tags.data_owner.strip():
            issues.append(_warn(
                f"{path}.tags.data_owner",
                "data_owner is empty — recommended for governance",
            ))

    ok = not any(iss.severity == "error" for iss in issues)
    return ok, issues
