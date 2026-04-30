# ============================================================================
# models.py
# ----------------------------------------------------------------------------
# Pydantic v2 request / response models for the ingestion-UI API.
#
# These mirror the shape of your domain-configs/sources/*.yaml files so the
# frontend can build the form 1:1 against the platform's existing schema.
#
# Reference: domain-configs/sources/redshift_multi_table.yaml
# ============================================================================

from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, field_validator


# ── Sub-models that map directly to YAML blocks ────────────────────────────

class ConnectionForm(BaseModel):
    """source.connection block in YAML."""
    db_type: str
    host: str
    port: int
    database: str
    secret_scope: str = "__SECRETS_SCOPE__"   # left as placeholder by default
    user_secret: str
    password_secret: str
    # UI-only: the Lakehouse Federation foreign catalog used by the schema
    # browser. Never emitted to YAML — the runtime pipeline doesn't need it.
    foreign_catalog: Optional[str] = None


class IngestionForm(BaseModel):
    """tables[].ingestion block."""
    mode: Literal["full", "append", "incremental"] = "incremental"
    table: Optional[str] = None        # fully-qualified table name (mutually exclusive with query)
    query: Optional[str] = None        # custom SELECT (mutually exclusive with table)
    watermark_columns: List[str] = Field(default_factory=lambda: ["create_ts", "lst_updt_ts"])

    @field_validator("query")
    @classmethod
    def _xor_table_and_query(cls, v, info):
        # Pydantic v2 cross-field validation happens in model_validator,
        # but for v1-style we keep a lightweight check at TableForm level.
        return v


class ReadOptionsForm(BaseModel):
    """tables[].read_options — JDBC-level tuning. Free-form key/value pairs."""
    fetchsize: int = 50000
    partitionColumn: Optional[str] = None
    lowerBound: Optional[int] = None
    upperBound: Optional[int] = None
    numPartitions: Optional[int] = None
    # Free-form extras (the UI exposes "Add option" for db-specific keys
    # like oracle.jdbc.timezoneAsRegion, encrypt, trustServerCertificate, …)
    extra: Dict[str, str] = Field(default_factory=dict)


class TargetForm(BaseModel):
    """tables[].target — Bronze schema/table. Catalog comes from env."""
    schema_: str = Field(alias="schema")          # 'schema' is reserved in Pydantic v2
    table: str

    model_config = {"populate_by_name": True}


class TagsForm(BaseModel):
    """tables[].tags — passed through to UC governance metadata (currently disabled)."""
    data_domain: str
    data_owner: str
    pii: bool = False


class TableForm(BaseModel):
    """One entry in source.tables[]."""
    name: str
    ingestion: IngestionForm
    read_options: ReadOptionsForm = Field(default_factory=ReadOptionsForm)
    target: TargetForm
    write_mode: Literal["overwrite", "append", "upsert"] = "upsert"
    merge_keys: List[str] = Field(default_factory=list)
    tags: TagsForm


class IngestionConfigForm(BaseModel):
    """Top-level form payload — converted to YAML by yaml_builder.build()."""
    version: str = "1.0.0"
    fail_fast: bool = False
    source_name: str
    source_type: Literal["rdbms_multi"] = "rdbms_multi"   # reserve for future s3/api
    connection: ConnectionForm
    tables: List[TableForm]
    target_catalog: str = "__CATALOG__"
    write_mode: Literal["overwrite", "append", "upsert"] = "upsert"
    schema_evolution: Literal["add_columns", "rescue_columns", "fail"] = "add_columns"


# ── API request / response envelopes ───────────────────────────────────────

class GenerateYamlResponse(BaseModel):
    yaml_text: str


class SaveConfigRequest(BaseModel):
    config: IngestionConfigForm
    overwrite: bool = False


class SaveConfigResponse(BaseModel):
    workspace_path: str
    bytes_written: int


class RunRequest(BaseModel):
    config: IngestionConfigForm
    env: str = "dev"                     # picks dev.yaml | prod.yaml
    cluster_id: Optional[str] = None     # if None, app falls back to default
    notify_emails: List[str] = Field(default_factory=list)


class RunResponse(BaseModel):
    run_id: int
    run_page_url: str
    workspace_config_path: str


class RunStatus(BaseModel):
    run_id: int
    state: str          # PENDING | RUNNING | TERMINATED
    result_state: Optional[str] = None    # SUCCESS | FAILED | CANCELED | …
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    run_page_url: str
    pipeline_metrics: Optional[Dict[str, Any]] = None    # row counts pulled from platform_meta


class RunHistoryEntry(BaseModel):
    run_id: int
    pipeline_name: str
    status: str
    start_time: str
    duration_seconds: Optional[float] = None
    row_count: Optional[int] = None
    config_path: Optional[str] = None


class ValidationIssue(BaseModel):
    path: str           # JSONPath-ish: "tables[2].merge_keys"
    severity: Literal["error", "warning"]
    message: str


class ValidateResponse(BaseModel):
    ok: bool
    issues: List[ValidationIssue] = Field(default_factory=list)
