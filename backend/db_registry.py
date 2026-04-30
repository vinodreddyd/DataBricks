# ============================================================================
# db_registry.py
# ----------------------------------------------------------------------------
# Single source of truth for supported RDBMS source types.
#
# Each entry mirrors the JDBC_DRIVERS table in
# platform-core/ingestion/bronze_rdbms_multi_template.py — keep these in sync
# whenever a new database is added on the platform side.
#
# The UI hits /api/db-types to populate the "DB Type" dropdown, then
# /api/templates/{db_type} to pre-fill port, driver hint, and secret-key
# defaults so the user only has to type host + DB name.
# ============================================================================

from typing import Dict, List, Optional
from pydantic import BaseModel


class DbTemplate(BaseModel):
    """Per-database defaults shown in the connection form."""
    db_type: str          # value written to YAML (source.connection.db_type)
    display_name: str     # label shown in dropdown
    default_port: int
    default_database: Optional[str] = None    # e.g. "master" for SQL Server
    jdbc_driver: str
    user_secret_default: str        # e.g. "redshift_user"
    password_secret_default: str    # e.g. "redshift_password"
    # Database-specific JDBC read_options the user might want to set.
    # Rendered as an "Advanced options" section in the table editor.
    suggested_read_options: Dict[str, str] = {}
    notes: Optional[str] = None     # tooltip text in the UI


# ── Registry ────────────────────────────────────────────────────────────────
# Order matters — the UI shows entries in this order in the dropdown.
DB_REGISTRY: Dict[str, DbTemplate] = {
    "redshift": DbTemplate(
        db_type="redshift",
        display_name="Amazon Redshift",
        default_port=5439,
        default_database="dev",
        jdbc_driver="com.amazon.redshift.jdbc42.Driver",
        user_secret_default="redshift_user",
        password_secret_default="redshift_password",
        suggested_read_options={"fetchsize": "50000"},
        notes="Requires com.amazon.redshift:redshift-jdbc42 on the cluster.",
    ),
    "oracle": DbTemplate(
        db_type="oracle",
        display_name="Oracle",
        default_port=1521,
        default_database="ORCL",
        jdbc_driver="oracle.jdbc.OracleDriver",
        user_secret_default="oracle_user",
        password_secret_default="oracle_password",
        suggested_read_options={
            "fetchsize": "5000",
            "oracle.jdbc.timezoneAsRegion": "false",
        },
        notes="Requires com.oracle.database.jdbc:ojdbc8 on the cluster.",
    ),
    "sqlserver": DbTemplate(
        db_type="sqlserver",
        display_name="Microsoft SQL Server",
        default_port=1433,
        default_database="master",
        jdbc_driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
        user_secret_default="sqlserver_user",
        password_secret_default="sqlserver_password",
        suggested_read_options={
            "fetchsize": "30000",
            "encrypt": "true",
            "trustServerCertificate": "false",
        },
        notes="JDBC URL uses ;databaseName=… property style.",
    ),
    "postgres": DbTemplate(
        db_type="postgres",
        display_name="PostgreSQL",
        default_port=5432,
        default_database="postgres",
        jdbc_driver="org.postgresql.Driver",
        user_secret_default="postgres_user",
        password_secret_default="postgres_password",
        suggested_read_options={"fetchsize": "20000"},
        notes="PostgreSQL driver is preinstalled on most Databricks runtimes.",
    ),
    "mysql": DbTemplate(
        db_type="mysql",
        display_name="MySQL",
        default_port=3306,
        default_database="mysql",
        jdbc_driver="com.mysql.cj.jdbc.Driver",
        user_secret_default="mysql_user",
        password_secret_default="mysql_password",
        suggested_read_options={"fetchsize": "10000"},
        notes="Requires com.mysql:mysql-connector-j on the cluster.",
    ),
    "db2": DbTemplate(
        db_type="db2",
        display_name="IBM DB2",
        default_port=50000,
        default_database="SAMPLE",
        jdbc_driver="com.ibm.db2.jcc.DB2Driver",
        user_secret_default="db2_user",
        password_secret_default="db2_password",
        suggested_read_options={"fetchsize": "5000"},
        notes="Requires com.ibm.db2:jcc on the cluster.",
    ),
}


def list_db_types() -> List[Dict[str, str]]:
    """Return the lightweight list used by the dropdown."""
    return [
        {"value": k, "label": v.display_name}
        for k, v in DB_REGISTRY.items()
    ]


def get_template(db_type: str) -> DbTemplate:
    """Return the full template (port, driver, secret defaults) for a db_type."""
    db_type = db_type.lower().strip()
    if db_type not in DB_REGISTRY:
        raise ValueError(
            f"Unsupported db_type '{db_type}'. "
            f"Supported: {sorted(DB_REGISTRY.keys())}"
        )
    return DB_REGISTRY[db_type]


# ── Ingestion-mode and write-mode enums (mirror the platform template) ────
INGESTION_MODES = ["full", "append", "incremental"]
WRITE_MODES     = ["overwrite", "append", "upsert"]

# Default watermark column names (most teams use these).
# Surfaced as a "Use defaults" button in the UI.
DEFAULT_WATERMARK_COLUMNS = ["create_ts", "lst_updt_ts"]
