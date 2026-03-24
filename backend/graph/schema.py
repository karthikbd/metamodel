"""DataLineageMetaModel v3 — Node Labels, Relationship Types, and Property Keys.

Schema version: v1.2  (constraints / indexes created directly in AuraDB)
"""


# ---------------------------------------------------------------------------
# Node labels — v3 ontology
# ---------------------------------------------------------------------------
class NodeLabel:
    DATA_SOURCE    = "DataSource"       # Source / target system (was: Repository)
    DATASET        = "Dataset"          # Table / view / file  (was: SchemaObject table)
    COLUMN         = "Column"           # Dataset column       (was: SchemaObject column)
    SCRIPT         = "Script"           # Executable code file (was: File + Function merged)
    JOB            = "Job"              # Pipeline / use-case  (was: Function + PipelineRegistry)
    TRANSFORMATION = "Transformation"   # In-code logic unit   (was: DataFlow edge data)
    DASHBOARD      = "Dashboard"        # BI dashboard (new)
    ALIAS          = "Alias"            # Column alias / business name (new)

    # Internal tracking — not part of the public ontology
    SCAN_RUN    = "ScanRun"
    DYNAMIC_SQL = "DynamicSQL"   # flagged for manual review

    # Kept for LLM / governance agents
    LLM_SUMMARY   = "LLMSummary"
    BUSINESS_RULE = "BusinessRule"
    STM           = "STM"            # Source-to-Target Mapping (legacy bridge)


# ---------------------------------------------------------------------------
# Relationship types — v3 ontology
# ---------------------------------------------------------------------------
class RelType:
    # Structure
    HAS_DATASET  = "HAS_DATASET"    # DataSource → Dataset
    HAS_COLUMN   = "HAS_COLUMN"     # Dataset → Column

    # Data interaction
    READS_FROM   = "READS_FROM"     # Job / Script → Dataset   (was: READS)
    WRITES_TO    = "WRITES_TO"      # Job / Script → Dataset   (was: WRITES)

    # Execution graph
    PART_OF      = "PART_OF"        # Script → Job             (was: REGISTERED_AS)
    CONTAINS     = "CONTAINS"       # Script → Transformation
    DEPENDS_ON   = "DEPENDS_ON"     # Job → Job  /  Script → Script  (was: CALLS)

    # Transformation detail
    USES         = "USES"           # Transformation → Column  /  Dashboard → Dataset
    PRODUCES     = "PRODUCES"       # Transformation → Column
    DERIVED_FROM = "DERIVED_FROM"   # Column → Column (with expression + confidence)

    # Semantic layer
    HAS_ALIAS    = "HAS_ALIAS"      # Column → Alias

    # Governance (kept for LLM / compliance agents)
    GOVERNED_BY  = "GOVERNED_BY"
    HAS_SUMMARY  = "HAS_SUMMARY"

    # Legacy lineage bridge (STM)
    MAPS_TO      = "MAPS_TO"        # Column → STM


# ---------------------------------------------------------------------------
# Property keys
# ---------------------------------------------------------------------------
class Prop:
    # Universal
    ID            = "id"
    NAME          = "name"
    STATUS        = "status"          # "active" | "deprecated"
    CONFIDENCE    = "confidence"      # "verified" | "inferred"
    SCAN_RUN_ID   = "scan_run_id"
    CREATED_AT    = "created_at"
    UPDATED_AT    = "updated_at"
    DEPRECATED_AT = "deprecated_at"
    TAGS          = "tags"
    OWNER         = "owner"
    DESCRIPTION   = "description"

    # DataSource
    TYPE          = "type"            # "database" | "file_store" | "api" | "stream"
    SUBTYPE       = "subtype"         # "postgres" | "s3" | "kafka" …
    HOST          = "host"
    PORT          = "port"
    ENVIRONMENT   = "environment"     # "dev" | "qa" | "prod"

    # Dataset
    QUALIFIED_NAME    = "qualified_name"
    DATASOURCE_ID     = "datasource_id"
    FORMAT            = "format"          # "table" | "view" | "parquet" | "csv" …
    LOCATION          = "location"
    SCHEMA_VERSION    = "schema_version"
    PARTITION_KEYS    = "partition_keys"
    REFRESH_FREQUENCY = "refresh_frequency"

    # Column
    DATASET_ID       = "dataset_id"
    DATA_TYPE        = "data_type"
    NULLABLE         = "nullable"
    IS_PRIMARY_KEY   = "is_primary_key"
    IS_FOREIGN_KEY   = "is_foreign_key"
    PII_FLAG         = "pii_flag"
    SENSITIVE_FLAG   = "sensitive_flag"
    ORDINAL_POSITION = "ordinal_position"

    # Script
    PATH             = "path"
    REPOSITORY       = "repository"
    BRANCH           = "branch"
    LANGUAGE         = "language"         # "Python" | "SQL" | "PySpark"
    HASH             = "hash"             # SHA-256 (was: file_hash)
    EXECUTION_ENGINE = "execution_engine"
    LINE_START       = "line_start"
    LINE_END         = "line_end"
    SCHEDULE         = "schedule"
    LAST_MODIFIED    = "last_modified"
    RISK_TAGS        = "risk_tags"

    # Job
    DOMAIN       = "domain"
    PIPELINE_TOOL = "pipeline_tool"
    TEAM         = "team"

    # Transformation
    LOGIC            = "logic"
    NORMALIZED_LOGIC = "normalized_logic"
    COMPLEXITY_SCORE = "complexity_score"
    IS_AGGREGATION   = "is_aggregation"
    IS_JOIN          = "is_join"
    IS_FILTER        = "is_filter"

    # Alias
    ALIAS_TYPE = "alias_type"    # "business" | "technical"

    # Edge properties
    EXPRESSION = "expression"
    ROLE       = "role"
    SEQUENCE   = "sequence"
    CRITICAL   = "critical"

    # Legacy / internal
    FILE_HASH  = "hash"          # alias — writers use Prop.HASH
    TIMESTAMP  = "timestamp"
    SUMMARY    = "summary"


# ---------------------------------------------------------------------------
# Confidence level constants
# ---------------------------------------------------------------------------
VERIFIED = "verified"
INFERRED = "inferred"
