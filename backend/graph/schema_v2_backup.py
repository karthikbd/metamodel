# ---------------------------------------------------------------------------
# Node labels
# ---------------------------------------------------------------------------
class NodeLabel:
    REPOSITORY    = "Repository"
    MODULE        = "Module"
    FILE          = "File"
    CLASS         = "Class"
    FUNCTION      = "Function"
    ARGUMENT      = "Argument"
    DECORATOR     = "Decorator"
    SCHEMA_OBJECT = "SchemaObject"
    EXTERNAL_DEP  = "ExternalDep"
    UNRESOLVED    = "Unresolved"
    DYNAMIC_SQL   = "DynamicSQL"
    LLM_SUMMARY        = "LLMSummary"
    BUSINESS_RULE      = "BusinessRule"
    SCAN_RUN           = "ScanRun"
    STM                = "STM"                # Source-to-Target Mapping target node
    PIPELINE_REGISTRY  = "PipelineRegistry"   # Phase 2 registered pipeline


# ---------------------------------------------------------------------------
# Relationship types
# ---------------------------------------------------------------------------
class RelType:
    # Code structure
    CONTAINS    = "CONTAINS"       # Repo→Module, Module→File, File→Class/Function
    DEFINES     = "DEFINES"        # Class→Function
    HAS_ARG     = "HAS_ARG"        # Function→Argument
    HAS_DECORATOR = "HAS_DECORATOR"# Function→Decorator
    CALLS       = "CALLS"          # Function→Function
    IMPORTS     = "IMPORTS"        # Function→ExternalDep

    # Data interaction
    READS       = "READS"          # Function→SchemaObject
    WRITES      = "WRITES"         # Function→SchemaObject
    DATA_FLOW   = "DATA_FLOW"      # SchemaObject→SchemaObject (with transform)

    # Governance
    GOVERNED_BY = "GOVERNED_BY"    # Function→BusinessRule
    HAS_SUMMARY = "HAS_SUMMARY"    # Function→LLMSummary

    # Lineage bridge
    MAPS_TO        = "MAPS_TO"         # SchemaObject→STM target node
    REGISTERED_AS  = "REGISTERED_AS"   # Function→PipelineRegistry


# ---------------------------------------------------------------------------
# Property keys (stable names referenced by queries)
# ---------------------------------------------------------------------------
class Prop:
    ID          = "id"
    NAME        = "name"
    PATH        = "path"
    FILE_HASH   = "file_hash"
    CONFIDENCE  = "confidence"      # "verified" | "inferred"
    STATUS      = "status"          # "active" | "deprecated"
    SCAN_RUN_ID = "scan_run_id"
    DEPRECATED_AT = "deprecated_at"
    REPLACED_BY = "replaced_by"
    RISK_TAGS   = "risk_tags"       # list: ["PII","audit_required","regulatory_report"]
    EXPRESSION  = "expression"      # DataFlow transform expression
    LINE_START  = "line_start"
    LINE_END    = "line_end"
    TABLE_NAME  = "table_name"
    COLUMN_NAME = "column_name"
    DATA_TYPE   = "data_type"
    MODEL_ID    = "model_id"
    TIMESTAMP   = "timestamp"
    SUMMARY     = "summary"


# ---------------------------------------------------------------------------
# Confidence constants
# ---------------------------------------------------------------------------
VERIFIED = "verified"
INFERRED = "inferred"
