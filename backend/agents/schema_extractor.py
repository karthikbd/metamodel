"""
Hydration Agent 3 — Schema & Transformation Extraction
DataLineageMetaModel v3.
Extracts:
  - Dataset nodes from SQL table references
  - Column nodes from SQL column references
  - READS_FROM / WRITES_TO edges from Job → Dataset
  - DERIVED_FROM edges with transform expressions
  - Flags DYNAMIC_SQL where string interpolation prevents static analysis
"""
import ast
import hashlib
import os
import re
from typing import AsyncGenerator

from agents.base_agent import BaseAgent, AgentEvent
from graph.writer import merge_node, write_datasets_batch, write_columns_batch, write_rels_batch
from graph.schema import NodeLabel, RelType, VERIFIED


def _node_id(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]


# Simple regex patterns for SQL detection
# Optional schema prefix: (?:\w+\.)? handles dbo.table_name → captures table_name only
_SELECT_RE  = re.compile(r"SELECT\s+(.*?)\s+FROM\s+(?:\w+\.)?([\w]+)(?:\s|$|,|\]|\))", re.I | re.S)
_INSERT_RE  = re.compile(r"INSERT\s+INTO\s+(?:\w+\.)?([\w]+)\s*\(([^)]+)\)", re.I)
_UPDATE_RE  = re.compile(r"UPDATE\s+(?:\w+\.)?([\w]+)\s+SET\s+(.+?)(?:\s+WHERE|$)", re.I | re.S)
_INTERP_RE  = re.compile(r"(%s|\{[^}]+\}|f['\"])", re.I)


def _clean_column(raw: str) -> str:
    """Normalise a raw column token extracted from SQL."""
    raw = raw.strip()
    # Skip SQL comment lines
    if raw.startswith('--') or raw.startswith('/*'):
        return ''
    # Handle 'expr AS alias' — take the alias
    as_match = re.split(r'\s+AS\s+', raw, flags=re.I)
    if len(as_match) > 1:
        raw = as_match[-1].strip()
    # Unwrap common aggregate calls: SUM(col), MAX(col), etc.
    raw = re.sub(r'^\w+\(([^)]+)\)$', r'\1', raw).strip()
    # Strip table qualifier: table.col → col
    raw = raw.split('.')[-1].strip()
    # Remove surrounding quotes
    raw = raw.strip('"\'')
    # Reject wildcards, purely numeric tokens, empty
    if not raw or raw == '*' or re.match(r'^\d', raw) or len(raw) > 80:
        return ''
    # Reject tokens that are clearly SQL keywords or look like code
    _KEYWORDS = {'null', 'true', 'false', 'cast', 'case', 'when', 'then',
                 'else', 'end', 'and', 'or', 'not', 'in', 'is', 'like'}
    if raw.lower() in _KEYWORDS:
        return ''
    return raw

# Pandas patterns: df["col"], df.col, df[["a","b"]]
_PD_COL_RE  = re.compile(r'\.(?:loc|iloc|__getitem__)\[(?:["\'](\w+)["\'])\]')
_PD_ACCESS  = re.compile(r'(?:df|data|result|frame)\[["\'](\w+)["\']\]')


class SchemaExtractorAgent(BaseAgent):
    name = "schema_extractor"

    async def run(self) -> AsyncGenerator[AgentEvent, None]:
        await self.info("Starting schema and transformation extraction")

        py_files = []
        for dirpath, _, filenames in os.walk(self.repo_root):
            for fn in filenames:
                if fn.endswith(".py"):
                    py_files.append(os.path.join(dirpath, fn))

        reads_written  = 0
        writes_written = 0
        schemas_found  = 0
        dynamic_flags  = 0

        all_datasets:   list[dict] = []
        all_columns:    list[dict] = []
        all_read_rels:  list[dict] = []
        all_write_rels: list[dict] = []

        for fpath in py_files:
            rel = os.path.relpath(fpath, self.repo_root)
            try:
                source = open(fpath, encoding="utf-8", errors="replace").read()
                tree = ast.parse(source)
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue

                func_id = _node_id("func", rel, node.name, str(node.lineno))
                func_source = ast.get_source_segment(source, node) or ""

                # ---- SQL string analysis ----
                for child in ast.walk(node):
                    if not isinstance(child, ast.Constant) or not isinstance(child.s if hasattr(child, 's') else child.value, str):
                        continue
                    sql = child.value if isinstance(child.value, str) else ""
                    if not sql or not any(kw in sql.upper() for kw in ("SELECT", "INSERT", "UPDATE", "FROM")):
                        continue

                    # Flag dynamic SQL (kept as inline write — exceptional path)
                    if _INTERP_RE.search(sql):
                        dsql_id = _node_id("dynsql", func_id, sql[:60])
                        await merge_node(
                            NodeLabel.DYNAMIC_SQL,
                            {"id": dsql_id},
                            {"snippet": sql[:200], "function_id": func_id,
                             "confidence": VERIFIED},
                            self.scan_run_id,
                        )
                        dynamic_flags += 1
                        await self.warn(f"DYNAMIC_SQL in {rel}:{node.name}")
                        continue

                    # SELECT → READS_FROM
                    for m in _SELECT_RE.finditer(sql):
                        cols_raw, table = m.group(1), m.group(2)
                        ds_id = _node_id("dataset", table)
                        all_datasets.append({"id": ds_id, "name": table, "qualified_name": table})
                        all_read_rels.append({"from_id": func_id, "to_id": ds_id, "confidence": VERIFIED})
                        reads_written += 1
                        schemas_found += 1
                        for col in [_clean_column(c) for c in cols_raw.split(",")]:
                            if not col:
                                continue
                            col_id = _node_id("column", table, col)
                            all_columns.append({
                                "id": col_id, "name": col,
                                "qualified_name": f"{table}.{col}",
                                "dataset_id": ds_id,
                            })

                    # INSERT → WRITES_TO
                    for m in _INSERT_RE.finditer(sql):
                        table, cols_raw = m.group(1), m.group(2)
                        ds_id = _node_id("dataset", table)
                        all_datasets.append({"id": ds_id, "name": table, "qualified_name": table})
                        all_write_rels.append({"from_id": func_id, "to_id": ds_id, "confidence": VERIFIED})
                        writes_written += 1
                        for col in [_clean_column(c) for c in cols_raw.split(",")]:
                            if not col:
                                continue
                            col_id = _node_id("column", table, col)
                            all_columns.append({
                                "id": col_id, "name": col,
                                "qualified_name": f"{table}.{col}",
                                "dataset_id": ds_id,
                            })
                            schemas_found += 1

                    # UPDATE → WRITES_TO
                    for m in _UPDATE_RE.finditer(sql):
                        table, set_clause = m.group(1), m.group(2)
                        ds_id = _node_id("dataset", table)
                        all_datasets.append({"id": ds_id, "name": table, "qualified_name": table})
                        all_write_rels.append({"from_id": func_id, "to_id": ds_id, "confidence": VERIFIED})
                        writes_written += 1
                        for assignment in set_clause.split(","):
                            parts = assignment.strip().split("=")
                            if parts:
                                col = _clean_column(parts[0])
                                if not col:
                                    continue
                                col_id = _node_id("column", table, col)
                                all_columns.append({
                                    "id": col_id, "name": col,
                                    "qualified_name": f"{table}.{col}",
                                    "dataset_id": ds_id,
                                })
                                schemas_found += 1

                # ---- Pandas column access ----
                for col in _PD_ACCESS.findall(func_source):
                    ds_id = _node_id("dataset", "dataframe")
                    all_datasets.append({"id": ds_id, "name": "dataframe", "qualified_name": "dataframe"})
                    all_read_rels.append({"from_id": func_id, "to_id": ds_id, "confidence": VERIFIED})
                    reads_written += 1
                    col_id = _node_id("column", "dataframe", col)
                    all_columns.append({
                        "id": col_id, "name": col,
                        "qualified_name": f"dataframe.{col}",
                        "dataset_id": ds_id,
                    })

        # Batch write: 4 round trips regardless of repo size
        await self.info(
            f"Writing {len(set(d['id'] for d in all_datasets))} datasets, "
            f"{len(set(c['id'] for c in all_columns))} columns, "
            f"{len(all_read_rels)} READS_FROM, {len(all_write_rels)} WRITES_TO in batch..."
        )
        await write_datasets_batch(all_datasets, self.scan_run_id)
        await write_columns_batch(all_columns, self.scan_run_id)
        await write_rels_batch(NodeLabel.JOB, RelType.READS_FROM, NodeLabel.DATASET, all_read_rels)
        await write_rels_batch(NodeLabel.JOB, RelType.WRITES_TO, NodeLabel.DATASET, all_write_rels)

        await self.success(
            f"Schema extraction complete: {schemas_found} Dataset/Column nodes, "
            f"{reads_written} READS_FROM, {writes_written} WRITES_TO, "
            f"{dynamic_flags} DYNAMIC_SQL flags"
        )
