"""
Hydration Agent 2 — Cross-Reference Resolution
Resolves import chains to build DEPENDS_ON edges between Job nodes.
Produces: DEPENDS_ON edges (Job→Job) for intra-project call chains.
Uses symbol resolution based on import paths, not naive name matching.
v3 ontology: Function→Job, CALLS→DEPENDS_ON, path stored in Job.domain.
"""
import ast
import hashlib
import os
from typing import AsyncGenerator

from agents.base_agent import BaseAgent, AgentEvent
from graph.neo4j_client import run_query
from graph import writer as _writer
from graph.schema import NodeLabel, RelType, VERIFIED


def _node_id(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]


class CrossRefResolverAgent(BaseAgent):
    name = "cross_ref_resolver"

    async def run(self) -> AsyncGenerator[AgentEvent, None]:
        await self.info("Starting cross-reference resolution")

        # Build symbol table: qualified_name → job_ids
        symbol_table: dict[str, list[str]] = {}
        try:
            jobs = await run_query(
                "MATCH (j:Job) RETURN j.id AS id, j.name AS name, j.domain AS path"
            )
            for j in jobs:
                key = (j["path"] or "").replace("/", ".").replace("\\", ".").rstrip(".py") + "." + j["name"]
                symbol_table.setdefault(key, []).append(j["id"])
        except Exception as exc:  # noqa: BLE001
            await self.warn(f"Neo4j unavailable, symbol table empty: {exc}")

        await self.info(f"Symbol table built: {len(symbol_table)} qualified names")

        py_files = []
        for dirpath, _, filenames in os.walk(self.repo_root):
            for fn in filenames:
                if fn.endswith(".py"):
                    py_files.append(os.path.join(dirpath, fn))

        deps_written = 0
        all_deps: list[dict] = []

        for fpath in py_files:
            rel = os.path.relpath(fpath, self.repo_root)
            try:
                source = open(fpath, encoding="utf-8", errors="replace").read()
                tree = ast.parse(source)
            except SyntaxError:
                continue

            # Extract imports from this file (used for qualified call resolution only)
            imported_modules: dict[str, str] = {}  # local_name → fully_qualified_name
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        local = alias.asname or alias.name
                        imported_modules[local] = alias.name
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for alias in node.names:
                        local = alias.asname or alias.name
                        # 'from X import Y' → imported_modules["Y"] = "X.Y" (already fully qualified)
                        imported_modules[local] = f"{module}.{alias.name}"

            file_deps = 0

            # Resolve function-to-function DEPENDS_ON edges
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                caller_id = _node_id("func", rel, node.name, str(node.lineno))

                for child in ast.walk(node):
                    if not isinstance(child, ast.Call):
                        continue
                    # Determine callee name
                    if isinstance(child.func, ast.Name):
                        callee_name = child.func.id
                    elif isinstance(child.func, ast.Attribute):
                        callee_name = child.func.attr
                    else:
                        continue

                    # Resolve: handle both 'from X import Y' and 'import X' styles
                    # For 'from X import Y': imported_modules["Y"] = "X.Y" (fully qualified — use directly)
                    # For 'import X': imported_modules["X"] = "X" (use as module prefix)
                    direct_qual = imported_modules.get(callee_name)
                    if direct_qual:
                        # Already fully qualified (e.g. "risk.credit_risk.score_customers_pd")
                        qualified = direct_qual
                    else:
                        mod_prefix = imported_modules.get(callee_name.split(".")[0], "")
                        qualified = f"{mod_prefix}.{callee_name}" if mod_prefix else callee_name
                    resolved = symbol_table.get(qualified) or symbol_table.get(callee_name)

                    if resolved:
                        for callee_id in resolved:
                            all_deps.append({
                                "from_id": caller_id,
                                "to_id": callee_id,
                                "confidence": VERIFIED,
                            })
                            deps_written += 1
                            file_deps += 1

            if file_deps > 0:
                await self.info(f"  {rel}: {file_deps} dependencies resolved")

        # Batch write: 1 round trip for all DEPENDS_ON edges
        await self.info(f"Writing {deps_written} DEPENDS_ON edges in batch...")
        await _writer.write_rels_batch(
            NodeLabel.JOB, RelType.DEPENDS_ON, NodeLabel.JOB, all_deps
        )

        await self.success(
            f"Cross-reference complete: {deps_written} DEPENDS_ON edges written"
        )
