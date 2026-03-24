"""
Hydration Agent 1 — AST Extraction
Parses every Python file using the built-in ast module.
Produces (DataLineageMetaModel v3):
  DataSource node  — root repository
  Script nodes     — one per .py file  (was: File)
  Job nodes        — one per function  (was: Function)
  PART_OF edges    — Script → Job
Computes SHA-256 per file; skips unchanged files on re-runs.
"""
import ast
import hashlib
import os
from typing import AsyncGenerator

from agents.base_agent import BaseAgent, AgentEvent
from graph import writer


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _node_id(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]


class ASTExtractorAgent(BaseAgent):
    name = "ast_extractor"

    async def run(self) -> AsyncGenerator[AgentEvent, None]:
        if self.force:
            await self.info("Force mode ON — bypassing file-hash cache")
        await self.info("Starting AST extraction", root=self.repo_root)

        repo_id = _node_id("repo", self.repo_root)
        await writer.write_data_source(
            {"id": repo_id, "name": os.path.basename(self.repo_root), "path": self.repo_root,
             "type": "file_store", "environment": "dev"},
            self.scan_run_id,
        )

        py_files = []
        for dirpath, _, filenames in os.walk(self.repo_root):
            for fn in filenames:
                if fn.endswith(".py"):
                    py_files.append(os.path.join(dirpath, fn))

        await self.info(f"Found {len(py_files)} Python files")

        skipped = 0
        processed = 0
        all_scripts: list[dict] = []
        all_jobs:    list[dict] = []

        for fpath in py_files:
            rel = os.path.relpath(fpath, self.repo_root)
            file_hash = _sha256(fpath)
            file_id = _node_id("file", rel)

            # Skip unchanged files (unless force mode is on)
            if not self.force:
                try:
                    from graph.neo4j_client import run_query  # noqa: PLC0415
                    existing = await run_query(
                        "MATCH (n:Script {id: $id}) RETURN n.hash AS h", {"id": file_id}
                    )
                    if existing and existing[0].get("h") == file_hash:
                        skipped += 1
                        continue
                except Exception:  # noqa: BLE001
                    pass  # cache unavailable — process file anyway

            await self.info(f"Parsing {rel}")

            try:
                source = open(fpath, encoding="utf-8", errors="replace").read()
                tree = ast.parse(source, filename=fpath)
            except SyntaxError as exc:
                await self.warn(f"SyntaxError in {rel}: {exc}")
                continue

            all_scripts.append(
                {"id": file_id, "name": os.path.basename(rel), "path": rel,
                 "repository": repo_id, "hash": file_hash, "language": "Python"}
            )

            file_funcs: list[str] = []

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    func_id = _node_id("func", rel, node.name, str(node.lineno))
                    decorators = [
                        ast.unparse(d) if hasattr(ast, "unparse") else d.id
                        if isinstance(d, ast.Name) else ""
                        for d in node.decorator_list
                    ]
                    risk_tags = []
                    for dec in decorators:
                        if "pii" in dec.lower():
                            risk_tags.append("PII")
                        if "audit" in dec.lower():
                            risk_tags.append("audit_required")
                        if "regulatory" in dec.lower():
                            risk_tags.append("regulatory_report")

                    all_jobs.append(
                        {
                            "id": func_id,
                            "name": node.name,
                            "domain": rel,
                            "type": "batch",
                            "line_start": node.lineno,
                            "line_end": getattr(node, "end_lineno", node.lineno),
                            "risk_tags": risk_tags,
                            "script_id": file_id,
                        }
                    )
                    file_funcs.append(f"{node.name}:L{node.lineno}")

            if file_funcs:
                preview = ", ".join(file_funcs[:6]) + ("…" if len(file_funcs) > 6 else "")
                await self.info(f"  → {len(file_funcs)} fn: {preview}")
            else:
                await self.info(f"  → (no functions)")

            processed += 1

        # Batch write: 2 round trips regardless of repo size
        await self.info(
            f"Writing {len(all_scripts)} scripts and {len(all_jobs)} jobs in batch..."
        )
        await writer.write_scripts_batch(all_scripts, self.scan_run_id)
        await writer.write_jobs_batch(all_jobs, self.scan_run_id)

        await self.success(
            f"AST extraction complete: {processed} files processed, {skipped} skipped (unchanged)"
        )
