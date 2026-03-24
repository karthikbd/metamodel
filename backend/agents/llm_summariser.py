"""
Hydration Agent 4 — LLM Summarisation
Runs last. Queries graph for context per Job, then calls an LLM.
Produces: LLMSummary nodes attached to Job nodes.
Only runs on Jobs that touch a Dataset OR have > 20 lines.
All outputs are marked confidence: inferred — never used for Phase 2 field resolution.
v3 ontology: Function→Job, SchemaObject→Dataset, READS→READS_FROM, WRITES→WRITES_TO.
"""
import hashlib
import json
from typing import AsyncGenerator

from agents.base_agent import BaseAgent, AgentEvent
from graph.neo4j_client import run_query
from graph.writer import write_llm_summary
from config import settings


def _node_id(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]


class LLMSummariserAgent(BaseAgent):
    name = "llm_summariser"

    async def run(self) -> AsyncGenerator[AgentEvent, None]:
        await self.info("Starting LLM summarisation")

        if not settings.openai_api_key:
            await self.warn("OPENAI_API_KEY not set — running in stub mode")
            use_stub = True
        else:
            use_stub = False

        # Candidates: Jobs touching a Dataset OR > 20 lines
        try:
            candidates = await run_query("""
                MATCH (j:Job)
                WHERE (j)-[:READS_FROM|WRITES_TO]->(:Dataset)
                   OR (j.line_end - j.line_start) > 20
                OPTIONAL MATCH (j)-[:READS_FROM]->(r:Dataset)
                OPTIONAL MATCH (j)-[:WRITES_TO]->(w:Dataset)
                OPTIONAL MATCH (j)-[:GOVERNED_BY]->(b:BusinessRule)
                WITH j,
                     collect(DISTINCT r.name) AS reads,
                     collect(DISTINCT w.name) AS writes,
                     collect(DISTINCT b.name) AS rules
                RETURN j.id AS id, j.name AS name, j.domain AS path,
                       j.risk_tags AS risk_tags,
                       reads, writes, rules,
                       (j.line_end - j.line_start) AS lines
            """)
        except Exception as exc:  # noqa: BLE001
            await self.warn(f"Neo4j unavailable, no candidates to summarise: {exc}")
            candidates = []

        await self.info(f"Found {len(candidates)} jobs to summarise")
        summarised = 0

        for fn in candidates:
            reads_str  = ", ".join(fn["reads"][:3])  or "—"
            writes_str = ", ".join(fn["writes"][:3]) or "—"
            await self.info(
                f"[{summarised + 1}/{len(candidates)}] {fn['name']} "
                f"({fn['path'] or 'unknown'}) | reads: {reads_str} | writes: {writes_str}"
            )

            context = {
                "function":   fn["name"],
                "path":       fn["path"],
                "reads":      fn["reads"],
                "writes":     fn["writes"],
                "rules":      fn["rules"],
                "risk_tags":  fn["risk_tags"] or [],
                "lines":      fn["lines"],
            }

            if use_stub:
                summary = (
                    f"[STUB] {fn['name']} reads from "
                    f"{', '.join(fn['reads'][:3]) or 'no datasets'} "
                    f"and writes to {', '.join(fn['writes'][:3]) or 'no datasets'}."
                )
                model_id = "stub"
            else:
                summary, model_id = await self._call_llm(context)

            summary_id = _node_id("summary", fn["id"])
            await write_llm_summary(
                {
                    "id":     summary_id,
                    "job_id": fn["id"],
                    "summary": summary,
                    "model_id": model_id,
                },
                self.scan_run_id,
            )
            summarised += 1

        await self.success(f"LLM summarisation complete: {summarised} summaries written")

    async def _call_llm(self, context: dict) -> tuple[str, str]:
        """Call OpenAI and return (summary, model_id)."""
        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=settings.openai_api_key)
            prompt = (
                "You are analysing a Python function extracted from a legacy codebase. "
                "The following facts are VERIFIED (from AST analysis):\n"
                f"{json.dumps(context, indent=2)}\n\n"
                "Write a concise one-paragraph summary of this function's purpose, "
                "what data it operates on, and any notable governance concerns. "
                "Do not invent facts beyond what is provided."
            )
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
            )
            return resp.choices[0].message.content.strip(), "gpt-4o-mini"
        except Exception as exc:
            await self.warn(f"LLM call failed: {exc}")
            return f"[LLM_ERROR] {exc}", "error"
