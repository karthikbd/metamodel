from fastapi import APIRouter
from graph import queries

router = APIRouter()

_REL_WEIGHTS = {
    "WRITES_TO": 10,
    "READS_FROM": 6,
}

_TAG_WEIGHTS = {
    "regulatory_report": 12,
    "PII": 10,
    "audit_required": 6,
}


def _impact_level(score: int) -> str:
    if score >= 50:
        return "critical"
    if score >= 30:
        return "high"
    if score >= 15:
        return "medium"
    return "low"


def _clean_callers(row: dict) -> list[str]:
    return [c for c in (row.get("callers") or []) if c]


def _score_relationships(rels: set[str]) -> int:
    return sum(weight for rel, weight in _REL_WEIGHTS.items() if rel in rels)


def _score_tags(tags: set[str]) -> int:
    return sum(weight for tag, weight in _TAG_WEIGHTS.items() if tag in tags)


def _job_score(row: dict) -> int:
    rels = set(row.get("relationships") or [])
    tags = set(row.get("risk_tags") or [])
    callers = _clean_callers(row)
    score = _score_relationships(rels)
    score += min(len(callers), 5) * 4
    score += _score_tags(tags)
    return min(score, 100)


def _decorate_rows(rows: list[dict]) -> list[dict]:
    decorated: list[dict] = []
    for row in rows:
        score = _job_score(row)
        relationships = row.get("relationships") or []
        decorated.append({
            **row,
            "relationship": " + ".join(relationships) if relationships else "—",
            "impact_score": score,
            "impact_level": _impact_level(score),
            "caller_count": len(_clean_callers(row)),
        })
    return sorted(decorated, key=lambda r: (-r["impact_score"], r.get("name", "")))


def _count_rows_with_tag(rows: list[dict], tag: str) -> int:
    return sum(1 for r in rows if tag in (r.get("risk_tags") or []))


def _overall_score(writers: int, readers: int, caller_fanout: int, pii_jobs: int, regulatory_jobs: int, audit_jobs: int) -> int:
    raw = writers * 12 + readers * 7 + caller_fanout * 4 + pii_jobs * 10 + regulatory_jobs * 12 + audit_jobs * 6
    return min(100, raw)


def _build_summary(rows: list[dict]) -> dict:
    writers = sum(1 for r in rows if "WRITES_TO" in (r.get("relationships") or []))
    readers = sum(1 for r in rows if "READS_FROM" in (r.get("relationships") or []))
    caller_fanout = sum(r.get("caller_count", 0) for r in rows)
    pii_jobs = _count_rows_with_tag(rows, "PII")
    regulatory_jobs = _count_rows_with_tag(rows, "regulatory_report")
    audit_jobs = _count_rows_with_tag(rows, "audit_required")
    overall_score = _overall_score(writers, readers, caller_fanout, pii_jobs, regulatory_jobs, audit_jobs)
    return {
        "affected_jobs": len(rows),
        "writers": writers,
        "readers": readers,
        "downstream_callers": caller_fanout,
        "pii_jobs": pii_jobs,
        "regulatory_jobs": regulatory_jobs,
        "audit_jobs": audit_jobs,
        "impact_score": overall_score,
        "impact_level": _impact_level(overall_score),
    }


@router.get("/column")
async def column_impact(table: str, column: str):
    rows = _decorate_rows(await queries.column_impact(table, column))
    return {
        "table": table,
        "column": column,
        "affected": rows,
        "count": len(rows),
        "summary": _build_summary(rows),
    }


@router.get("/dataset")
async def dataset_impact(name: str):
    rows = _decorate_rows(await queries.dataset_impact(name))
    return {
        "dataset": name,
        "affected": rows,
        "count": len(rows),
        "summary": _build_summary(rows),
    }
