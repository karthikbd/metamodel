"""
Cross-module scheduler DAG for realistic job orchestration.

This script models production-like sequencing where jobs run across different
modules (etl/compliance/risk/reporting) with explicit dependencies and spacing.

Examples:
  python -m scripts.job_scheduler --date 2026-03-25 --source-file data/customer.csv
  python -m scripts.job_scheduler --date 2026-03-25 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable, Any

from utils.audit_log import emit_audit_event

from etl.customer_ingest import run_customer_ingest
from etl.risk_pipeline import run_daily_risk_pipeline
from compliance.aml_screening import run_daily_aml_screening
from compliance.kyc_validator import run_kyc_refresh_batch
from reporting.mis_daily import publish_daily_mis_pack
from reporting.ccar_report import run_ccar_annual_cycle
from reporting.basel3 import generate_pillar3_disclosure


logger = logging.getLogger(__name__)


@dataclass
class JobSpec:
    name: str
    fn: Callable[..., Any]
    depends_on: list[str] = field(default_factory=list)
    kwargs_factory: Callable[[date, argparse.Namespace], dict[str, Any]] = lambda *_: {}


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )


def _build_jobs() -> list[JobSpec]:
    return [
        JobSpec(
            name="customer_ingest",
            fn=run_customer_ingest,
            kwargs_factory=lambda d, a: {"source_file": a.source_file, "batch_date": d},
        ),
        JobSpec(
            name="kyc_refresh",
            fn=run_kyc_refresh_batch,
            depends_on=["customer_ingest"],
            kwargs_factory=lambda d, _a: {"as_of_date": d},
        ),
        JobSpec(
            name="daily_aml_screening",
            fn=run_daily_aml_screening,
            depends_on=["customer_ingest", "kyc_refresh"],
            kwargs_factory=lambda d, _a: {"business_date": d},
        ),
        JobSpec(
            name="daily_risk_pipeline",
            fn=run_daily_risk_pipeline,
            depends_on=["daily_aml_screening"],
            kwargs_factory=lambda d, _a: {"as_of_date": d},
        ),
        JobSpec(
            name="mis_daily_pack",
            fn=publish_daily_mis_pack,
            depends_on=["daily_risk_pipeline"],
            kwargs_factory=lambda d, _a: {"as_of_date": d},
        ),
        JobSpec(
            name="ccar_annual_cycle",
            fn=run_ccar_annual_cycle,
            depends_on=["daily_risk_pipeline"],
            kwargs_factory=lambda d, _a: {"base_date": d},
        ),
        JobSpec(
            name="basel_pillar3",
            fn=generate_pillar3_disclosure,
            depends_on=["mis_daily_pack", "ccar_annual_cycle"],
            kwargs_factory=lambda d, _a: {"as_of_date": d},
        ),
    ]


def _check_dependencies(job: JobSpec, completed: set[str]) -> None:
    missing = [dep for dep in job.depends_on if dep not in completed]
    if missing:
        raise RuntimeError(f"Job '{job.name}' dependencies not met: {missing}")


def _run_job(job: JobSpec, run_date: date, args: argparse.Namespace) -> None:
    payload = {
        "depends_on": job.depends_on,
        "run_date": run_date.isoformat(),
        "module": job.fn.__module__,
        "function": job.fn.__name__,
    }
    emit_audit_event(
        "job_execution",
        module=job.fn.__module__,
        function=job.fn.__name__,
        job=job.name,
        status="STARTED",
        metadata=payload,
    )

    start = time.time()
    try:
        kwargs = job.kwargs_factory(run_date, args)
        result = job.fn(**kwargs)
        elapsed_ms = int((time.time() - start) * 1000)
        emit_audit_event(
            "job_execution",
            module=job.fn.__module__,
            function=job.fn.__name__,
            job=job.name,
            status="SUCCESS",
            duration_ms=elapsed_ms,
            metadata={"result_type": type(result).__name__},
        )
        logger.info("Job completed: %s (%d ms)", job.name, elapsed_ms)
    except Exception as exc:
        elapsed_ms = int((time.time() - start) * 1000)
        emit_audit_event(
            "job_execution",
            module=job.fn.__module__,
            function=job.fn.__name__,
            job=job.name,
            status="FAILED",
            duration_ms=elapsed_ms,
            metadata={"error": str(exc)},
        )
        logger.exception("Job failed: %s", job.name)
        raise


def run_schedule_once(run_date: date, args: argparse.Namespace) -> None:
    jobs = _build_jobs()
    completed: set[str] = set()

    emit_audit_event(
        "scheduler_run",
        module=__name__,
        function="run_schedule_once",
        job="daily_batch",
        status="STARTED",
        metadata={"run_date": run_date.isoformat(), "job_count": len(jobs), "dry_run": args.dry_run},
    )

    try:
        for idx, job in enumerate(jobs, start=1):
            _check_dependencies(job, completed)
            logger.info("[%d/%d] %s", idx, len(jobs), job.name)
            if args.dry_run:
                emit_audit_event(
                    "job_execution",
                    module=job.fn.__module__,
                    function=job.fn.__name__,
                    job=job.name,
                    status="SKIPPED_DRY_RUN",
                    metadata={"depends_on": job.depends_on},
                )
            else:
                _run_job(job, run_date, args)
            completed.add(job.name)

            # Space between jobs to model realistic scheduler behavior
            if idx < len(jobs) and args.spacing_seconds > 0:
                logger.info("Sleeping %ss before next job", args.spacing_seconds)
                time.sleep(args.spacing_seconds)

        emit_audit_event(
            "scheduler_run",
            module=__name__,
            function="run_schedule_once",
            job="daily_batch",
            status="SUCCESS",
            metadata={"completed_jobs": sorted(completed)},
        )
    except Exception as exc:
        emit_audit_event(
            "scheduler_run",
            module=__name__,
            function="run_schedule_once",
            job="daily_batch",
            status="FAILED",
            metadata={"error": str(exc), "completed_jobs": sorted(completed)},
        )
        raise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run sample repo scheduler DAG once")
    parser.add_argument("--date", default=date.today().isoformat(), help="Business date (YYYY-MM-DD)")
    parser.add_argument(
        "--source-file",
        default="data/customer_extract.csv",
        help="Source file path for customer_ingest",
    )
    parser.add_argument(
        "--spacing-seconds",
        type=int,
        default=2,
        help="Pause between jobs to mimic scheduler spacing",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan and log sequence without executing jobs")
    return parser.parse_args()


def main() -> None:
    _setup_logging()
    args = _parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    run_schedule_once(run_date, args)


if __name__ == "__main__":
    main()
