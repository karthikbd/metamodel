"""
Shared decorators for PNC Meta Model sample system.
These decorators are detected by the AST Extractor agent and tagged
onto function nodes: pii_handler, audit_required, regulatory_report.
"""
import functools
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)


def pii_handler(func):
    """Mark a function as handling Personally Identifiable Information.
    Functions with this decorator must also carry @audit_required to be compliant.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.warning(f"[PII] Accessing PII in {func.__name__}")
        return func(*args, **kwargs)
    wrapper._is_pii = True
    return wrapper


def audit_required(func):
    """Mark a function as requiring audit trail logging.
    All PII-handling functions must carry this decorator.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = round(time.time() - start, 3)
        logger.info(f"[AUDIT] {func.__name__} completed in {elapsed}s at {datetime.utcnow().isoformat()}")
        return result
    wrapper._is_audited = True
    return wrapper


def regulatory_report(report_type: str = "GENERIC"):
    """Mark a function as producing a regulatory submission.
    Detected by AST Extractor and tagged regulatory_report risk_tag.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"[REGULATORY] Generating {report_type} report via {func.__name__}")
            return func(*args, **kwargs)
        wrapper._regulatory_type = report_type
        return wrapper
    return decorator


def deprecated_field(field_name: str, replacement: str = None):
    """Mark a function that accesses a deprecated field."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.warning(
                f"[DEPRECATED] {func.__name__} accesses deprecated field '{field_name}'. "
                f"Use '{replacement}' instead." if replacement else ""
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry(max_attempts: int = 3, delay: float = 1.0):
    """Retry decorator for transient DB failures."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Retry {attempt+1}/{max_attempts} for {func.__name__}: {exc}")
                    time.sleep(delay)
        return wrapper
    return decorator
