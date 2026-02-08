# ============================================================================
# Adaptive Data Governance Framework
# src/utils/logger.py
# ============================================================================
# Structured JSON logging with correlation IDs.
# Uses Loguru for structured output compatible with ELK stack.
# ============================================================================

from __future__ import annotations

import sys
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Optional

from loguru import logger

# Context variable for correlation IDs across a pipeline run
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def set_correlation_id(cid: Optional[str] = None) -> str:
    """Set (or auto-generate) a correlation ID for the current context."""
    cid = cid or uuid.uuid4().hex[:12]
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    """Retrieve the current correlation ID."""
    return _correlation_id.get()


def _json_formatter(record: dict) -> str:
    """Produce a single-line JSON log entry with correlation ID."""
    import json
    from datetime import timezone

    ts = record["time"].astimezone(timezone.utc).isoformat()
    payload = {
        "timestamp": ts,
        "level": record["level"].name,
        "correlation_id": _correlation_id.get(""),
        "module": record["module"],
        "function": record["function"],
        "line": record["line"],
        "message": record["message"],
    }
    if record["exception"] is not None:
        payload["exception"] = str(record["exception"])
    return json.dumps(payload) + "\n"


def setup_logger(
    level: str = "INFO",
    log_dir: str = "logs",
    log_file: str = "governance.log",
    json_format: bool = True,
    console: bool = True,
    rotation: str = "10 MB",
    retention: str = "30 days",
) -> None:
    """Configure Loguru for the governance framework.

    Parameters
    ----------
    level : str
        Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    log_dir : str
        Directory to store log files.
    log_file : str
        Name of the log file.
    json_format : bool
        If True, logs are emitted as single-line JSON.
    console : bool
        Whether to also log to stderr.
    rotation : str
        When to rotate the log file (Loguru syntax).
    retention : str
        How long to keep rotated log files.
    """

    # Remove default Loguru handler
    logger.remove()

    fmt = _json_formatter if json_format else (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "{message}"
    )

    # Console handler
    if console:
        logger.add(
            sys.stderr,
            level=level,
            format=fmt if not json_format else "{message}",
            colorize=not json_format,
            serialize=json_format,
        )

    # File handler
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_path / log_file),
        level=level,
        format=fmt if not json_format else "{message}",
        rotation=rotation,
        retention=retention,
        compression="gz",
        serialize=json_format,
    )

    logger.info("Logger initialised — level={}, file={}", level, log_path / log_file)


# Convenience re-export so callers can do `from src.utils.logger import logger`
__all__ = [
    "logger",
    "setup_logger",
    "set_correlation_id",
    "get_correlation_id",
]
