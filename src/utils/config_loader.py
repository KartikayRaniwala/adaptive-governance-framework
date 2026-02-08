# ============================================================================
# Adaptive Data Governance Framework
# src/utils/config_loader.py
# ============================================================================
# Centralised configuration management.
# Loads from YAML and validates with Pydantic models.
# ============================================================================

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger
from pydantic import BaseModel, Field, field_validator


# ============================================================================
# Pydantic configuration models
# ============================================================================

class SparkConfig(BaseModel):
    app_name: str = "AdaptiveGovernance"
    master: str = "local[*]"
    executor_memory: str = "4g"
    executor_cores: int = 2
    driver_memory: str = "2g"
    sql: Dict[str, str] = Field(default_factory=dict)
    delta: Dict[str, bool] = Field(default_factory=lambda: {"enabled": True})


class StorageConfig(BaseModel):
    type: str = "local"
    base_path: str = "data"
    layers: Dict[str, str] = Field(default_factory=lambda: {
        "bronze": "data/bronze",
        "silver": "data/silver",
        "gold": "data/gold",
    })
    delta_lake: Dict[str, str] = Field(default_factory=dict)


class DataQualityConfig(BaseModel):
    enabled: bool = True
    validation_level: str = "strict"
    quarantine_enabled: bool = True
    quarantine_path: str = "data/quarantine"
    metrics_path: str = "data/metrics"
    expectations: List[Dict[str, Any]] = Field(default_factory=list)


class PIIDetectionConfig(BaseModel):
    enabled: bool = True
    model: str = "distilbert-base-uncased"
    confidence_threshold: float = 0.85
    masking_strategy: str = "hash"
    entities: List[str] = Field(default_factory=list)
    batch_size: int = 32
    max_length: int = 512


class ComplianceConfig(BaseModel):
    dpdp_enabled: bool = True
    consent_required: bool = True
    retention_days: int = 2555
    audit_logging: bool = True
    encryption_at_rest: bool = True


class MedallionLayerConfig(BaseModel):
    description: str = ""
    retention_days: int = 90
    partitioning: Optional[str] = None
    optimization: Optional[str] = None
    scd_type: Optional[int] = None
    aggregation_levels: Optional[List[str]] = None


class MedallionConfig(BaseModel):
    bronze: MedallionLayerConfig = Field(default_factory=MedallionLayerConfig)
    silver: MedallionLayerConfig = Field(default_factory=MedallionLayerConfig)
    gold: MedallionLayerConfig = Field(default_factory=MedallionLayerConfig)


class MonitoringConfig(BaseModel):
    enabled: bool = True
    prometheus_port: int = 9090
    metrics: List[str] = Field(default_factory=list)


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"
    output: str = "file"
    file_path: str = "logs/governance.log"
    rotation: str = "daily"


class ProjectConfig(BaseModel):
    """Top-level validated configuration."""
    name: str = "Adaptive Data Governance Framework"
    version: str = "1.0.0"
    environment: str = "development"


class FrameworkConfig(BaseModel):
    """Root configuration container that validates the full YAML."""
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    pii_detection: PIIDetectionConfig = Field(default_factory=PIIDetectionConfig)
    compliance: ComplianceConfig = Field(default_factory=ComplianceConfig)
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# ============================================================================
# Loader
# ============================================================================

_DEFAULT_CONFIG_PATH = Path("config/config.yaml")


def load_config(
    path: Optional[str | Path] = None,
) -> FrameworkConfig:
    """Load and validate the YAML configuration file.

    Parameters
    ----------
    path : str | Path | None
        Path to the YAML config.  Falls back to ``config/config.yaml``.

    Returns
    -------
    FrameworkConfig
        A fully validated Pydantic model of the configuration.
    """
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH

    if not config_path.exists():
        logger.warning(
            "Config file not found at {p}. Using defaults.",
            p=config_path,
        )
        return FrameworkConfig()

    with open(config_path, "r") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    config = FrameworkConfig(**raw)
    logger.info(
        "Configuration loaded from {p} (env={env})",
        p=config_path,
        env=config.project.environment,
    )
    return config


def load_raw_config(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Return the raw YAML dictionary without Pydantic validation."""
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH

    if not config_path.exists():
        logger.warning("Config file not found at {p}.", p=config_path)
        return {}

    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}
