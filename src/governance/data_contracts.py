# ============================================================================
# Adaptive Data Governance Framework
# src/governance/data_contracts.py
# ============================================================================
# Declarative Data Contracts — "Data Quality as Code"
#
# Contracts are YAML-defined, versioned, and externalized from application
# logic.  Each contract specifies:
#   - Schema expectations (column types, nullability, uniqueness)
#   - Semantic rules (value ranges, regex patterns, cross-column checks)
#   - SLA thresholds (freshness, min DQ score)
#   - Ownership and consumer metadata
#
# The ContractEnforcer integrates with Great Expectations to validate
# DataFrames and auto-quarantine failures.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ============================================================================
# DataContract model
# ============================================================================

class DataContract:
    """A declarative, versioned data quality contract.

    Attributes
    ----------
    name : str
        Contract identifier (e.g. ``"orders_v2"``).
    version : str
        Semantic version.
    owner : str
        Domain team or person accountable.
    description : str
    consumers : list[str]
        Downstream teams that depend on this contract.
    schema : dict
        Column-level constraints::

            {
              "order_id": {"type": "string", "nullable": false, "unique": true},
              "order_value": {"type": "double", "nullable": false, "min": 0},
            }
    rules : list[dict]
        Business rules::

            [{"name": "delivery_after_purchase",
              "type": "cross_column",
              "condition": "delivery_date >= order_timestamp"}]
    sla : dict
        ``{"freshness_hours": 24, "min_dq_score": 85}``
    """

    def __init__(
        self,
        name: str,
        version: str = "1.0.0",
        owner: str = "",
        description: str = "",
        consumers: Optional[List[str]] = None,
        schema: Optional[Dict[str, Dict]] = None,
        rules: Optional[List[Dict]] = None,
        sla: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.version = version
        self.owner = owner
        self.description = description
        self.consumers = consumers or []
        self.schema = schema or {}
        self.rules = rules or []
        self.sla = sla or {"freshness_hours": 24, "min_dq_score": 85}

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "version": self.version,
            "owner": self.owner,
            "description": self.description,
            "consumers": self.consumers,
            "schema": self.schema,
            "rules": self.rules,
            "sla": self.sla,
        }

    @classmethod
    def from_yaml(cls, path: str) -> "DataContract":
        """Load a contract from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    def save_yaml(self, directory: str) -> Path:
        """Persist contract to a YAML file."""
        out = Path(directory)
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{self.name}_v{self.version}.yaml"
        with open(path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, sort_keys=False)
        return path


# ============================================================================
# ContractRegistry
# ============================================================================

class ContractRegistry:
    """Registry for managing and discovering data contracts.

    Persists contracts as YAML files under ``contracts_dir``.
    """

    def __init__(self, contracts_dir: str = "config/data_contracts"):
        self.contracts_dir = Path(contracts_dir)
        self.contracts_dir.mkdir(parents=True, exist_ok=True)
        self._contracts: Dict[str, DataContract] = {}
        self._load_all()

    def _load_all(self) -> None:
        for path in self.contracts_dir.glob("*.yaml"):
            try:
                contract = DataContract.from_yaml(str(path))
                self._contracts[contract.name] = contract
            except Exception as exc:
                logger.warning("Failed to load contract {}: {}", path, exc)

    def register(self, contract: DataContract) -> Path:
        """Register (or update) a contract."""
        self._contracts[contract.name] = contract
        path = contract.save_yaml(str(self.contracts_dir))
        logger.info("Contract registered: {} v{}", contract.name, contract.version)
        return path

    def get(self, name: str) -> Optional[DataContract]:
        return self._contracts.get(name)

    def list_all(self) -> List[Dict]:
        return [c.to_dict() for c in self._contracts.values()]

    def get_consumers(self, name: str) -> List[str]:
        """Who depends on this contract?"""
        c = self.get(name)
        return c.consumers if c else []


# ============================================================================
# ContractEnforcer
# ============================================================================

class ContractEnforcer:
    """Validate Spark DataFrames against Data Contracts.

    Integrates with Great Expectations for assertion evaluation and
    auto-quarantines records that violate the contract.

    Parameters
    ----------
    spark : SparkSession
    registry : ContractRegistry
    quarantine_path : str
        Delta path for quarantined records.
    """

    def __init__(
        self,
        spark: SparkSession,
        registry: Optional[ContractRegistry] = None,
        quarantine_path: str = "data/quarantine",
    ):
        self.spark = spark
        self.registry = registry or ContractRegistry()
        self.quarantine_path = Path(quarantine_path)

    def enforce(
        self,
        df: DataFrame,
        contract_name: str,
    ) -> Tuple[DataFrame, DataFrame, Dict]:
        """Validate *df* against the named contract.

        Returns
        -------
        (valid_df, quarantine_df, report)
            ``valid_df``       — rows that pass all checks.
            ``quarantine_df``  — rows that failed at least one check.
            ``report``         — summary dict with per-check results.
        """
        contract = self.registry.get(contract_name)
        if contract is None:
            raise ValueError(f"Contract '{contract_name}' not found in registry.")

        logger.info(
            "Enforcing contract '{}' v{} on DataFrame ({} rows)",
            contract.name, contract.version, df.count(),
        )

        all_conditions: List[Tuple[str, F.Column]] = []
        check_results: List[Dict] = []

        # ----- Schema checks -----
        for col_name, spec in contract.schema.items():
            # Not-null check
            if not spec.get("nullable", True):
                cond = F.col(col_name).isNotNull()
                all_conditions.append((f"{col_name}_not_null", cond))
                total = df.count()
                passing = df.filter(cond).count()
                check_results.append({
                    "check": f"{col_name}_not_null",
                    "passed": passing == total,
                    "pass_rate": round(passing / total * 100, 2) if total else 0,
                })

            # Min value check
            min_val = spec.get("min")
            if min_val is not None:
                cond = F.col(col_name) >= min_val
                all_conditions.append((f"{col_name}_min_{min_val}", cond))
                total = df.filter(F.col(col_name).isNotNull()).count()
                passing = df.filter(cond).count()
                check_results.append({
                    "check": f"{col_name}_min_{min_val}",
                    "passed": passing == total,
                    "pass_rate": round(passing / max(total, 1) * 100, 2),
                })

            # Max value check
            max_val = spec.get("max")
            if max_val is not None:
                cond = F.col(col_name) <= max_val
                all_conditions.append((f"{col_name}_max_{max_val}", cond))
                total = df.filter(F.col(col_name).isNotNull()).count()
                passing = df.filter(cond).count()
                check_results.append({
                    "check": f"{col_name}_max_{max_val}",
                    "passed": passing == total,
                    "pass_rate": round(passing / max(total, 1) * 100, 2),
                })

            # Regex pattern check
            regex = spec.get("regex")
            if regex:
                cond = F.col(col_name).rlike(regex)
                all_conditions.append((f"{col_name}_regex", cond))
                total = df.filter(F.col(col_name).isNotNull()).count()
                passing = df.filter(cond).count()
                check_results.append({
                    "check": f"{col_name}_regex",
                    "passed": passing == total,
                    "pass_rate": round(passing / max(total, 1) * 100, 2),
                })

        # ----- Business rules -----
        for rule in contract.rules:
            rule_name = rule.get("name", "unnamed")
            condition_expr = rule.get("condition", "true")
            try:
                cond = F.expr(condition_expr)
                all_conditions.append((rule_name, cond))
                total = df.count()
                passing = df.filter(cond).count()
                check_results.append({
                    "check": rule_name,
                    "passed": passing == total,
                    "pass_rate": round(passing / max(total, 1) * 100, 2),
                })
            except Exception as exc:
                logger.error("Rule '{}' failed to evaluate: {}", rule_name, exc)
                check_results.append({
                    "check": rule_name,
                    "passed": False,
                    "pass_rate": 0,
                    "error": str(exc),
                })

        # ----- Split valid / quarantine -----
        if all_conditions:
            composite = all_conditions[0][1]
            for _, cond in all_conditions[1:]:
                composite = composite & cond
            valid_df = df.filter(composite)
            quarantine_df = df.subtract(valid_df)
        else:
            valid_df = df
            quarantine_df = self.spark.createDataFrame([], df.schema)

        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        # ----- Write quarantine -----
        if quarantine_count > 0:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            q_path = str(self.quarantine_path / contract_name / ts)
            (
                quarantine_df
                .withColumn("_quarantine_reason", F.lit(f"contract:{contract_name}"))
                .withColumn("_quarantine_ts", F.current_timestamp())
                .write.format("delta").mode("append").save(q_path)
            )
            logger.warning(
                "Quarantined {} records → {}", quarantine_count, q_path,
            )

        # ----- Overall score -----
        total_checks = len(check_results)
        passed_checks = sum(1 for c in check_results if c["passed"])
        overall_score = round(passed_checks / max(total_checks, 1) * 100, 2)

        report = {
            "contract": contract_name,
            "version": contract.version,
            "owner": contract.owner,
            "timestamp": datetime.now().isoformat(),
            "total_rows": valid_count + quarantine_count,
            "valid_rows": valid_count,
            "quarantined_rows": quarantine_count,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "overall_score": overall_score,
            "sla_met": overall_score >= contract.sla.get("min_dq_score", 85),
            "checks": check_results,
        }

        if report["sla_met"]:
            logger.info(
                "✅  Contract '{}' PASSED — score {:.1f}%",
                contract_name, overall_score,
            )
        else:
            logger.warning(
                "❌  Contract '{}' FAILED — score {:.1f}% (SLA: {}%)",
                contract_name, overall_score,
                contract.sla.get("min_dq_score", 85),
            )

        return valid_df, quarantine_df, report
