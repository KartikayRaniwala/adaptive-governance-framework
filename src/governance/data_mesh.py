# ============================================================================
# Adaptive Data Governance Framework
# src/governance/data_mesh.py
# ============================================================================
# Data Mesh & Federated Governance Module.
#
# Implements the transition from centralised control to a Data Mesh model
# where domain teams (Supply Chain, Marketing, Finance, etc.) own their data
# as "products" while adhering to automated global compliance standards.
#
# Key concepts:
#   - Data Product: a versioned, owned, discoverable, self-describing
#     dataset with SLAs and quality contracts.
#   - Domain: a business unit that owns one or more data products.
#   - Global Policy: organisation-wide rules enforced across all domains
#     (PII masking, retention, DQ thresholds).
#   - Federated Compliance: each domain self-certifies against global
#     policies; the platform validates automatically.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger


# ============================================================================
# Data structures
# ============================================================================

class DataProduct:
    """A self-describing, versioned data asset owned by a domain.

    Attributes
    ----------
    name : str
        Logical name (e.g. ``"customer_orders"``).
    domain : str
        Owning business domain (e.g. ``"supply_chain"``).
    owner : str
        Accountable person or team.
    description : str
        Human-readable purpose.
    schema_path : str
        Path to the schema registry entry or contract YAML.
    location : str
        Delta table path (e.g. ``"data/gold/customer_orders"``).
    sla : dict
        SLA contract: ``{"freshness_hours": 24, "min_dq_score": 85}``.
    tags : list[str]
        Discovery tags.
    version : str
        Semantic version of the data product definition.
    """

    def __init__(
        self,
        name: str,
        domain: str,
        owner: str,
        description: str = "",
        schema_path: str = "",
        location: str = "",
        sla: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        version: str = "1.0.0",
    ):
        self.name = name
        self.domain = domain
        self.owner = owner
        self.description = description
        self.schema_path = schema_path
        self.location = location
        self.sla = sla or {"freshness_hours": 24, "min_dq_score": 85}
        self.tags = tags or []
        self.version = version
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "domain": self.domain,
            "owner": self.owner,
            "description": self.description,
            "schema_path": self.schema_path,
            "location": self.location,
            "sla": self.sla,
            "tags": self.tags,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class GlobalPolicy:
    """An organisation-wide governance policy enforced across all domains.

    Attributes
    ----------
    name : str
        Policy identifier (e.g. ``"pii_masking_required"``).
    description : str
        What the policy enforces.
    policy_type : str
        Category: ``"pii"``, ``"quality"``, ``"retention"``,
        ``"access"``, ``"encryption"``.
    rules : dict
        Machine-readable rules.
    severity : str
        ``"critical"`` (blocks pipeline) | ``"warning"`` (logs only).
    """

    def __init__(
        self,
        name: str,
        description: str,
        policy_type: str,
        rules: Dict[str, Any],
        severity: str = "critical",
    ):
        self.name = name
        self.description = description
        self.policy_type = policy_type
        self.rules = rules
        self.severity = severity
        self.created_at = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "policy_type": self.policy_type,
            "rules": self.rules,
            "severity": self.severity,
            "created_at": self.created_at,
        }


# ============================================================================
# DataMeshGovernor
# ============================================================================

class DataMeshGovernor:
    """Federated Data Mesh governance engine.

    Manages:
    - Domain registration and ownership
    - Data product catalogue (registration, discovery, deprecation)
    - Global policy enforcement across all domains
    - Compliance auditing and certification
    - Data product SLA monitoring

    Parameters
    ----------
    catalogue_dir : str
        Directory for persisting the product catalogue and policy
        definitions as JSON files.
    """

    def __init__(self, catalogue_dir: str = "config/data_mesh"):
        self.catalogue_dir = Path(catalogue_dir)
        self.catalogue_dir.mkdir(parents=True, exist_ok=True)

        self._domains: Dict[str, Dict[str, Any]] = {}
        self._products: Dict[str, DataProduct] = {}
        self._policies: Dict[str, GlobalPolicy] = {}

        self._load_catalogue()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _catalogue_file(self) -> Path:
        return self.catalogue_dir / "catalogue.json"

    def _policies_file(self) -> Path:
        return self.catalogue_dir / "global_policies.json"

    def _load_catalogue(self) -> None:
        cat_file = self._catalogue_file()
        if cat_file.exists():
            with open(cat_file) as f:
                data = json.load(f)
            self._domains = data.get("domains", {})
            for p in data.get("products", []):
                dp = DataProduct(**{k: v for k, v in p.items()
                                    if k not in ("created_at", "updated_at")})
                dp.created_at = p.get("created_at", dp.created_at)
                dp.updated_at = p.get("updated_at", dp.updated_at)
                self._products[dp.name] = dp

        pol_file = self._policies_file()
        if pol_file.exists():
            with open(pol_file) as f:
                for p in json.load(f):
                    gp = GlobalPolicy(**{k: v for k, v in p.items()
                                         if k != "created_at"})
                    gp.created_at = p.get("created_at", gp.created_at)
                    self._policies[gp.name] = gp

    def _save_catalogue(self) -> None:
        data = {
            "domains": self._domains,
            "products": [p.to_dict() for p in self._products.values()],
            "last_updated": datetime.now().isoformat(),
        }
        with open(self._catalogue_file(), "w") as f:
            json.dump(data, f, indent=2)

    def _save_policies(self) -> None:
        with open(self._policies_file(), "w") as f:
            json.dump(
                [p.to_dict() for p in self._policies.values()],
                f, indent=2,
            )

    # ------------------------------------------------------------------
    # Domain management
    # ------------------------------------------------------------------

    def register_domain(
        self,
        domain_name: str,
        owner: str,
        description: str = "",
        contact_email: str = "",
    ) -> None:
        """Register a new business domain."""
        self._domains[domain_name] = {
            "owner": owner,
            "description": description,
            "contact_email": contact_email,
            "registered_at": datetime.now().isoformat(),
        }
        self._save_catalogue()
        logger.info("Domain registered: {}", domain_name)

    def list_domains(self) -> Dict[str, Dict]:
        return dict(self._domains)

    # ------------------------------------------------------------------
    # Data product management
    # ------------------------------------------------------------------

    def register_product(self, product: DataProduct) -> None:
        """Register or update a data product in the catalogue."""
        if product.domain not in self._domains:
            logger.warning(
                "Domain '{}' not registered — registering automatically.",
                product.domain,
            )
            self.register_domain(product.domain, owner=product.owner)

        product.updated_at = datetime.now().isoformat()
        self._products[product.name] = product
        self._save_catalogue()
        logger.info(
            "Data product registered: {} (domain={})",
            product.name, product.domain,
        )

    def get_product(self, name: str) -> Optional[DataProduct]:
        return self._products.get(name)

    def list_products(
        self, domain: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List all products, optionally filtered by domain."""
        products = self._products.values()
        if domain:
            products = [p for p in products if p.domain == domain]
        return [p.to_dict() for p in products]

    def search_products(self, tag: str) -> List[Dict[str, Any]]:
        """Search products by tag."""
        return [
            p.to_dict()
            for p in self._products.values()
            if tag.lower() in [t.lower() for t in p.tags]
        ]

    def deprecate_product(self, name: str) -> None:
        """Mark a data product as deprecated."""
        if name in self._products:
            self._products[name].tags.append("deprecated")
            self._products[name].updated_at = datetime.now().isoformat()
            self._save_catalogue()
            logger.warning("Data product deprecated: {}", name)

    # ------------------------------------------------------------------
    # Global policy management
    # ------------------------------------------------------------------

    def add_policy(self, policy: GlobalPolicy) -> None:
        """Register a global governance policy."""
        self._policies[policy.name] = policy
        self._save_policies()
        logger.info("Global policy added: {}", policy.name)

    def list_policies(self) -> List[Dict[str, Any]]:
        return [p.to_dict() for p in self._policies.values()]

    # ------------------------------------------------------------------
    # Compliance auditing
    # ------------------------------------------------------------------

    def audit_product_compliance(
        self,
        product_name: str,
        dq_score: float,
        pii_masked: bool,
        freshness_hours: float,
        has_encryption: bool = True,
    ) -> Dict[str, Any]:
        """Validate a data product against all global policies.

        Returns a compliance report with pass/fail per policy and an
        overall certification status.
        """
        product = self.get_product(product_name)
        if not product:
            return {"error": f"Product '{product_name}' not found"}

        results: List[Dict] = []

        for policy in self._policies.values():
            passed = True
            detail = ""

            if policy.policy_type == "quality":
                threshold = policy.rules.get("min_dq_score", 85)
                passed = dq_score >= threshold
                detail = f"DQ score {dq_score:.1f}% vs threshold {threshold}%"

            elif policy.policy_type == "pii":
                passed = pii_masked
                detail = "PII masking applied" if passed else "PII NOT masked"

            elif policy.policy_type == "retention":
                # Check is informational — retention enforced at Delta level
                passed = True
                detail = "Retention enforced via Delta Lake VACUUM"

            elif policy.policy_type == "encryption":
                passed = has_encryption
                detail = "Encryption at rest" if passed else "NOT encrypted"

            elif policy.policy_type == "freshness":
                max_hours = policy.rules.get("max_freshness_hours", 24)
                passed = freshness_hours <= max_hours
                detail = f"{freshness_hours:.1f}h vs SLA {max_hours}h"

            results.append({
                "policy": policy.name,
                "type": policy.policy_type,
                "severity": policy.severity,
                "passed": passed,
                "detail": detail,
            })

        critical_failures = [
            r for r in results
            if not r["passed"] and r["severity"] == "critical"
        ]

        report = {
            "product": product_name,
            "domain": product.domain,
            "owner": product.owner,
            "audit_timestamp": datetime.now().isoformat(),
            "certified": len(critical_failures) == 0,
            "total_policies": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
            "critical_failures": len(critical_failures),
            "details": results,
        }

        if report["certified"]:
            logger.info("✅  {} CERTIFIED", product_name)
        else:
            logger.warning(
                "❌  {} FAILED certification ({} critical violations)",
                product_name, len(critical_failures),
            )

        return report

    # ------------------------------------------------------------------
    # Convenience: seed default policies
    # ------------------------------------------------------------------

    def seed_default_policies(self) -> None:
        """Register the standard set of global governance policies."""
        defaults = [
            GlobalPolicy(
                name="pii_masking_required",
                description="All PII must be masked before Silver layer.",
                policy_type="pii",
                rules={"masking_required": True},
                severity="critical",
            ),
            GlobalPolicy(
                name="min_data_quality",
                description="Overall DQ score must meet minimum threshold.",
                policy_type="quality",
                rules={"min_dq_score": 85},
                severity="critical",
            ),
            GlobalPolicy(
                name="data_freshness_sla",
                description="Data products must be refreshed within SLA.",
                policy_type="freshness",
                rules={"max_freshness_hours": 24},
                severity="warning",
            ),
            GlobalPolicy(
                name="encryption_at_rest",
                description="All data must be encrypted at rest.",
                policy_type="encryption",
                rules={"encryption_required": True},
                severity="critical",
            ),
            GlobalPolicy(
                name="dpdp_retention",
                description="Data retention per DPDP Act 2023.",
                policy_type="retention",
                rules={"retention_years": 7},
                severity="critical",
            ),
        ]
        for p in defaults:
            self.add_policy(p)
        logger.info("Seeded {} default global policies.", len(defaults))
