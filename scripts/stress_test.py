# ============================================================================
# Adaptive Data Governance Framework
# scripts/stress_test.py
# ============================================================================
# Generates "poisoned" datasets to validate every quarantine / alerting /
# masking mechanism in the framework.  Configurable via YAML profiles so
# the dissertation can demonstrate different failure-injection scenarios.
#
# Poison profiles:
#   1. pii_leak        – Credit-card numbers & Aadhaar in free-text fields
#   2. null_spike       – 80 % null burst across critical columns
#   3. schema_break     – Extra / missing / wrong-type columns
#   4. late_data        – Out-of-order timestamps (hours to days late)
#   5. duplicate_flood  – N-fold exact duplicates
#   6. format_corrupt   – Encoding / delimiter / truncation corruption
#   7. value_anomaly    – Extreme outliers (negative prices, ₹999999 orders)
#   8. mixed            – Random mix of all the above
# ============================================================================

from __future__ import annotations

import copy
import json
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

# ---------------------------------------------------------------------------
# Default poison profile
# ---------------------------------------------------------------------------
_DEFAULT_PROFILE: Dict[str, Any] = {
    "name": "mixed",
    "total_records": 10_000,
    "output_format": "json",              # json | csv | parquet
    "output_dir": "data/stress_test",
    "seed": 42,
    "poisons": {
        "pii_leak":       {"enabled": True,  "pct": 0.08},
        "null_spike":     {"enabled": True,  "pct": 0.15, "columns": ["email", "phone", "order_value"]},
        "schema_break":   {"enabled": True,  "pct": 0.05},
        "late_data":      {"enabled": True,  "pct": 0.10, "max_delay_hours": 72},
        "duplicate_flood": {"enabled": True, "pct": 0.10, "max_copies": 5},
        "format_corrupt": {"enabled": True,  "pct": 0.05},
        "value_anomaly":  {"enabled": True,  "pct": 0.07},
    },
}


# ============================================================================
# Poison generators
# ============================================================================

def _inject_pii_leak(record: Dict, _cfg: Dict) -> Dict:
    """Inject PII into a free-text field (delivery_notes or search_query)."""
    from faker import Faker
    fake = Faker("en_IN")
    target_field = random.choice(["delivery_notes", "search_query", "comments"])
    pii_type = random.choice(["credit_card", "aadhaar", "pan", "email", "ifsc"])

    if pii_type == "credit_card":
        cc = fake.credit_card_number()
        record[target_field] = f"Please charge card {cc}"
    elif pii_type == "aadhaar":
        aadhaar = f"{random.randint(2000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}"
        record[target_field] = f"Aadhaar: {aadhaar}"
    elif pii_type == "pan":
        pan = (
            random.choice(string.ascii_uppercase)
            + random.choice(string.ascii_uppercase)
            + random.choice(string.ascii_uppercase)
            + random.choice("PCHABGJLFT")
            + random.choice(string.ascii_uppercase)
            + str(random.randint(1000, 9999))
            + random.choice(string.ascii_uppercase)
        )
        record[target_field] = f"PAN is {pan}"
    elif pii_type == "email":
        record[target_field] = f"Contact me at {fake.email()}"
    else:
        ifsc = "SBIN0" + "".join(random.choices(string.digits, k=6))
        record[target_field] = f"Account IFSC {ifsc}"

    record["_poison"] = "pii_leak"
    return record


def _inject_null_spike(record: Dict, cfg: Dict) -> Dict:
    """Set multiple columns to None / empty."""
    cols = cfg.get("columns", ["email", "phone", "order_value"])
    for c in cols:
        if c in record:
            record[c] = None
    record["_poison"] = "null_spike"
    return record


def _inject_schema_break(record: Dict, _cfg: Dict) -> Dict:
    """Add extra columns, remove expected columns, or change value types."""
    mutation = random.choice(["extra_col", "missing_col", "type_change"])
    if mutation == "extra_col":
        record["__unexpected_col_" + uuid.uuid4().hex[:6]] = "surprise"
    elif mutation == "missing_col":
        removable = [k for k in record if not k.startswith("_")]
        if removable:
            del record[random.choice(removable)]
    else:  # type_change
        if "order_value" in record:
            record["order_value"] = "not_a_number"
        elif "quantity" in record:
            record["quantity"] = "NaN"
    record["_poison"] = "schema_break"
    return record


def _inject_late_data(record: Dict, cfg: Dict) -> Dict:
    """Push the timestamp backward by hours to days."""
    max_delay = cfg.get("max_delay_hours", 72)
    delay = timedelta(hours=random.uniform(2, max_delay))
    try:
        ts = datetime.fromisoformat(record.get("order_timestamp", datetime.utcnow().isoformat()))
    except (ValueError, TypeError):
        ts = datetime.utcnow()
    record["order_timestamp"] = (ts - delay).isoformat()
    record["_poison"] = "late_data"
    return record


def _inject_duplicates(record: Dict, cfg: Dict) -> List[Dict]:
    """Return 1-N exact copies (including the original)."""
    copies = random.randint(2, cfg.get("max_copies", 5))
    record["_poison"] = "duplicate"
    return [copy.deepcopy(record) for _ in range(copies)]


def _inject_format_corrupt(record: Dict, _cfg: Dict) -> Dict:
    """Corrupt string fields with encoding issues / truncation."""
    corrupt_type = random.choice(["encoding", "truncation", "delimiter"])
    text_fields = [k for k, v in record.items() if isinstance(v, str) and not k.startswith("_")]
    if not text_fields:
        record["_poison"] = "format_corrupt"
        return record
    field = random.choice(text_fields)
    val = record[field]
    if corrupt_type == "encoding":
        record[field] = val.encode("utf-8", errors="replace").decode("latin-1")
    elif corrupt_type == "truncation":
        record[field] = val[: max(1, len(val) // 3)]
    else:  # delimiter
        record[field] = val.replace(",", "\t").replace("|", ",")
    record["_poison"] = "format_corrupt"
    return record


def _inject_value_anomaly(record: Dict, _cfg: Dict) -> Dict:
    """Inject extreme numeric outliers."""
    anomaly = random.choice(["negative_price", "huge_value", "zero_qty"])
    if anomaly == "negative_price" and "order_value" in record:
        record["order_value"] = -abs(random.uniform(100, 50000))
    elif anomaly == "huge_value" and "order_value" in record:
        record["order_value"] = random.uniform(5_000_000, 99_999_999)
    elif "quantity" in record:
        record["quantity"] = 0
    record["_poison"] = "value_anomaly"
    return record


_POISON_FNS = {
    "pii_leak": _inject_pii_leak,
    "null_spike": _inject_null_spike,
    "schema_break": _inject_schema_break,
    "late_data": _inject_late_data,
    "format_corrupt": _inject_format_corrupt,
    "value_anomaly": _inject_value_anomaly,
    # duplicate_flood is handled separately (returns list)
}


# ============================================================================
# Base record generator
# ============================================================================

def _generate_clean_record() -> Dict:
    """Generate a single clean e-commerce order record."""
    from faker import Faker
    fake = Faker("en_IN")
    return {
        "order_id": uuid.uuid4().hex,
        "customer_id": uuid.uuid4().hex[:16],
        "customer_name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "product_id": f"PROD-{random.randint(10000, 99999)}",
        "product_name": fake.word().capitalize() + " " + random.choice(["Phone", "Laptop", "Shoe", "Bag", "Tablet"]),
        "quantity": random.randint(1, 10),
        "order_value": round(random.uniform(99, 50000), 2),
        "order_status": random.choice(["placed", "shipped", "delivered", "returned", "cancelled"]),
        "payment_method": random.choice(["UPI", "credit_card", "debit_card", "COD", "wallet"]),
        "order_timestamp": (
            datetime.utcnow() - timedelta(hours=random.uniform(0, 24))
        ).isoformat(),
        "delivery_city": fake.city(),
        "delivery_pincode": str(random.randint(100000, 999999)),
        "delivery_notes": "",
        "search_query": "",
        "comments": "",
    }


# ============================================================================
# Main generator
# ============================================================================

class StressTestGenerator:
    """Generate poisoned datasets based on a YAML profile.

    Parameters
    ----------
    profile_path : str or None
        Path to a YAML profile.  If ``None``, uses the default mixed profile.
    """

    def __init__(self, profile_path: Optional[str] = None):
        if profile_path and os.path.exists(profile_path):
            with open(profile_path) as f:
                self.profile = yaml.safe_load(f)
            logger.info("Loaded stress-test profile from {}", profile_path)
        else:
            self.profile = copy.deepcopy(_DEFAULT_PROFILE)
            logger.info("Using default mixed profile")

        self.seed = self.profile.get("seed", 42)
        random.seed(self.seed)

    # ------------------------------------------------------------------ #
    # Generate
    # ------------------------------------------------------------------ #
    def generate(self) -> List[Dict]:
        """Generate the full poisoned dataset."""
        total = self.profile.get("total_records", 10_000)
        poisons = self.profile.get("poisons", {})

        # Decide which poisons are active
        active_poisons: List[tuple] = []  # (name, pct, cfg)
        for name, cfg in poisons.items():
            if cfg.get("enabled", False):
                active_poisons.append((name, cfg.get("pct", 0.05), cfg))

        # Pre-compute how many records get each poison
        poison_assignments: List[Optional[tuple]] = [None] * total
        for name, pct, cfg in active_poisons:
            n_affected = int(total * pct)
            indices = random.sample(range(total), min(n_affected, total))
            for idx in indices:
                if poison_assignments[idx] is None:
                    poison_assignments[idx] = (name, cfg)

        # Generate
        records: List[Dict] = []
        stats = {"clean": 0}
        for i in range(total):
            rec = _generate_clean_record()
            assignment = poison_assignments[i]
            if assignment is None:
                rec["_poison"] = "clean"
                records.append(rec)
                stats["clean"] = stats.get("clean", 0) + 1
            else:
                name, cfg = assignment
                if name == "duplicate_flood":
                    dupes = _inject_duplicates(rec, cfg)
                    records.extend(dupes)
                    stats[name] = stats.get(name, 0) + len(dupes)
                elif name in _POISON_FNS:
                    poisoned = _POISON_FNS[name](rec, cfg)
                    records.append(poisoned)
                    stats[name] = stats.get(name, 0) + 1
                else:
                    rec["_poison"] = "clean"
                    records.append(rec)
                    stats["clean"] += 1

        logger.info("Generated {} records (target {}). Breakdown: {}", len(records), total, stats)
        return records

    # ------------------------------------------------------------------ #
    # Write
    # ------------------------------------------------------------------ #
    def write(self, records: Optional[List[Dict]] = None) -> Path:
        """Generate (if needed) and write the poisoned dataset to disk.

        Returns the path of the written file.
        """
        if records is None:
            records = self.generate()

        out_dir = Path(self.profile.get("output_dir", "data/stress_test"))
        out_dir.mkdir(parents=True, exist_ok=True)
        fmt = self.profile.get("output_format", "json")
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        name = self.profile.get("name", "stress")

        if fmt == "json":
            path = out_dir / f"{name}_{ts}.json"
            with open(path, "w") as f:
                for rec in records:
                    f.write(json.dumps(rec, default=str) + "\n")
        elif fmt == "csv":
            import csv
            path = out_dir / f"{name}_{ts}.csv"
            if records:
                keys = records[0].keys()
                with open(path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(records)
        else:
            # Fall back to JSON-lines for unknown formats
            path = out_dir / f"{name}_{ts}.json"
            with open(path, "w") as f:
                for rec in records:
                    f.write(json.dumps(rec, default=str) + "\n")

        logger.info("Stress-test dataset written → {} ({} records)", path, len(records))
        return path

    # ------------------------------------------------------------------ #
    # Summary report
    # ------------------------------------------------------------------ #
    def summarise(self, records: List[Dict]) -> Dict:
        """Return a summary of poison distribution in the dataset."""
        from collections import Counter
        counter = Counter(r.get("_poison", "unknown") for r in records)
        return {
            "total_records": len(records),
            "breakdown": dict(counter),
            "poison_pct": round(
                (len(records) - counter.get("clean", 0)) / max(len(records), 1) * 100, 2,
            ),
        }


# ============================================================================
# Pre-built profiles
# ============================================================================

def create_profile_templates(output_dir: str = "config/stress_test") -> None:
    """Write example YAML profiles for common scenarios."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    profiles = {
        "pii_heavy": {
            "name": "pii_heavy",
            "total_records": 5000,
            "output_format": "json",
            "output_dir": "data/stress_test",
            "seed": 101,
            "poisons": {
                "pii_leak": {"enabled": True, "pct": 0.25},
                "null_spike": {"enabled": False, "pct": 0},
                "schema_break": {"enabled": False, "pct": 0},
                "late_data": {"enabled": False, "pct": 0},
                "duplicate_flood": {"enabled": False, "pct": 0},
                "format_corrupt": {"enabled": False, "pct": 0},
                "value_anomaly": {"enabled": False, "pct": 0},
            },
        },
        "null_burst": {
            "name": "null_burst",
            "total_records": 5000,
            "output_format": "json",
            "output_dir": "data/stress_test",
            "seed": 202,
            "poisons": {
                "pii_leak": {"enabled": False, "pct": 0},
                "null_spike": {
                    "enabled": True, "pct": 0.80,
                    "columns": ["email", "phone", "order_value", "customer_name", "delivery_city"],
                },
                "schema_break": {"enabled": False, "pct": 0},
                "late_data": {"enabled": False, "pct": 0},
                "duplicate_flood": {"enabled": False, "pct": 0},
                "format_corrupt": {"enabled": False, "pct": 0},
                "value_anomaly": {"enabled": False, "pct": 0},
            },
        },
        "schema_chaos": {
            "name": "schema_chaos",
            "total_records": 5000,
            "output_format": "json",
            "output_dir": "data/stress_test",
            "seed": 303,
            "poisons": {
                "pii_leak": {"enabled": False, "pct": 0},
                "null_spike": {"enabled": False, "pct": 0},
                "schema_break": {"enabled": True, "pct": 0.30},
                "late_data": {"enabled": False, "pct": 0},
                "duplicate_flood": {"enabled": False, "pct": 0},
                "format_corrupt": {"enabled": True, "pct": 0.15},
                "value_anomaly": {"enabled": False, "pct": 0},
            },
        },
        "full_chaos": {
            "name": "full_chaos",
            "total_records": 50000,
            "output_format": "json",
            "output_dir": "data/stress_test",
            "seed": 999,
            "poisons": {
                "pii_leak": {"enabled": True, "pct": 0.12},
                "null_spike": {"enabled": True, "pct": 0.20, "columns": ["email", "phone", "order_value"]},
                "schema_break": {"enabled": True, "pct": 0.10},
                "late_data": {"enabled": True, "pct": 0.15, "max_delay_hours": 120},
                "duplicate_flood": {"enabled": True, "pct": 0.15, "max_copies": 8},
                "format_corrupt": {"enabled": True, "pct": 0.08},
                "value_anomaly": {"enabled": True, "pct": 0.10},
            },
        },
    }

    for name, profile in profiles.items():
        path = out / f"{name}.yaml"
        with open(path, "w") as f:
            yaml.dump(profile, f, default_flow_style=False, sort_keys=False)
        logger.info("Profile template written → {}", path)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Stress-test data generator for the Adaptive Governance Framework",
    )
    sub = parser.add_subparsers(dest="command")

    # generate
    gen = sub.add_parser("generate", help="Generate a poisoned dataset")
    gen.add_argument("--profile", type=str, default=None, help="Path to YAML profile")
    gen.add_argument("--records", type=int, default=None)
    gen.add_argument("--format", choices=["json", "csv"], default=None)

    # templates
    sub.add_parser("templates", help="Write example profile templates")

    args = parser.parse_args()

    if args.command == "templates":
        create_profile_templates()
    elif args.command == "generate":
        gen = StressTestGenerator(profile_path=args.profile)
        if args.records:
            gen.profile["total_records"] = args.records
        if args.format:
            gen.profile["output_format"] = args.format
        recs = gen.generate()
        gen.write(recs)
        summary = gen.summarise(recs)
        logger.info("Summary: {}", json.dumps(summary, indent=2))
    else:
        parser.print_help()
