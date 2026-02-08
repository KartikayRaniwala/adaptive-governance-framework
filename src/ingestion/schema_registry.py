# ============================================================================
# Adaptive Data Governance Framework
# src/ingestion/schema_registry.py
# ============================================================================
# Centralised schema registry for the data lakehouse.
# Stores, versions, and compares PySpark StructType schemas across
# Bronze / Silver / Gold layers.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger
from pyspark.sql.types import StructField, StructType


class SchemaRegistry:
    """Manages schema versions for every table in the lakehouse.

    Parameters
    ----------
    registry_dir : str
        Directory where schema JSON files are stored.
    """

    def __init__(self, registry_dir: str = "config/schemas"):
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Schema serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _schema_to_dict(schema: StructType) -> List[Dict]:
        return [
            {
                "name": f.name,
                "type": str(f.dataType),
                "nullable": f.nullable,
            }
            for f in schema.fields
        ]

    @staticmethod
    def _dict_to_schema(fields: List[Dict]) -> StructType:
        from pyspark.sql.types import _parse_datatype_string  # noqa: internal helper

        return StructType(
            [
                StructField(
                    f["name"],
                    _parse_datatype_string(f["type"]),
                    f.get("nullable", True),
                )
                for f in fields
            ]
        )

    # ------------------------------------------------------------------
    # Register / retrieve
    # ------------------------------------------------------------------

    def register_schema(
        self,
        table_name: str,
        layer: str,
        schema: StructType,
        description: str = "",
    ) -> Path:
        """Persist a schema version to disk.

        Parameters
        ----------
        table_name : str
            Logical table name (e.g. ``"orders"``).
        layer : str
            Lakehouse layer (``"bronze"``, ``"silver"``, ``"gold"``).
        schema : StructType
            PySpark schema to register.
        description : str
            Human-readable description.

        Returns
        -------
        Path
            File path of the saved schema.
        """
        entry = {
            "table": table_name,
            "layer": layer,
            "version": datetime.now().isoformat(),
            "description": description,
            "fields": self._schema_to_dict(schema),
        }

        file_path = self.registry_dir / f"{layer}_{table_name}.json"
        with open(file_path, "w") as f:
            json.dump(entry, f, indent=2)

        logger.info(
            "Schema registered — {layer}.{table} ({n} fields)",
            layer=layer, table=table_name, n=len(schema.fields),
        )
        return file_path

    def get_schema(self, table_name: str, layer: str) -> Optional[StructType]:
        """Load a previously registered schema.

        Returns ``None`` if no schema file exists.
        """
        file_path = self.registry_dir / f"{layer}_{table_name}.json"
        if not file_path.exists():
            logger.warning("No schema found for {l}.{t}", l=layer, t=table_name)
            return None

        with open(file_path) as f:
            entry = json.load(f)

        return self._dict_to_schema(entry["fields"])

    # ------------------------------------------------------------------
    # Drift comparison
    # ------------------------------------------------------------------

    def compare_schemas(
        self,
        table_name: str,
        layer: str,
        incoming_schema: StructType,
    ) -> Dict:
        """Compare *incoming_schema* against the registered version.

        Returns
        -------
        dict
            ``has_drift``, ``added``, ``removed``, ``type_changes``.
        """
        registered = self.get_schema(table_name, layer)
        if registered is None:
            return {
                "has_drift": False,
                "added": [],
                "removed": [],
                "type_changes": [],
                "message": "No prior schema — first registration.",
            }

        reg_map = {f.name: str(f.dataType) for f in registered.fields}
        inc_map = {f.name: str(f.dataType) for f in incoming_schema.fields}

        added = [c for c in inc_map if c not in reg_map]
        removed = [c for c in reg_map if c not in inc_map]
        type_changes = [
            {"column": c, "old": reg_map[c], "new": inc_map[c]}
            for c in inc_map
            if c in reg_map and inc_map[c] != reg_map[c]
        ]

        has_drift = bool(added or removed or type_changes)
        if has_drift:
            logger.warning(
                "Schema drift for {l}.{t}: +{a}, -{r}, Δ{c}",
                l=layer, t=table_name,
                a=len(added), r=len(removed), c=len(type_changes),
            )
        return {
            "has_drift": has_drift,
            "added": added,
            "removed": removed,
            "type_changes": type_changes,
        }

    # ------------------------------------------------------------------
    # List all registered schemas
    # ------------------------------------------------------------------

    def list_schemas(self) -> List[Dict]:
        """Return metadata for every registered schema."""
        schemas = []
        for file_path in sorted(self.registry_dir.glob("*.json")):
            with open(file_path) as f:
                entry = json.load(f)
            schemas.append({
                "table": entry["table"],
                "layer": entry["layer"],
                "version": entry["version"],
                "field_count": len(entry["fields"]),
                "path": str(file_path),
            })
        return schemas
