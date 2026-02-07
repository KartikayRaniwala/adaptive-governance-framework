# ============================================================================
# Adaptive Data Governance Framework
# src/utils/schemas.py
# ============================================================================
# Centralised schema definitions for every layer of the data lakehouse.
#
# This module provides:
#   1. **Pydantic models** – used for row-level validation in Python-native
#      code (ingestion scripts, unit tests, API endpoints).
#   2. **PySpark StructType schemas** – used when reading / writing DataFrames
#      inside the Spark cluster (Bronze → Silver → Gold pipeline).
#   3. **get_schema()** – a convenience factory that returns the correct
#      PySpark StructType for a given lakehouse layer name.
#
# The field inventory is intentionally kept in sync with
# ``src/ingestion/data_collector.py`` so that generated synthetic data can be
# validated without schema drift.
# ============================================================================

from __future__ import annotations

import re
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================================
# 1.  Enumerations (shared by Pydantic & downstream code)
# ============================================================================

class ProductCategory(str, Enum):
    """Allowed product categories for e-commerce orders.

    Values are kept in sync with
    ``src.ingestion.data_collector.PRODUCT_CATEGORIES``.
    """

    ELECTRONICS = "Electronics"
    FASHION = "Fashion"
    HOME = "Home"
    BOOKS = "Books"
    BEAUTY = "Beauty"


class PaymentMethod(str, Enum):
    """Allowed payment methods.

    Values are kept in sync with
    ``src.ingestion.data_collector.PAYMENT_METHODS``.
    """

    UPI = "UPI"
    CARD = "Card"
    COD = "COD"
    WALLET = "Wallet"
    NETBANKING = "NetBanking"


# ============================================================================
# 2.  Pydantic Model – OrderSchema
# ============================================================================

# Pre-compiled regex for 6-digit Indian PIN codes
_PINCODE_RE = re.compile(r"^\d{6}$")


class OrderSchema(BaseModel):
    """Pydantic model representing a single e-commerce order row.

    This model is the **canonical row-level contract** used by ingestion
    scripts, data-quality checks, and API serialisation.  It mirrors the
    columns produced by
    :py:class:`~src.ingestion.data_collector.DataCollector`.

    Attributes
    ----------
    order_id : str
        A UUID-4 string that uniquely identifies the order.
    customer_id : str
        A UUID-4 string that uniquely identifies the customer.
    product_category : ProductCategory
        One of the allowed product categories (see :class:`ProductCategory`).
    order_value : float
        Monetary value of the order in INR.  Must be ≥ 0.
    delivery_instructions : str | None
        Free-text delivery notes.  May contain embedded PII in synthetic
        datasets.
    customer_review : str | None
        Free-text customer review.  May contain embedded PII in synthetic
        datasets.
    order_timestamp : datetime
        UTC timestamp when the order was placed.
    delivery_city : str
        Name of the Indian city for delivery.
    delivery_pincode : str
        6-digit Indian PIN code (validated via regex).
    payment_method : PaymentMethod
        One of the allowed payment methods (see :class:`PaymentMethod`).

    Examples
    --------
    >>> from datetime import datetime
    >>> order = OrderSchema(
    ...     order_id="550e8400-e29b-41d4-a716-446655440000",
    ...     customer_id="6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    ...     product_category="Electronics",
    ...     order_value=4999.99,
    ...     delivery_instructions="Leave at the door",
    ...     customer_review="Great product!",
    ...     order_timestamp=datetime(2025, 6, 15, 10, 30),
    ...     delivery_city="Mumbai",
    ...     delivery_pincode="400001",
    ...     payment_method="UPI",
    ... )
    >>> order.order_id
    '550e8400-e29b-41d4-a716-446655440000'
    """

    order_id: str = Field(
        ...,
        description="UUID-4 string uniquely identifying the order.",
    )
    customer_id: str = Field(
        ...,
        description="UUID-4 string uniquely identifying the customer.",
    )
    product_category: ProductCategory = Field(
        ...,
        description="Product category (must be one of the allowed values).",
    )
    order_value: float = Field(
        ...,
        ge=0,
        description="Order value in INR.  Must be non-negative.",
    )
    delivery_instructions: Optional[str] = Field(
        default=None,
        description="Free-text delivery notes (may contain PII).",
    )
    customer_review: Optional[str] = Field(
        default=None,
        description="Free-text customer review (may contain PII).",
    )
    order_timestamp: datetime = Field(
        ...,
        description="UTC timestamp when the order was placed.",
    )
    delivery_city: str = Field(
        ...,
        description="Indian city name for delivery.",
    )
    delivery_pincode: str = Field(
        ...,
        description="6-digit Indian PIN code.",
    )
    payment_method: PaymentMethod = Field(
        ...,
        description="Payment method used (must be one of the allowed values).",
    )

    # ---------------------------------------------------------------- validators

    @field_validator("order_id", "customer_id")
    @classmethod
    def _validate_uuid(cls, value: str) -> str:
        """Ensure the value is a valid UUID-4 string.

        Parameters
        ----------
        value : str
            The raw string to validate.

        Returns
        -------
        str
            The validated (lower-cased, hyphenated) UUID string.

        Raises
        ------
        ValueError
            If *value* cannot be parsed as a UUID.
        """
        try:
            parsed = uuid.UUID(value, version=4)
        except (ValueError, AttributeError) as exc:
            raise ValueError(
                f"'{value}' is not a valid UUID-4 string."
            ) from exc
        return str(parsed)

    @field_validator("delivery_pincode")
    @classmethod
    def _validate_pincode(cls, value: str) -> str:
        """Ensure the PIN code is exactly 6 digits.

        Parameters
        ----------
        value : str
            The raw pincode string.

        Returns
        -------
        str
            The validated 6-digit pincode string.

        Raises
        ------
        ValueError
            If *value* does not match the ``^\\d{6}$`` pattern.
        """
        if not _PINCODE_RE.match(value):
            raise ValueError(
                f"delivery_pincode must be exactly 6 digits, got '{value}'."
            )
        return value

    # ---------------------------------------------------------------- config

    class Config:
        """Pydantic model configuration."""

        str_strip_whitespace = True
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "order_id": "550e8400-e29b-41d4-a716-446655440000",
                "customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "product_category": "Electronics",
                "order_value": 4999.99,
                "delivery_instructions": "Leave at the door",
                "customer_review": "Great product!",
                "order_timestamp": "2025-06-15T10:30:00",
                "delivery_city": "Mumbai",
                "delivery_pincode": "400001",
                "payment_method": "UPI",
            }
        }


# ============================================================================
# 3.  PySpark StructType Schemas  (Bronze / Silver / Gold)
# ============================================================================

# ---------------------------------------------------------------------------
# 3a.  BRONZE_SCHEMA
# ---------------------------------------------------------------------------
# The Bronze layer is a raw, untransformed landing zone.  Every column is
# stored as a ``StringType`` except ``order_timestamp`` which is preserved as
# ``TimestampType`` so that downstream time-partitioning works out of the box.
# ---------------------------------------------------------------------------

BRONZE_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_category", StringType(), nullable=True),
        StructField("order_value", StringType(), nullable=True),
        StructField("delivery_instructions", StringType(), nullable=True),
        StructField("customer_review", StringType(), nullable=True),
        StructField("order_timestamp", TimestampType(), nullable=True),
        StructField("delivery_city", StringType(), nullable=True),
        StructField("delivery_pincode", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
    ]
)
"""PySpark schema for the **Bronze** (raw landing) layer.

All fields are ``StringType`` except ``order_timestamp`` which uses
``TimestampType``.  ``order_id`` is non-nullable because it serves as the
natural key for deduplication in downstream layers.

Notes
-----
*  No type casting is applied at this stage – values arrive exactly as the
   source system emits them.
*  Schema-on-read errors (e.g. malformed timestamps) surface during the
   Bronze → Silver promotion step.
"""  

# ---------------------------------------------------------------------------
# 3b.  SILVER_SCHEMA
# ---------------------------------------------------------------------------
# The Silver layer applies type casting, PII masking, and basic cleansing.
# Two *new* columns are introduced:
#   • ``delivery_instructions_masked`` – PII-redacted copy of the original.
#   • ``customer_review_masked``       – PII-redacted copy of the original.
# The original free-text columns are retained so that auditors can compare
# masked vs. raw values during governance reviews.
# ---------------------------------------------------------------------------

SILVER_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_category", StringType(), nullable=False),
        StructField("order_value", DoubleType(), nullable=False),
        StructField("delivery_instructions", StringType(), nullable=True),
        StructField("delivery_instructions_masked", StringType(), nullable=True),
        StructField("customer_review", StringType(), nullable=True),
        StructField("customer_review_masked", StringType(), nullable=True),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("delivery_city", StringType(), nullable=False),
        StructField("delivery_pincode", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
    ]
)
"""PySpark schema for the **Silver** (cleansed & masked) layer.

Key differences from Bronze:

* ``order_value`` is cast to ``DoubleType``.
* ``order_id``, ``customer_id``, ``product_category``, ``order_timestamp``,
  ``delivery_city``, ``delivery_pincode``, and ``payment_method`` are all
  **non-nullable** after cleansing (rows that fail validation are quarantined).
* Two additional columns carry PII-masked versions of the free-text fields:
  ``delivery_instructions_masked`` and ``customer_review_masked``.
"""

# ---------------------------------------------------------------------------
# 3c.  GOLD_CUSTOMER_SCHEMA
# ---------------------------------------------------------------------------
# The Gold layer contains business-level aggregates.  This particular schema
# represents the **Customer 360** table used by the adaptive governance
# scoring engine and downstream BI dashboards.
# ---------------------------------------------------------------------------

GOLD_CUSTOMER_SCHEMA: StructType = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("total_orders", LongType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=False),
        StructField("avg_order_value", DoubleType(), nullable=False),
        StructField("clv_proxy", DoubleType(), nullable=False),
        StructField("customer_segment", StringType(), nullable=False),
    ]
)
"""PySpark schema for the **Gold** customer-360 aggregation table.

Fields
------
customer_id : str
    Unique customer identifier (non-nullable).
total_orders : long
    Lifetime count of orders placed by this customer.
total_revenue : double
    Lifetime sum of ``order_value`` for this customer.
avg_order_value : double
    ``total_revenue / total_orders``.
clv_proxy : double
    A simple Customer Lifetime Value proxy computed during the Gold
    aggregation step (e.g. ``avg_order_value × order_frequency_factor``).
customer_segment : str
    Human-readable segment label assigned by the governance scoring engine
    (e.g. ``"High-Value``, ``"Medium-Value``, ``"Low-Value"``).
"""


# ============================================================================
# 4.  Schema Registry & Factory
# ============================================================================

# Mapping of canonical layer names to their PySpark StructType definitions.
_SCHEMA_REGISTRY: Dict[str, StructType] = {
    "bronze": BRONZE_SCHEMA,
    "silver": SILVER_SCHEMA,
    "gold_customer": GOLD_CUSTOMER_SCHEMA,
}


def get_schema(
    layer: str,
) -> StructType:
    """Return the PySpark ``StructType`` for the requested lakehouse layer.

    This is a thin convenience wrapper around the module-level schema
    constants.  It normalises the *layer* argument to lower-case and looks
    it up in the internal registry.

    Parameters
    ----------
    layer : str
        The name of the lakehouse layer.  Recognised values (case-insensitive):

        * ``"bronze"``        – raw landing zone schema.
        * ``"silver"``        – cleansed & PII-masked schema.
        * ``"gold_customer"`` – customer-360 aggregation schema.

    Returns
    -------
    pyspark.sql.types.StructType
        The matching PySpark schema.

    Raises
    ------
    ValueError
        If *layer* is not found in the registry.  The error message lists
        all valid layer names to aid debugging.

    Examples
    --------
    >>> from src.utils.schemas import get_schema
    >>> bronze = get_schema("bronze")
    >>> [f.name for f in bronze.fields]
    ['order_id', 'customer_id', 'product_category', 'order_value',
     'delivery_instructions', 'customer_review', 'order_timestamp',
     'delivery_city', 'delivery_pincode', 'payment_method']

    >>> silver = get_schema("Silver")   # case-insensitive
    >>> "delivery_instructions_masked" in [f.name for f in silver.fields]
    True

    >>> get_schema("platinum")
    Traceback (most recent call last):
        ...
    ValueError: Unknown layer 'platinum'. Valid layers: bronze, silver, gold_customer
    """
    key = layer.strip().lower()
    if key not in _SCHEMA_REGISTRY:
        valid = ", ".join(sorted(_SCHEMA_REGISTRY.keys()))
        raise ValueError(
            f"Unknown layer '{layer}'. Valid layers: {valid}"
        )
    return _SCHEMA_REGISTRY[key]


# ============================================================================
# 5.  Module-level convenience exports
# ============================================================================

__all__ = [
    # Enums
    "ProductCategory",
    "PaymentMethod",
    # Pydantic
    "OrderSchema",
    # PySpark schemas
    "BRONZE_SCHEMA",
    "SILVER_SCHEMA",
    "GOLD_CUSTOMER_SCHEMA",
    # Factory
    "get_schema",
]