# ============================================================================
# Adaptive Data Governance Framework
# src/governance/identity_resolution.py
# ============================================================================
# Master Data Management with fuzzy matching for entity resolution.
# Resolves duplicate customer identities across disparate data sources
# (orders, reviews, support tickets) into Golden Records.
#
# Techniques:
#   - Exact-match blocking (email, phone)
#   - Levenshtein / Jaro-Winkler fuzzy name matching
#   - Soundex phonetic matching for Indian names
#   - Probabilistic record linkage with configurable thresholds
# ============================================================================

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional, Tuple

from loguru import logger
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType


# ---------------------------------------------------------------------------
# Soundex UDF for phonetic matching of Indian names
# ---------------------------------------------------------------------------

def _soundex_indian(name: str) -> str:
    """American Soundex adapted for Indian name transliterations.

    Keeps first letter, maps consonants to digits, drops vowels/H/W/Y,
    pads/truncates to 4 chars.
    """
    if not name or not isinstance(name, str):
        return ""
    name = name.upper().strip()
    if not name:
        return ""

    _MAP = {
        "B": "1", "F": "1", "P": "1", "V": "1",
        "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2",
        "S": "2", "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
    }

    code = name[0]
    prev_digit = _MAP.get(name[0], "0")
    for ch in name[1:]:
        digit = _MAP.get(ch, "0")
        if digit != "0" and digit != prev_digit:
            code += digit
        prev_digit = digit if digit != "0" else prev_digit
    return (code + "000")[:4]


def _jaro_winkler(s1: str, s2: str) -> float:
    """Jaro-Winkler similarity (0.0–1.0) — efficient pure-Python impl."""
    if not s1 or not s2:
        return 0.0
    if s1 == s2:
        return 1.0

    s1 = s1.upper()
    s2 = s2.upper()
    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2

    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len1
        + matches / len2
        + (matches - transpositions / 2) / matches
    ) / 3.0

    # Winkler prefix bonus (up to 4 chars)
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * 0.1 * (1 - jaro)


# ============================================================================
# IdentityResolver
# ============================================================================

class IdentityResolver:
    """Resolve duplicate customer identities into Golden Records.

    Parameters
    ----------
    spark : SparkSession
    match_threshold : float
        Minimum composite similarity score (0.0–1.0) to consider two
        records as the same entity.  Default 0.80.
    """

    def __init__(
        self,
        spark: SparkSession,
        match_threshold: float = 0.80,
    ):
        self.spark = spark
        self.match_threshold = match_threshold
        self._register_udfs()

    def _register_udfs(self) -> None:
        """Register Spark UDFs for fuzzy matching."""
        from pyspark.sql.functions import udf

        @udf(returnType=StringType())
        def soundex_udf(name: str) -> str:
            return _soundex_indian(name)

        @udf(returnType=DoubleType())
        def jaro_winkler_udf(s1: str, s2: str) -> float:
            return _jaro_winkler(s1, s2)

        self._soundex_udf = soundex_udf
        self._jaro_winkler_udf = jaro_winkler_udf

    # ------------------------------------------------------------------
    # Blocking — reduce candidate pairs
    # ------------------------------------------------------------------

    def _blocking_keys(self, df: DataFrame) -> DataFrame:
        """Add blocking keys to reduce the comparison space.

        Blocking strategies:
        1. Normalised email domain
        2. Phone last-6 digits
        3. Soundex of first_name + last_name
        4. City + pincode first 3 digits
        """
        return (
            df
            .withColumn(
                "_block_email",
                F.lower(F.trim(F.col("email"))),
            )
            .withColumn(
                "_block_phone_tail",
                F.regexp_extract(
                    F.regexp_replace(F.col("phone"), r"[\s\-\+]", ""),
                    r"(\d{6})$", 1,
                ),
            )
            .withColumn(
                "_block_soundex",
                self._soundex_udf(
                    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
                ),
            )
            .withColumn(
                "_block_geo",
                F.concat_ws(
                    "_",
                    F.lower(F.col("city")),
                    F.substring(F.col("pincode"), 1, 3),
                ),
            )
        )

    # ------------------------------------------------------------------
    # Exact-match dedup (fast pass)
    # ------------------------------------------------------------------

    def exact_match_dedup(
        self,
        df: DataFrame,
        match_columns: List[str],
        id_column: str = "customer_id",
    ) -> DataFrame:
        """Deterministic dedup on exact column values.

        Groups records with identical values in *match_columns* and
        assigns a single ``_golden_id`` to each group.

        Returns the original DataFrame with a ``_golden_id`` column.
        """
        logger.info(
            "Exact-match dedup on columns={}", match_columns,
        )
        window = Window.partitionBy(*match_columns).orderBy(F.col(id_column))
        golden = (
            df
            .withColumn(
                "_golden_id",
                F.first(F.col(id_column)).over(window),
            )
        )
        n_original = df.select(id_column).distinct().count()
        n_golden = golden.select("_golden_id").distinct().count()
        logger.info(
            "Exact dedup: {} identities → {} golden records (saved {})",
            n_original, n_golden, n_original - n_golden,
        )
        return golden

    # ------------------------------------------------------------------
    # Fuzzy-match linkage (probabilistic)
    # ------------------------------------------------------------------

    def fuzzy_match_link(
        self,
        df: DataFrame,
        name_col_first: str = "first_name",
        name_col_last: str = "last_name",
        email_col: str = "email",
        phone_col: str = "phone",
        id_column: str = "customer_id",
    ) -> DataFrame:
        """Probabilistic record linkage using Jaro-Winkler + blocking.

        Steps
        -----
        1. Add blocking keys (Soundex, phone tail, geo).
        2. Self-join within each block.
        3. Score pairs with weighted Jaro-Winkler similarity.
        4. Accept pairs above ``match_threshold``.
        5. Assign ``_golden_id`` via connected-component resolution.

        Returns the original DataFrame with ``_golden_id``.
        """
        logger.info(
            "Fuzzy linkage — threshold={}", self.match_threshold,
        )
        blocked = self._blocking_keys(df)

        # Self-join on Soundex block (most discriminating)
        left = blocked.alias("L")
        right = blocked.alias("R")

        candidates = left.join(
            right,
            (F.col("L._block_soundex") == F.col("R._block_soundex"))
            & (F.col(f"L.{id_column}") < F.col(f"R.{id_column}")),
        )

        # Score each candidate pair
        scored = (
            candidates
            .withColumn(
                "_name_sim",
                self._jaro_winkler_udf(
                    F.concat_ws(" ", F.col(f"L.{name_col_first}"), F.col(f"L.{name_col_last}")),
                    F.concat_ws(" ", F.col(f"R.{name_col_first}"), F.col(f"R.{name_col_last}")),
                ),
            )
            .withColumn(
                "_email_match",
                F.when(
                    F.col("L._block_email") == F.col("R._block_email"), 1.0
                ).otherwise(0.0),
            )
            .withColumn(
                "_phone_match",
                F.when(
                    (F.col("L._block_phone_tail") == F.col("R._block_phone_tail"))
                    & (F.col("L._block_phone_tail") != ""),
                    1.0,
                ).otherwise(0.0),
            )
            .withColumn(
                "_composite_score",
                F.col("_name_sim") * 0.40
                + F.col("_email_match") * 0.35
                + F.col("_phone_match") * 0.25,
            )
            .filter(F.col("_composite_score") >= self.match_threshold)
            .select(
                F.col(f"L.{id_column}").alias("id_left"),
                F.col(f"R.{id_column}").alias("id_right"),
                F.col("_composite_score"),
            )
        )

        # Connected-component resolution via iterative self-join
        golden_map = self._resolve_components(scored, id_column, df)
        return golden_map

    # ------------------------------------------------------------------
    # Connected-component resolution
    # ------------------------------------------------------------------

    def _resolve_components(
        self,
        pairs: DataFrame,
        id_column: str,
        original: DataFrame,
    ) -> DataFrame:
        """Resolve transitive matches into clusters and assign
        ``_golden_id`` = minimum member ID in each cluster.

        Uses a simple iterative approach (max 10 rounds).
        """
        # Build initial mapping: each ID → itself
        all_ids = original.select(
            F.col(id_column).alias("member_id"),
        ).distinct().withColumn("_golden_id", F.col("member_id"))

        # Edge list
        edges = (
            pairs
            .select(
                F.col("id_left").alias("member_id"),
                F.col("id_right").alias("_linked"),
            )
            .unionByName(
                pairs.select(
                    F.col("id_right").alias("member_id"),
                    F.col("id_left").alias("_linked"),
                )
            )
        )

        # Iteratively propagate the smallest ID through edges
        for i in range(10):
            # Join edges with current golden map
            new_map = (
                edges
                .join(
                    all_ids.withColumnRenamed("_golden_id", "_golden_linked"),
                    edges["_linked"] == all_ids["member_id"],
                    "left",
                )
                .select(
                    edges["member_id"],
                    F.least(
                        all_ids["_golden_id"],
                        F.col("_golden_linked"),
                    ).alias("_golden_id"),
                )
                .groupBy("member_id")
                .agg(F.min("_golden_id").alias("_golden_id"))
            )

            # Merge with unmapped IDs
            updated = (
                all_ids.alias("old")
                .join(new_map.alias("new"), "member_id", "left")
                .select(
                    F.col("member_id"),
                    F.least(
                        F.col("old._golden_id"),
                        F.coalesce(F.col("new._golden_id"), F.col("old._golden_id")),
                    ).alias("_golden_id"),
                )
            )

            if updated.exceptAll(all_ids).count() == 0:
                break
            all_ids = updated

        # Join back to original
        result = original.join(
            all_ids,
            original[id_column] == all_ids["member_id"],
            "left",
        ).drop("member_id")

        n_original = original.select(id_column).distinct().count()
        n_golden = result.select("_golden_id").distinct().count()
        logger.info(
            "Fuzzy linkage complete: {} identities → {} golden records",
            n_original, n_golden,
        )
        return result

    # ------------------------------------------------------------------
    # Golden Record creation
    # ------------------------------------------------------------------

    def create_golden_records(
        self,
        resolved_df: DataFrame,
        id_column: str = "customer_id",
        recency_col: str = "registration_date",
    ) -> DataFrame:
        """Create a single Golden Record per ``_golden_id``.

        Selection strategy:
        - For each cluster, pick the most recent (recency_col) record as
          the canonical profile.
        - Merge consent flags (logical OR — if any record has consent,
          the golden record inherits it).
        """
        logger.info("Creating golden records …")

        window = Window.partitionBy("_golden_id").orderBy(
            F.col(recency_col).desc()
        )
        ranked = resolved_df.withColumn("_rn", F.row_number().over(window))

        # Canonical profile = most recent
        golden = ranked.filter(F.col("_rn") == 1).drop("_rn")

        # Merge consent flags across cluster
        consent_cols = [c for c in resolved_df.columns if c.startswith("consent_")]
        if consent_cols:
            consent_agg = resolved_df.groupBy("_golden_id").agg(
                *[
                    F.max(F.col(c).cast("int")).cast("boolean").alias(f"_{c}_merged")
                    for c in consent_cols
                ]
            )
            golden = golden.join(consent_agg, "_golden_id", "left")
            for c in consent_cols:
                golden = golden.withColumn(
                    c, F.coalesce(F.col(f"_{c}_merged"), F.col(c))
                ).drop(f"_{c}_merged")

        logger.info(
            "Golden records created — {} unique entities",
            golden.count(),
        )
        return golden
