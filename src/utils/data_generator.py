# ============================================================================
# Adaptive Data Governance Framework
# src/utils/data_generator.py
# ============================================================================
# Generates large-scale synthetic e-commerce data using chunked Pandas.
# Designed for millions of rows without OOM.
#
# Real-world scenarios injected:
#   - Seasonal purchasing patterns (festival spikes, weekend surges)
#   - Geographic clustering (tier-1 vs tier-2/3 cities)
#   - Duplicate customers (~3%) for identity resolution testing
#   - PII leakage in free-text fields (~10-15%)
#   - Late-arriving data (timestamps hours/days old)
#   - Negative/extreme order values (value anomalies ~2%)
#   - Null spikes in optional fields (~5-8%)
#   - Fraudulent transaction patterns (burst orders, high-value)
# ============================================================================

from __future__ import annotations

import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
from faker import Faker
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRODUCT_CATEGORIES: List[str] = [
    "Electronics", "Fashion", "Home & Kitchen", "Books",
    "Beauty & Personal Care", "Sports & Outdoors", "Toys & Games",
    "Grocery & Gourmet", "Automotive", "Health & Wellness",
    "Jewellery", "Baby Products", "Pet Supplies",
    "Office Products", "Musical Instruments",
]

PAYMENT_METHODS: List[str] = [
    "UPI", "Credit Card", "Debit Card", "COD",
    "Wallet", "NetBanking", "EMI", "BNPL",
]

INDIAN_CITIES_WEIGHTED = [
    ("Mumbai", "Maharashtra", 1, 0.12),
    ("Delhi", "Delhi", 1, 0.11),
    ("Bangalore", "Karnataka", 1, 0.10),
    ("Hyderabad", "Telangana", 1, 0.08),
    ("Chennai", "Tamil Nadu", 1, 0.07),
    ("Kolkata", "West Bengal", 1, 0.06),
    ("Pune", "Maharashtra", 1, 0.05),
    ("Ahmedabad", "Gujarat", 1, 0.04),
    ("Jaipur", "Rajasthan", 2, 0.03),
    ("Lucknow", "Uttar Pradesh", 2, 0.03),
    ("Chandigarh", "Punjab", 2, 0.02),
    ("Bhopal", "Madhya Pradesh", 2, 0.02),
    ("Patna", "Bihar", 2, 0.02),
    ("Indore", "Madhya Pradesh", 2, 0.02),
    ("Nagpur", "Maharashtra", 2, 0.02),
    ("Coimbatore", "Tamil Nadu", 2, 0.02),
    ("Kochi", "Kerala", 2, 0.02),
    ("Visakhapatnam", "Andhra Pradesh", 2, 0.02),
    ("Thiruvananthapuram", "Kerala", 2, 0.015),
    ("Guwahati", "Assam", 2, 0.015),
    ("Varanasi", "Uttar Pradesh", 3, 0.01),
    ("Surat", "Gujarat", 2, 0.015),
    ("Mysuru", "Karnataka", 3, 0.01),
    ("Jodhpur", "Rajasthan", 3, 0.01),
    ("Agra", "Uttar Pradesh", 3, 0.01),
    ("Nashik", "Maharashtra", 3, 0.01),
    ("Ranchi", "Jharkhand", 3, 0.01),
    ("Dehradun", "Uttarakhand", 3, 0.01),
    ("Raipur", "Chhattisgarh", 3, 0.008),
    ("Siliguri", "West Bengal", 3, 0.007),
]

CITIES = [c[0] for c in INDIAN_CITIES_WEIGHTED]
CITY_WEIGHTS = [c[3] for c in INDIAN_CITIES_WEIGHTED]
_total_w = sum(CITY_WEIGHTS)
CITY_WEIGHTS = [w / _total_w for w in CITY_WEIGHTS]

ORDER_STATUSES = [
    "delivered", "shipped", "processing", "cancelled", "returned",
    "refund_initiated", "out_for_delivery",
]
ORDER_STATUS_WEIGHTS = [0.55, 0.12, 0.10, 0.08, 0.05, 0.05, 0.05]

FESTIVAL_PERIODS = [
    (1, 14, 17, 2.5, "Makar Sankranti Sale"),
    (3, 8, 11, 2.0, "Holi Sale"),
    (8, 15, 18, 2.0, "Independence Day Sale"),
    (10, 1, 10, 3.5, "Navratri / Big Billion Days"),
    (10, 20, 25, 4.0, "Diwali Sale"),
    (11, 11, 15, 3.0, "Singles Day"),
    (11, 25, 30, 2.5, "Black Friday"),
    (12, 25, 31, 2.0, "Year-End Sale"),
]

REVIEW_SENTIMENTS = {
    1: [
        "Terrible product. Complete waste of money.",
        "Worst purchase ever. Arrived broken.",
        "DO NOT BUY. Quality is pathetic.",
        "Fake product. Nothing like the pictures.",
        "Returned immediately. This is a scam.",
    ],
    2: [
        "Below average quality. Not worth the price.",
        "Disappointed with build quality.",
        "Delivery was delayed by 10 days and product was average.",
        "Not as described. Colour and size were different.",
        "Barely usable. Would not recommend.",
    ],
    3: [
        "Decent product for the price.",
        "Average quality but delivery was on time.",
        "It's okay. Works as expected.",
        "Fair product. Could be better.",
        "Satisfactory purchase.",
    ],
    4: [
        "Good product! Value for money.",
        "Happy with the purchase. Good quality.",
        "Fast delivery and good packaging.",
        "Recommended! Good quality.",
        "Nice product. Very satisfied.",
    ],
    5: [
        "Absolutely love it! Best purchase this year.",
        "Outstanding quality! Exceeded expectations.",
        "Perfect product. Exactly as described. 5 stars!",
        "Incredible value for money.",
        "Top notch! Premium build quality.",
    ],
}

_fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)
np.random.seed(42)


def _weighted_choice(values, weights):
    return random.choices(values, weights=weights, k=1)[0]


def _is_festival_period(dt):
    for month, d_start, d_end, mult, name in FESTIVAL_PERIODS:
        if dt.month == month and d_start <= dt.day <= d_end:
            return True, mult, name
    return False, 1.0, None


# ============================================================================
# Customer generator
# ============================================================================

def generate_customers(n: int = 1_000_000, chunk_size: int = 50_000) -> pd.DataFrame:
    """Generate synthetic Indian customer records with PII."""
    logger.info("Generating {:,} customer records …", n)

    all_chunks = []
    base_customers = []

    for chunk_start in range(0, n, chunk_size):
        actual = min(chunk_size, n - chunk_start)
        records = []
        for _ in range(actual):
            first = _fake.first_name()
            last = _fake.last_name()
            email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{_fake.free_email_domain()}"
            phone = f"+91-{random.randint(7000000000, 9999999999)}"
            aadhaar = f"{random.randint(2000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}"
            pan = "".join(random.choices(string.ascii_uppercase, k=5)) + \
                  "".join(random.choices(string.digits, k=4)) + \
                  random.choice(string.ascii_uppercase)
            city = _weighted_choice(CITIES, CITY_WEIGHTS)
            city_info = next(c for c in INDIAN_CITIES_WEIGHTED if c[0] == city)

            record = {
                "customer_id": uuid.uuid4().hex,
                "first_name": first,
                "last_name": last,
                "email": email,
                "phone": phone,
                "aadhaar": aadhaar,
                "pan_card": pan,
                "city": city,
                "state": city_info[1],
                "city_tier": city_info[2],
                "pincode": str(random.randint(100000, 999999)),
                "registration_date": _fake.date_between(
                    start_date="-4y", end_date="today"
                ).isoformat(),
                "customer_segment": random.choices(
                    ["premium", "regular", "budget", "inactive"],
                    weights=[0.10, 0.45, 0.35, 0.10], k=1
                )[0],
                "consent_marketing": random.choice([True, False]),
                "consent_data_processing": True,
            }
            records.append(record)
            if random.random() < 0.03 and len(base_customers) < n * 0.03:
                base_customers.append(record)

        all_chunks.append(pd.DataFrame(records))
        if chunk_start % (chunk_size * 5) == 0:
            logger.debug("  customers {:,}/{:,}", chunk_start + actual, n)

    df = pd.concat(all_chunks, ignore_index=True)

    # Inject duplicates for identity resolution
    if base_customers:
        dups = []
        for orig in base_customers:
            dup = orig.copy()
            dup["customer_id"] = uuid.uuid4().hex
            if random.random() < 0.5:
                dup["first_name"] = orig["first_name"][:3] + random.choice(["a", "i", ""])
            dups.append(dup)
        df = pd.concat([df, pd.DataFrame(dups)], ignore_index=True)
        logger.info("Injected {:,} duplicate customers", len(dups))

    for col in ["email", "phone", "aadhaar"]:
        mask = np.random.random(len(df)) < 0.05
        df.loc[mask, col] = None

    invalid_mask = np.random.random(len(df)) < 0.02
    df.loc[invalid_mask, "email"] = "not-an-email"

    logger.info("Customer generation complete — {:,} rows", len(df))
    return df


# ============================================================================
# Product generator
# ============================================================================

def generate_products(n: int = 50_000) -> pd.DataFrame:
    logger.info("Generating {:,} product records …", n)
    records = []
    for _ in range(n):
        category = random.choice(PRODUCT_CATEGORIES)
        if random.random() < 0.05:
            price = round(random.uniform(50000, 500000), 2)
        elif random.random() < 0.20:
            price = round(random.uniform(5000, 50000), 2)
        else:
            price = round(random.uniform(99, 5000), 2)

        if random.random() < 0.05:
            description = (
                f"{_fake.catch_phrase()}. Contact: {_fake.name()}, "
                f"email {_fake.email()}, "
                f"phone +91-{random.randint(7000000000, 9999999999)}, "
                f"Aadhaar: {random.randint(2000,9999)} "
                f"{random.randint(1000,9999)} {random.randint(1000,9999)}"
            )
        else:
            description = _fake.catch_phrase()

        records.append({
            "product_id": uuid.uuid4().hex,
            "product_name": _fake.bs().title(),
            "category": category, "price": price,
            "description": description,
            "seller_id": uuid.uuid4().hex[:16],
            "rating": round(random.uniform(1.0, 5.0), 1),
            "stock_quantity": random.randint(0, 10000),
        })
    df = pd.DataFrame(records)
    logger.info("Product generation complete — {:,} rows", len(df))
    return df


# ============================================================================
# Order generator — with seasonal patterns + fraud + anomalies
# ============================================================================

def generate_orders(
    n: int = 5_000_000,
    customer_ids: Optional[List[str]] = None,
    product_ids: Optional[List[str]] = None,
    chunk_size: int = 100_000,
) -> pd.DataFrame:
    logger.info("Generating {:,} order records …", n)

    if customer_ids is None:
        customer_ids = [uuid.uuid4().hex for _ in range(100_000)]
    if product_ids is None:
        product_ids = [uuid.uuid4().hex for _ in range(50_000)]

    fraud_customers = set(random.sample(
        customer_ids, max(1, len(customer_ids) // 200)
    ))
    all_chunks = []

    for chunk_start in range(0, n, chunk_size):
        actual = min(chunk_size, n - chunk_start)
        records = []
        for _ in range(actual):
            order_date = _fake.date_time_between(start_date="-2y", end_date="now")
            is_fest, fest_mult, _ = _is_festival_period(order_date)

            customer_id = random.choice(customer_ids)
            status = _weighted_choice(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)

            base_value = round(min(np.random.lognormal(7.5, 1.2), 200000), 2)
            if is_fest:
                base_value *= random.uniform(1.0, fest_mult)
            if order_date.weekday() >= 5:
                base_value *= random.uniform(1.0, 1.15)

            if customer_id in fraud_customers and random.random() < 0.3:
                base_value = round(random.uniform(50000, 200000), 2)
                status = "processing"

            delivery_date = order_date + timedelta(days=random.randint(1, 15))

            if random.random() < 0.10:
                delivery_instructions = (
                    f"Deliver to {_fake.name()}, "
                    f"Aadhaar: {random.randint(2000,9999)} "
                    f"{random.randint(1000,9999)} {random.randint(1000,9999)}, "
                    f"Phone: +91-{random.randint(7000000000, 9999999999)}, "
                    f"PAN: {''.join(random.choices(string.ascii_uppercase, k=5))}"
                    f"{''.join(random.choices(string.digits, k=4))}"
                    f"{random.choice(string.ascii_uppercase)}"
                )
            else:
                delivery_instructions = random.choice([
                    "Leave at door", "Ring bell twice",
                    "Call before delivery", "Hand to security",
                    "Fragile - handle with care",
                    "Leave with neighbour", "Gift wrap requested", "",
                ])

            records.append({
                "order_id": uuid.uuid4().hex,
                "customer_id": customer_id,
                "product_id": random.choice(product_ids),
                "product_category": random.choice(PRODUCT_CATEGORIES),
                "order_value": round(base_value, 2),
                "payment_method": random.choice(PAYMENT_METHODS),
                "order_status": status,
                "order_timestamp": order_date.isoformat(),
                "delivery_date": delivery_date.isoformat()
                    if status in ("delivered", "out_for_delivery") else None,
                "delivery_city": _weighted_choice(CITIES, CITY_WEIGHTS),
                "delivery_pincode": str(random.randint(100000, 999999)),
                "delivery_instructions": delivery_instructions,
            })

        all_chunks.append(pd.DataFrame(records))
        if chunk_start % (chunk_size * 5) == 0:
            logger.debug("  orders {:,}/{:,}", chunk_start + actual, n)

    df = pd.concat(all_chunks, ignore_index=True)

    # Negative values (refund errors)
    drift = np.random.random(len(df)) < 0.02
    df.loc[drift, "order_value"] = -abs(df.loc[drift, "order_value"])

    # Extreme outliers
    outlier = np.random.random(len(df)) < 0.005
    df.loc[outlier, "order_value"] = np.random.uniform(500000, 2000000, outlier.sum())

    # Late-arriving data
    late = np.random.random(len(df)) < 0.03
    for idx in df.index[late]:
        orig = datetime.fromisoformat(df.at[idx, "order_timestamp"])
        df.at[idx, "order_timestamp"] = (
            orig - timedelta(hours=random.randint(6, 168))
        ).isoformat()

    for col in ["delivery_instructions", "delivery_date"]:
        mask = np.random.random(len(df)) < 0.05
        df.loc[mask, col] = None

    city_null = np.random.random(len(df)) < 0.01
    df.loc[city_null, "delivery_city"] = None

    logger.info("Order generation complete — {:,} rows", len(df))
    return df


# ============================================================================
# Review generator
# ============================================================================

def generate_reviews(
    n: int = 2_000_000,
    customer_ids: Optional[List[str]] = None,
    product_ids: Optional[List[str]] = None,
    chunk_size: int = 100_000,
) -> pd.DataFrame:
    logger.info("Generating {:,} review records …", n)

    if customer_ids is None:
        customer_ids = [uuid.uuid4().hex for _ in range(100_000)]
    if product_ids is None:
        product_ids = [uuid.uuid4().hex for _ in range(50_000)]

    all_chunks = []
    for chunk_start in range(0, n, chunk_size):
        actual = min(chunk_size, n - chunk_start)
        records = []
        for _ in range(actual):
            rating = random.choices(
                [1, 2, 3, 4, 5], weights=[0.12, 0.08, 0.15, 0.25, 0.40], k=1
            )[0]
            base = random.choice(REVIEW_SENTIMENTS[rating])

            if random.random() < 0.15:
                pii = random.choice(["full", "email", "phone", "aadhaar"])
                if pii == "full":
                    review_text = (
                        f"{base} My name is {_fake.name()} and you can "
                        f"reach me at {_fake.email()} or "
                        f"+91-{random.randint(7000000000, 9999999999)}."
                    )
                elif pii == "email":
                    review_text = f"{base} Contact: {_fake.email()}"
                elif pii == "phone":
                    review_text = f"{base} Call +91-{random.randint(7000000000, 9999999999)}"
                else:
                    review_text = (
                        f"{base} Aadhaar: {random.randint(2000,9999)} "
                        f"{random.randint(1000,9999)} {random.randint(1000,9999)}"
                    )
            else:
                review_text = base if random.random() > 0.3 else f"{base} {_fake.sentence()}"

            records.append({
                "review_id": uuid.uuid4().hex,
                "customer_id": random.choice(customer_ids),
                "product_id": random.choice(product_ids),
                "rating": rating,
                "review_text": review_text,
                "review_date": _fake.date_between(
                    start_date="-2y", end_date="today"
                ).isoformat(),
                "verified_purchase": random.choices(
                    [True, False], weights=[0.7, 0.3], k=1
                )[0],
            })

        all_chunks.append(pd.DataFrame(records))
        if chunk_start % (chunk_size * 10) == 0:
            logger.debug("  reviews {:,}/{:,}", chunk_start + actual, n)

    df = pd.concat(all_chunks, ignore_index=True)
    logger.info("Review generation complete — {:,} rows", len(df))
    return df


# ============================================================================
# Order Items generator
# ============================================================================

def generate_order_items(
    n: int = 10_000_000,
    order_ids: Optional[List[str]] = None,
    product_ids: Optional[List[str]] = None,
    chunk_size: int = 200_000,
) -> pd.DataFrame:
    logger.info("Generating {:,} order-item records …", n)

    if order_ids is None:
        order_ids = [uuid.uuid4().hex for _ in range(1_000_000)]
    if product_ids is None:
        product_ids = [uuid.uuid4().hex for _ in range(50_000)]

    all_chunks = []
    for chunk_start in range(0, n, chunk_size):
        actual = min(chunk_size, n - chunk_start)
        records = []
        for _ in range(actual):
            qty = random.choices(
                range(1, 11),
                weights=[0.40, 0.25, 0.15, 0.08, 0.05, 0.03, 0.02, 0.01, 0.005, 0.005],
                k=1
            )[0]
            unit_price = round(min(np.random.lognormal(6.5, 1.0), 100000), 2)
            records.append({
                "item_id": uuid.uuid4().hex,
                "order_id": random.choice(order_ids),
                "product_id": random.choice(product_ids),
                "quantity": qty, "unit_price": unit_price,
                "total_price": round(qty * unit_price, 2),
                "discount_pct": round(random.choices(
                    [0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
                    weights=[0.30, 0.20, 0.20, 0.15, 0.08, 0.05, 0.02], k=1
                )[0], 2),
            })
        all_chunks.append(pd.DataFrame(records))
        if chunk_start % (chunk_size * 10) == 0:
            logger.debug("  items {:,}/{:,}", chunk_start + actual, n)

    df = pd.concat(all_chunks, ignore_index=True)
    neg = np.random.random(len(df)) < 0.005
    df.loc[neg, "total_price"] = -abs(df.loc[neg, "total_price"])
    logger.info("Order-item generation complete — {:,} rows", len(df))
    return df


# ============================================================================
# Convenience: generate full dataset suite
# ============================================================================

def generate_all(
    output_dir: str = "data/raw",
    customers_n: int = 1_000_000,
    products_n: int = 50_000,
    orders_n: int = 5_000_000,
    reviews_n: int = 2_000_000,
    order_items_n: int = 10_000_000,
    file_format: str = "parquet",
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(customers_n)
    cust_ids = customers["customer_id"].tolist()

    products = generate_products(products_n)
    prod_ids = products["product_id"].tolist()

    orders = generate_orders(orders_n, customer_ids=cust_ids, product_ids=prod_ids)
    order_ids = orders["order_id"].tolist()

    reviews = generate_reviews(reviews_n, customer_ids=cust_ids, product_ids=prod_ids)

    items = generate_order_items(
        order_items_n, order_ids=order_ids, product_ids=prod_ids
    )

    for name, df in [
        ("customers", customers), ("products", products),
        ("orders", orders), ("reviews", reviews),
        ("order_items", items),
    ]:
        path = out / f"{name}.{file_format}"
        if file_format == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)
        logger.info("Saved {:,} rows -> {}", len(df), path)

    logger.info("All synthetic datasets generated in {}", out)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate synthetic data")
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--customers", type=int, default=1_000_000)
    parser.add_argument("--products", type=int, default=50_000)
    parser.add_argument("--orders", type=int, default=5_000_000)
    parser.add_argument("--reviews", type=int, default=2_000_000)
    parser.add_argument("--items", type=int, default=10_000_000)
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    args = parser.parse_args()
    generate_all(
        output_dir=args.output,
        customers_n=args.customers, products_n=args.products,
        orders_n=args.orders, reviews_n=args.reviews,
        order_items_n=args.items, file_format=args.format,
    )
