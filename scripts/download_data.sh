#!/bin/bash
# ============================================================================
# Download / generate sample datasets
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

mkdir -p data/raw

echo "Generating synthetic e-commerce data …"
python -m src.utils.data_generator \
    --output data/raw \
    --customers 10000 \
    --products 10000 \
    --orders 100000 \
    --reviews 50000 \
    --items 100000 \
    --format parquet

echo ""
echo "Data generation complete!  Files in data/raw/:"
ls -lh data/raw/
