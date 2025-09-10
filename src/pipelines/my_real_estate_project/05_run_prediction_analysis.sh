#!/bin/bash
# 05_run_prediction_analysis.sh - Run 05_prediction_analysis models and script

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 05: Prediction Analysis${NC}"
echo "=================================="

echo -e "${BLUE}Step 1: Running dbt models in 05_prediction_analysis${NC}"
dbt run --select 05_prediction_analysis

echo -e "${BLUE}Step 2: Running price prediction Python script${NC}"
python scripts/05_price_prediction_growth.py

echo -e "${GREEN}✅ Stage 05 completed successfully!${NC}"