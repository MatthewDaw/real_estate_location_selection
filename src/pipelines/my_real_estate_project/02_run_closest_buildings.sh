#!/bin/bash
# 02_run_closest_buildings.sh - Run 02_closest_buildings models and script

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 02: Closest Buildings${NC}"
echo "=================================="

echo -e "${BLUE}Step 1: Running dbt models in 02_closest_buildings${NC}"
dbt run --select 02_closest_buildings

echo -e "${BLUE}Step 2: Running closest buildings Python script${NC}"
python scripts/02_closest_buildings.py

echo -e "${GREEN}✅ Stage 02 completed successfully!${NC}"