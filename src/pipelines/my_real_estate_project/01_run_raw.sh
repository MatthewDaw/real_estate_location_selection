#!/bin/bash
# 01_run_raw.sh - Run only 01_raw models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 01: Raw Models${NC}"
echo "=========================="

echo -e "${BLUE}Running: dbt run --select 01_raw${NC}"
dbt run --select 01_raw

echo -e "${GREEN}✅ Stage 01 completed successfully!${NC}"