#!/bin/bash
# 04_run_units_history.sh - Run only 04_units_history models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 04: Units History Models${NC}"
echo "=================================="

echo -e "${BLUE}Running: dbt run --select 04_units_history${NC}"
dbt run --select 04_units_history

echo -e "${GREEN}✅ Stage 04 completed successfully!${NC}"