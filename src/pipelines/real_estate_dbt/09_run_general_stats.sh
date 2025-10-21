#!/bin/bash
# 08_run_main_tables.sh - Run only 08_main_tables models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 09: Expanded main tables${NC}"
echo "========================="

echo -e "${BLUE}Running: dbt run --select 09_expanded_main_tables${NC}"
dbt run --select 09_expanded_main_tables

echo -e "${GREEN}✅ Stage 09 completed successfully!${NC}"
