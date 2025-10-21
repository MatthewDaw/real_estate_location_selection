#!/bin/bash
# 08_run_main_tables.sh - Run only 08_main_tables models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 10:Business potential for each buying option.${NC}"
echo "========================="

echo -e "${BLUE}Running: dbt run --select 10_business_potential${NC}"
dbt run --select 10_business_potential

echo -e "${GREEN}✅ Stage 10 completed successfully!${NC}"