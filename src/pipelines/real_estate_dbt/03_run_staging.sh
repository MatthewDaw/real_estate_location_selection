#!/bin/bash
# 03_run_staging.sh - Run only 03_staging models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 03: Staging Models${NC}"
echo "============================"

echo -e "${BLUE}Running: dbt run --select 03_staging${NC}"
dbt run --select 03_staging

echo -e "${GREEN}✅ Stage 03 completed successfully!${NC}"