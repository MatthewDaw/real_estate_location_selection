#!/bin/bash
# 06_property_summaries.sh - Run only 06_property_summaries models

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Stage 06: Building Summaries${NC}"
echo "================================"

echo -e "${BLUE}Running: dbt run --select 06_property_summaries${NC}"
dbt run --select 06_property_summaries

echo -e "${GREEN}✅ Stage 06 completed successfully!${NC}"