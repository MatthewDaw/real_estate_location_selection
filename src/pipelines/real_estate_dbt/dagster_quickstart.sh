#!/bin/bash
# Quick start script for Dagster real estate pipeline

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Real Estate Dagster Pipeline Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check Python version
echo -e "${BLUE}Checking Python version...${NC}"
python --version
echo ""

# Install the package
echo -e "${BLUE}Installing real-estate-dagster package...${NC}"
pip install -e ".[dev]"
echo -e "${GREEN}✓ Package installed${NC}"
echo ""

# Check dbt connection
echo -e "${BLUE}Checking dbt configuration...${NC}"
if dbt debug --profiles-dir ~/.dbt; then
    echo -e "${GREEN}✓ dbt connection successful${NC}"
else
    echo -e "${YELLOW}⚠ dbt connection check failed - please verify your profiles.yml${NC}"
fi
echo ""

# Install dbt packages
echo -e "${BLUE}Installing dbt packages...${NC}"
dbt deps
echo -e "${GREEN}✓ dbt packages installed${NC}"
echo ""

# Parse dbt project to generate manifest
echo -e "${BLUE}Generating dbt manifest...${NC}"
dbt parse
echo -e "${GREEN}✓ dbt manifest generated${NC}"
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}To start the Dagster UI:${NC}"
echo -e "  dagster dev -f real_estate_dagster/definitions.py"
echo ""
echo -e "${BLUE}Or run the complete pipeline:${NC}"
echo -e "  dagster job execute -f real_estate_dagster/definitions.py -j complete_real_estate_pipeline"
echo ""
echo -e "${BLUE}For more information, see:${NC}"
echo -e "  DAGSTER_README.md"
echo ""
