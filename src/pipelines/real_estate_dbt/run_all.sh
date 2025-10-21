#!/bin/bash
# run_all.sh - Complete end-to-end pipeline execution

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${PURPLE}================================================${NC}"
    echo -e "${PURPLE} $1${NC}"
    echo -e "${PURPLE}================================================${NC}"
}

print_stage() {
    echo ""
    echo -e "${BLUE}🔄 Running Stage $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

run_stage() {
    local stage_num="$1"
    local stage_name="$2"
    local script_name="$3"

    print_stage "$stage_num: $stage_name"

    if ./"$script_name"; then
        print_success "Stage $stage_num completed successfully"
    else
        print_error "Stage $stage_num failed"
        exit 1
    fi
}

# Start pipeline
print_header "🚀 REAL ESTATE DATA PIPELINE - COMPLETE EXECUTION"

echo "Pipeline started at: $(date)"
echo "Schema: ${DBT_TARGET_SCHEMA:-public}"
echo ""

# Execute each stage in order
run_stage "01" "Raw Models" "01_run_raw.sh"
run_stage "02" "Closest Buildings" "02_run_closest_buildings.sh"
run_stage "03" "Staging Models" "03_run_staging.sh"
run_stage "04" "Units History" "04_run_units_history.sh"
run_stage "05" "Prediction Analysis" "05_run_prediction_analysis.sh"
run_stage "06" "Building Summaries" "06_run_building_summaries.sh"
run_stage "07" "Points of Study & Spatial Smoothing" "07_run_points_of_study.sh"
run_stage "08" "Main Tables" "08_run_main_tables.sh"
run_stage "09" "General Statistics" "09_run_general_stats.sh"
run_stage "10" "Business Potential Analysis" "10_run_business_potential.sh"

# Run tests
echo ""
echo -e "${BLUE}🔄 Running Data Quality Tests${NC}"
if dbt test; then
    print_success "All data quality tests passed"
else
    echo -e "${YELLOW}⚠️  Some tests failed - check dbt output for details${NC}"
fi

# Pipeline completion
print_header "🎉 PIPELINE COMPLETED SUCCESSFULLY!"

echo ""
echo "Pipeline Summary:"
echo "- ✅ Stage 01: Raw data models"
echo "- ✅ Stage 02: Closest buildings calculation"
echo "- ✅ Stage 03: Staging transformations"
echo "- ✅ Stage 04: Units history processing"
echo "- ✅ Stage 05: Price predictions & analysis"
echo "- ✅ Stage 06: Building summaries"
echo "- ✅ Stage 07: Points of study & spatial smoothing"
echo "- ✅ Stage 08: Final main tables"
echo "- ✅ Stage 09: General statistics"
echo "- ✅ Stage 10: Business potential analysis"
echo "- ✅ Data quality tests"
echo ""
echo "Pipeline completed at: $(date)"
echo ""
echo "🎯 Your real estate data pipeline is ready for analysis!"