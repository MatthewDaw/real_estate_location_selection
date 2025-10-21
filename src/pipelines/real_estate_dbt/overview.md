# Real Estate Analytics Project

{% docs __overview__ %}

## Project Overview

This dbt project provides comprehensive analytics for the Utah real estate market, combining data from multiple sources to deliver insights across rental properties, for-sale homes, and land listings.

### Data Sources

Our analysis combines data from three primary sources:

1. **Hello Data API** - Rental building and unit data
2. **Zillow** - For-sale property listings  
3. **Landwatch** - Land and property listings

### Architecture

The project follows a layered data architecture with 8 distinct layers:

```
Raw Data → Proximity → Staging → History → Predictions → Summaries → Research → Final Tables
```

Each layer serves a specific purpose in transforming raw data into actionable insights.

### Key Features

- **Geographic Focus**: All data filtered to Utah market
- **Multi-source Integration**: Combines rental, sale, and land data
- **Time-series Analysis**: Historical pricing and availability trends
- **Spatial Analytics**: Building proximity and location analysis
- **Predictive Modeling**: Forecasting and trend analysis

### Getting Started

1. **Raw Layer (01_raw)**: Start here to understand the source data
2. **Staging Layer (03_staging)**: Clean, validated data ready for analysis
3. **Main Tables (08_main_tables)**: Final analytical outputs for dashboards

### Data Quality

- All models include comprehensive data tests
- Source freshness monitoring for external APIs
- Referential integrity checks between related tables
- Price and coordinate validation rules

{% docs smoothed_values_detailed %}
### 🐍 Advanced Geospatial Smoothing Model

The `smoothed_values` model represents a sophisticated Python-based geospatial analysis pipeline that computes spatially smoothed rental pricing metrics using advanced spatial algorithms.

#### **🏗️ Hybrid dbt + Python Workflow**
```
dbt Model (smoothed_values.sql)
    │
    ├─➤ Creates table structure & indexes
    │
Python Script (07_precompute_spatial_smoothing.py)  
    │
    ├─➤ Reads: points_of_interest, hd_building_summary, zillow_building_summary
    ├─➤ Processes: KDTree spatial analysis (5-mile radius)
    ├─➤ Writes: Populates smoothed_values table
    │
Downstream dbt Models
    │
    └─➤ Reference {{ ref('smoothed_values') }} for analysis
```

#### **🚀 Why Python + KDTree?**
This model uses Python with SciPy's KDTree spatial indexing because:
- **Performance**: KDTree provides O(log n) spatial queries vs O(n²) SQL joins
- **Scalability**: Handles thousands of POIs × thousands of buildings efficiently  
- **Precision**: Uses proper geodetic distance calculations
- **Memory Efficiency**: Vectorized operations with NumPy/Pandas
- **Flexibility**: Configurable radius parameters and spatial algorithms

#### **📊 Spatial Analysis Method**
For each Point of Interest (POI):
1. **Spatial Query**: Find all buildings within 5-mile radius using KDTree
2. **Metric Aggregation**: Compute averaged pricing metrics for nearby buildings
3. **Multi-source Fusion**: Combine Hello Data rental + Zillow sale data
4. **Quality Metrics**: Track count of buildings contributing to each average

#### **🏗️ Data Dependencies**
- **`points_of_interest`**: Spatial anchor points (restaurants, schools, etc.)
- **`hd_building_summary`**: Hello Data rental pricing and predictions
- **`zillow_building_summary`**: Zillow sale pricing and market trends

#### **⚡ Performance Features**
- **Spatial Indexing**: KDTree for sub-second proximity queries
- **Bulk Processing**: Processes 1000s of POIs efficiently
- **Configurable Radius**: Default 5 miles, adjustable via CLI
- **Memory Optimized**: Streaming processing for large datasets

#### **🔧 Script Execution**
```bash
# Run spatial smoothing with default 5-mile radius
python scripts/07_precompute_spatial_smoothing.py

# Custom radius and output table
python scripts/07_precompute_spatial_smoothing.py \
  --radius-miles 3.0 \
  --output-table custom_smoothed_values
```

#### **📈 Output Metrics**
The model produces spatially smoothed metrics including:
- **Hello Data**: Predicted/average/effective pricing per sqft and bedroom
- **Zillow Data**: Market trends and price predictions  
- **Spatial Counts**: Number of buildings contributing to each average
- **Quality Indicators**: Coverage and density metrics

{% enddocs %}

## Model Layers

{% docs layer_01_raw %}
### 01_raw - Raw Data Layer

**Purpose**: Ingest and minimally process raw data from external sources.

**Materialization**: Views (for fast development and testing)

**Key Transformations**:
- Geographic filtering (Utah only)
- Basic data quality filters
- Deduplication where necessary
- Maintains all source columns for flexibility

**Models**:
- `raw_buildings` - Hello Data building information
- `raw_units` - Hello Data unit details  
- `raw_units_history` - Historical unit data
- `raw_zillow` - Zillow property listings
- `raw_landwatch` - Landwatch property listings

{% enddocs %}

{% docs layer_03_staging %}
### 03_staging - Staging Layer

**Purpose**: Clean, standardize, and validate data for downstream consumption.

**Materialization**: Views (lightweight transformations)

**Key Transformations**:
- Data type standardization
- Null handling and default values
- Business rule validation
- Column renaming for consistency
- Derived field calculations

{% enddocs %}

{% docs layer_05_prediction %}
### 05_prediction_analysis - Prediction Layer

**Purpose**: Predictive modeling and forecasting analysis.

**Materialization**: Tables (for performance with complex calculations)

**Key Features**:
- Price trend forecasting
- Occupancy predictions
- Market cycle analysis
- Seasonal adjustment models

{% enddocs %}

{% docs layer_08_main %}
### 08_main_tables - Final Analytical Layer

**Purpose**: Final analytical tables optimized for reporting and dashboards.

**Materialization**: Tables (for query performance)

**Key Features**:
- Pre-aggregated metrics
- Optimized for BI tools
- Comprehensive business logic
- Ready for end-user consumption

{% enddocs %}

## Data Definitions

{% docs utah_market %}
### Utah Market Definition

All data in this project is filtered to the Utah real estate market using the state abbreviation 'UT'. This includes:

- **Geographic Scope**: All properties within Utah state boundaries
- **Market Focus**: Rental, for-sale, and land markets
- **Time Period**: Data availability varies by source (see source freshness)

{% enddocs %}

{% docs price_filtering %}
### Price Filtering Logic

To ensure data quality and focus on relevant market segments, price filtering is applied:

- **Minimum Price**: $10,000 (excludes data errors and non-market transactions)
- **Maximum Price**: $800,000 (focuses on mainstream market, excludes luxury outliers)
- **Application**: Applied to Zillow and Landwatch data sources
- **Rationale**: Balances data quality with market coverage

{% enddocs %}

{% docs coordinate_validation %}
### Geographic Coordinate Validation

All geographic data undergoes validation to ensure accuracy:

- **Latitude Range**: -90 to 90 degrees
- **Longitude Range**: -180 to 180 degrees  
- **Null Handling**: Records with missing coordinates are excluded
- **Spatial Consistency**: Coordinates must align with stated addresses

{% enddocs %}