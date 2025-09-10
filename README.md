# Utah Real Estate Analytics - dbt Project

This dbt project provides comprehensive analytics for the Utah real estate market, combining rental, for-sale, and land listing data from multiple sources.

## 🏠 Project Overview

Our analytics platform processes data from three major real estate sources:
- **Hello Data API**: Rental buildings and units
- **Zillow**: For-sale property listings
- **Landwatch**: Land and property listings

All data is focused on the Utah market with comprehensive geographic and price filtering.

## 📊 Architecture

The project follows a layered data architecture:

```
01_raw → 02_closest_buildings → 03_staging → 04_units_history → 05_prediction_analysis → 06_property_summaries → 07_points_of_study → 08_main_tables
```

### Layer Descriptions

| Layer | Purpose | Materialization | Use Case |
|-------|---------|-----------------|----------|
| `01_raw` | Data ingestion with basic filtering | Views | Source data validation |
| `02_closest_buildings` | Spatial proximity analysis | Views | Location intelligence |
| `03_staging` | Cleaned and standardized data | Views | Development and testing |
| `04_units_history` | Time-series unit analysis | Views | Trend analysis |
| `05_prediction_analysis` | Predictive modeling | Tables | Forecasting |
| `06_property_summaries` | Building-level aggregations | Views | Portfolio analysis |
| `07_points_of_study` | Research datasets | Tables | Special studies |
| `08_main_tables` | Final analytical tables | Tables | Reporting & dashboards |

## 🚀 Quick Start

### Prerequisites
- dbt Core 1.0+ installed
- Access to the raw data sources
- PostgreSQL/BigQuery/Snowflake connection configured

### Setup
1. Clone this repository
2. Configure your `profiles.yml` with database connection details
3. Install dependencies: `dbt deps`
4. Test connection: `dbt debug`

### Initial Run
```bash
# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve
```

## 📋 Data Sources

### Hello Data Buildings
- **Source**: `hello_data_buildings_raw`
- **Records**: ~X buildings in Utah
- **Refresh**: Daily
- **Key Fields**: Building characteristics, amenities, location, management

### Hello Data Units  
- **Source**: `hello_data_units_raw` 
- **Records**: ~X units in Utah
- **Refresh**: Daily
- **Key Fields**: Unit specs, pricing, availability, floorplans

### Zillow Properties
- **Source**: `zillow_property_raw`
- **Records**: ~X for-sale properties in Utah
- **Refresh**: Daily  
- **Key Fields**: Listing price, property details, market metrics

### Landwatch Properties
- **Source**: `landwatch_properties_raw`
- **Records**: ~X land listings in Utah
- **Refresh**: Daily
- **Key Fields**: Lot size, land characteristics, pricing

## 🎯 Key Models

### Core Dimensional Tables
- `dim_properties`: Comprehensive building and unit characteristics
- `dim_geography`: Location hierarchies and geographic data
- `dim_time`: Time dimension for temporal analysis

### Fact Tables  
- `fact_unit_pricing`: Historical unit pricing and availability
- `fact_property_listings`: Property listing events and changes
- `fact_market_metrics`: Aggregated market indicators

### Analytics Tables
- `market_summary`: High-level market trends and KPIs
- `building_performance`: Building-level occupancy and pricing metrics
- `competitive_analysis`: Cross-property comparison metrics

## 🔍 Data Quality

### Testing Strategy
- **Source Tests**: Data freshness, uniqueness, and referential integrity
- **Model Tests**: Business logic validation and data quality checks
- **Custom Tests**: Utah-specific geographic and price validation

### Key Data Tests
- Geographic coordinates within valid ranges
- Price filtering ($10K - $800K range)
- State filtering (Utah only)
- Relationship integrity between buildings and units
- Data freshness (within 48 hours)

## 📈 Usage Examples

### Market Analysis
```sql
-- Average rent by city
SELECT 
    city,
    AVG(effective_price) as avg_rent,
    COUNT(*) as unit_count
FROM {{ ref('fact_unit_pricing') }}
WHERE state = 'UT' 
    AND availability = 'available'
GROUP BY city
ORDER BY avg_rent DESC;
```

### Building Performance
```sql
-- Top performing buildings by occupancy
SELECT 
    building_name,
    leased_percentage,
    total_units,
    avg_effective_rent
FROM {{ ref('building_performance') }}
WHERE leased_percentage > 0.90
ORDER BY leased_percentage DESC;
```

## 📝 Documentation

Comprehensive documentation is available through dbt docs:

```bash
dbt docs generate
dbt docs serve
```

This includes:
- **Model Documentation**: Detailed descriptions of all tables and columns
- **Data Lineage**: Visual representation of data flow
- **Source Documentation**: External data source details
- **Test Results**: Current data quality status

## 🛠️ Development

### Folder Structure
```
models/
├── 01_raw/           # Raw data models
├── 02_closest_buildings/  # Proximity analysis
├── 03_staging/       # Staging models  
├── 04_units_history/ # Historical analysis
├── 05_prediction_analysis/ # Predictive models
├── 06_property_summaries/  # Aggregations
├── 07_points_of_study/     # Research datasets
└── 08_main_tables/   # Final analytical tables

tests/               # Custom data tests
macros/             # Reusable SQL macros
seeds/              # Static reference data
snapshots/          # SCD Type 2 tables
```

### Coding Standards
- Use descriptive model names following the layer prefix convention
- Include comprehensive column documentation
- Add appropriate data tests for all models
- Follow SQL style guide (4-space indentation, explicit joins)
- Use Jinja variables for configuration values

### Adding New Models
1. Create SQL file in appropriate layer folder
2. Add model documentation to `schema.yml`
3. Include relevant data tests
4. Update dependencies and documentation
5. Test locally before committing

## 🔐 Data Governance

### Data Classification
- **Public**: Aggregated market statistics
- **Internal**: Building and unit details
- **Restricted**: PII and sensitive business data

### Access Control
- Production data access requires approval
- Development environment mirrors production structure
- Audit logging enabled for all data access

### Privacy & Compliance
- No personally identifiable information (PII) stored
- Geographic data limited to property-level coordinates
- Compliance with local data protection regulations

## 📞 Support

### Team Contacts
- **Data Engineering**: data-eng@company.com
- **Analytics**: analytics@company.com  
- **Data Science**: data-science@company.com

### Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [Company Data Wiki](internal-wiki-link)
- [Real Estate Data Dictionary](data-dictionary-link)

## 🔄 Release Notes

### v1.0.0 (Current)
- Initial project setup with Utah market focus
- Core raw data models implemented
- Basic staging layer established
- Comprehensive documentation framework

### Roadmap
- [ ] Advanced predictive models
- [ ] Real-time data streaming
- [ ] Additional geographic markets
- [ ] Enhanced visualization dashboards