# scripts/visualize_predictions.py
"""
Property Price Visualization Tool

Creates clean visualizations of historical data with prediction points and confidence intervals
for both building and Zillow analysis results, including geographic analysis.
"""

import os
import sys
import click
import logging
import json
import yaml
from pathlib import Path
from typing import Optional, List

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_dbt_database_url():
    """Extract database URL from dbt profiles."""
    profiles_paths = [
        Path.home() / '.dbt' / 'profiles.yml',
        Path('profiles.yml'),
        Path('~/.dbt/profiles.yml').expanduser()
    ]

    profiles_path = None
    for path in profiles_paths:
        if path.exists():
            profiles_path = path
            break

    if not profiles_path:
        return None

    try:
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)

        profile_name = 'my_real_estate_project'
        target = 'dev'

        if profile_name in profiles:
            profile = profiles[profile_name]
            if 'outputs' in profile and target in profile['outputs']:
                output = profile['outputs'][target]

                if output.get('type') == 'postgres':
                    host = output.get('host', 'localhost')
                    port = output.get('port', 5432)
                    user = output.get('user')
                    password = output.get('password', '')
                    dbname = output.get('dbname')

                    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    except Exception as e:
        logger.warning(f"Could not read dbt profiles: {e}")
        return None

    return None


class PropertyVisualizer:
    """Create visualizations for property price predictions."""

    def __init__(self, db_url: str = None):
        self.db_url = db_url or os.environ.get('DATABASE_URL') or get_dbt_database_url()

        if not self.db_url:
            logger.error(
                "No database URL provided. Please set DATABASE_URL environment variable or configure dbt profiles.")
            sys.exit(1)

        self.engine = create_engine(self.db_url)

    def get_building_data(self, building_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Get historical data and analysis results for a building."""

        # Get historical price data
        historical_query = f"""
        SELECT building_id, price, effective_price, date
        FROM units_history_grouped_by_building
        WHERE building_id = '{building_id}'
        ORDER BY date
        """

        historical_data = pd.read_sql(historical_query, self.engine)

        # Get analysis results
        analysis_query = f"""
        SELECT * FROM building_analysis 
        WHERE building_id = '{building_id}'
        ORDER BY prediction_date DESC 
        LIMIT 1
        """

        analysis_data = pd.read_sql(analysis_query, self.engine)

        return historical_data, analysis_data

    def get_zillow_data(self, property_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Get historical data and analysis results for a Zillow property."""

        # Get price history from the new zillow_history model
        zillow_query = f"""
        SELECT id, date, price, price_change_rate
        FROM zillow_history
        WHERE id = '{property_id}'
        ORDER BY date
        """

        historical_data = pd.read_sql(zillow_query, self.engine)

        # Get analysis results
        analysis_query = f"""
        SELECT * FROM zillow_analysis 
        WHERE id = '{property_id}'
        ORDER BY prediction_date DESC 
        LIMIT 1
        """

        analysis_data = pd.read_sql(analysis_query, self.engine)

        return historical_data, analysis_data

    def get_building_geographic_data(self, geography_type: str, geography_value: str) -> tuple[
        pd.DataFrame, pd.DataFrame]:
        """Get historical data and analysis results for a building geographic area."""

        # Get historical price data from the geographic aggregation
        historical_query = f"""
        SELECT geography_type, geography_value, price, effective_price, date, unit_count
        FROM units_history_grouped_by_geography
        WHERE geography_type = '{geography_type}' AND geography_value = '{geography_value}'
        ORDER BY date
        """

        historical_data = pd.read_sql(historical_query, self.engine)

        # Get analysis results
        analysis_query = f"""
        SELECT * FROM building_geographic_analysis 
        WHERE geography_type = '{geography_type}' AND geography_value = '{geography_value}'
        ORDER BY prediction_date DESC 
        LIMIT 1
        """

        analysis_data = pd.read_sql(analysis_query, self.engine)

        return historical_data, analysis_data

    def get_zillow_geographic_data(self, geography_type: str, geography_value: str) -> tuple[
        pd.DataFrame, pd.DataFrame]:
        """Get historical data and analysis results for a Zillow geographic area."""

        # Use the new zillow_history_grouped_by_geography model - much simpler!
        historical_query = f"""
        SELECT geography_type, geography_value, date, price, price_change_rate, property_count
        FROM zillow_history_grouped_by_geography
        WHERE geography_type = '{geography_type}' AND geography_value = '{geography_value}'
        ORDER BY date
        """

        historical_data = pd.read_sql(historical_query, self.engine)

        # Get analysis results
        analysis_query = f"""
        SELECT * FROM zillow_geographic_analysis 
        WHERE geography_type = '{geography_type}' AND geography_value = '{geography_value}'
        ORDER BY prediction_date DESC 
        LIMIT 1
        """

        analysis_data = pd.read_sql(analysis_query, self.engine)

        return historical_data, analysis_data


    def create_building_visualization(self, building_id: str, output_dir: str = "graphs"):
        """Create visualization for a building with both price and effective price."""

        historical_data, analysis_data = self.get_building_data(building_id)

        if historical_data.empty or analysis_data.empty:
            logger.warning(f"No data found for building {building_id}")
            return

        # Ensure output directory exists
        building_dir = Path(output_dir) / "buildings"
        building_dir.mkdir(parents=True, exist_ok=True)

        result = analysis_data.iloc[0]

        # Create figure with 2 subplots (price and effective price)
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle(f'Building {building_id} - Price Predictions with Confidence Intervals',
                     fontsize=14, fontweight='bold')

        # Convert dates
        historical_data['date'] = pd.to_datetime(historical_data['date'])
        today = pd.Timestamp.now().normalize()
        # Assume 365 days for future prediction (can be made configurable)
        future_date = today + pd.Timedelta(days=365)

        # Plot 1: Regular Price
        ax1.plot(historical_data['date'], historical_data['price'],
                 'b-', linewidth=2, label='Historical Price', alpha=0.8)

        # Current prediction point with confidence interval
        ax1.errorbar([today], [result['predicted_current_price']],
                     yerr=[[result['predicted_current_price'] - result['predicted_current_price_lower']],
                           [result['predicted_current_price_upper'] - result['predicted_current_price']]],
                     fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                     label=f"Current: ${result['predicted_current_price']:,.0f}")

        # Future prediction point with confidence interval
        ax1.errorbar([future_date], [result['predicted_future_price']],
                     yerr=[[result['predicted_future_price'] - result['predicted_future_price_lower']],
                           [result['predicted_future_price_upper'] - result['predicted_future_price']]],
                     fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                     label=f"Future (1 year): ${result['predicted_future_price']:,.0f}")

        ax1.set_title('Regular Price Analysis')
        ax1.set_ylabel('Price ($)')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # Plot 2: Effective Price
        ax2.plot(historical_data['date'], historical_data['effective_price'],
                 'purple', linewidth=2, label='Historical Effective Price', alpha=0.8)

        # Current effective price prediction with confidence interval
        ax2.errorbar([today], [result['predicted_current_effective_price']],
                     yerr=[[result['predicted_current_effective_price'] - result[
                         'predicted_current_effective_price_lower']],
                           [result['predicted_current_effective_price_upper'] - result[
                               'predicted_current_effective_price']]],
                     fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                     label=f"Current: ${result['predicted_current_effective_price']:,.0f}")

        # Future effective price prediction with confidence interval
        ax2.errorbar([future_date], [result['predicted_future_effective_price']],
                     yerr=[[result['predicted_future_effective_price'] - result[
                         'predicted_future_effective_price_lower']],
                           [result['predicted_future_effective_price_upper'] - result[
                               'predicted_future_effective_price']]],
                     fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                     label=f"Future (1 year): ${result['predicted_future_effective_price']:,.0f}")

        ax2.set_title('Effective Price Analysis')
        ax2.set_ylabel('Effective Price ($)')
        ax2.set_xlabel('Date')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # Format x-axis
        for ax in [ax1, ax2]:
            ax.tick_params(axis='x', rotation=45)
            # Set reasonable x-axis limits to include future prediction
            min_date = pd.to_datetime(historical_data['date'].min())
            max_date = pd.to_datetime(max(future_date, historical_data['date'].max()))
            ax.set_xlim(min_date - pd.Timedelta(days=30), max_date + pd.Timedelta(days=30))

        plt.tight_layout()

        # Save the plot
        filename = building_dir / f"building_{building_id}_predictions.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Building graph saved: {filename}")

        # Print summary
        print(f"\nBuilding {building_id} Summary:")
        print(
            f"  Current Price: ${result['predicted_current_price']:,.0f} [{result['predicted_current_price_lower']:,.0f} - {result['predicted_current_price_upper']:,.0f}]")
        print(
            f"  Future Price: ${result['predicted_future_price']:,.0f} [{result['predicted_future_price_lower']:,.0f} - {result['predicted_future_price_upper']:,.0f}]")
        print(
            f"  Current Effective Price: ${result['predicted_current_effective_price']:,.0f} [{result['predicted_current_effective_price_lower']:,.0f} - {result['predicted_current_effective_price_upper']:,.0f}]")
        print(
            f"  Future Effective Price: ${result['predicted_future_effective_price']:,.0f} [{result['predicted_future_effective_price_lower']:,.0f} - {result['predicted_future_effective_price_upper']:,.0f}]")
        print(f"  Annual Growth: {result['average_percent_gain_per_year']:+.1f}%")

    def create_zillow_visualization(self, property_id: str, output_dir: str = "graphs"):
        """Create visualization for a Zillow property."""

        historical_data, analysis_data = self.get_zillow_data(property_id)

        if historical_data.empty or analysis_data.empty:
            logger.warning(f"No data found for Zillow property {property_id}")
            return

        # Ensure output directory exists
        zillow_dir = Path(output_dir) / "zillow_properties"
        zillow_dir.mkdir(parents=True, exist_ok=True)

        result = analysis_data.iloc[0]

        # Create single plot for Zillow (only has regular price)
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        fig.suptitle(f'Zillow Property {property_id} - Price Predictions with Confidence Intervals',
                     fontsize=14, fontweight='bold')

        # Convert dates
        historical_data['date'] = pd.to_datetime(historical_data['date'])
        today = pd.Timestamp.now().normalize()
        # Assume 365 days for future prediction (can be made configurable)
        future_date = today + pd.Timedelta(days=365)

        # Plot historical price
        ax.plot(historical_data['date'], historical_data['price'],
                'b-', linewidth=2, label='Historical Price', alpha=0.8)

        # Current prediction point with confidence interval
        ax.errorbar([today], [result['predicted_current_price']],
                    yerr=[[result['predicted_current_price'] - result['predicted_current_price_lower']],
                          [result['predicted_current_price_upper'] - result['predicted_current_price']]],
                    fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                    label=f"Current: ${result['predicted_current_price']:,.0f}")

        # Future prediction point with confidence interval
        ax.errorbar([future_date], [result['predicted_future_price']],
                    yerr=[[result['predicted_future_price'] - result['predicted_future_price_lower']],
                          [result['predicted_future_price_upper'] - result['predicted_future_price']]],
                    fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                    label=f"Future (1 year): ${result['predicted_future_price']:,.0f}")

        ax.set_title('Price Analysis')
        ax.set_ylabel('Price ($)')
        ax.set_xlabel('Date')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.tick_params(axis='x', rotation=45)

        # Set reasonable x-axis limits to include future prediction
        min_date = pd.to_datetime(historical_data['date'].min())
        max_date = pd.to_datetime(max(future_date, historical_data['date'].max()))
        ax.set_xlim(min_date - pd.Timedelta(days=30), max_date + pd.Timedelta(days=30))

        plt.tight_layout()

        # Save the plot
        filename = zillow_dir / f"zillow_{property_id}_predictions.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Zillow graph saved: {filename}")

        # Print summary
        print(f"\nZillow Property {property_id} Summary:")
        print(
            f"  Current Price: ${result['predicted_current_price']:,.0f} [{result['predicted_current_price_lower']:,.0f} - {result['predicted_current_price_upper']:,.0f}]")
        print(
            f"  Future Price: ${result['predicted_future_price']:,.0f} [{result['predicted_future_price_lower']:,.0f} - {result['predicted_future_price_upper']:,.0f}]")
        print(f"  Annual Growth: {result['average_percent_gain_per_year']:+.1f}%")

    def create_building_geographic_visualization(self, geography_type: str, geography_value: str,
                                                 output_dir: str = "graphs"):
        """Create visualization for a building geographic area with both price and effective price."""

        historical_data, analysis_data = self.get_building_geographic_data(geography_type, geography_value)

        if historical_data.empty or analysis_data.empty:
            logger.warning(f"No data found for building geographic area {geography_type}: {geography_value}")
            return

        # Ensure output directory exists
        building_geo_dir = Path(output_dir) / "building_geographic" / geography_type
        building_geo_dir.mkdir(parents=True, exist_ok=True)

        result = analysis_data.iloc[0]

        # Create figure with 2 subplots (price and effective price)
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle(f'{geography_type.title()}: {geography_value} - Building Market Analysis',
                     fontsize=14, fontweight='bold')

        # Convert dates
        historical_data['date'] = pd.to_datetime(historical_data['date'])
        today = pd.Timestamp.now().normalize()
        # Assume 365 days for future prediction (can be made configurable)
        future_date = today + pd.Timedelta(days=365)

        # Plot 1: Regular Price
        ax1.plot(historical_data['date'], historical_data['price'],
                 'b-', linewidth=2, label='Historical Avg Price', alpha=0.8)

        # Current prediction point with confidence interval
        ax1.errorbar([today], [result['predicted_current_price']],
                     yerr=[[result['predicted_current_price'] - result['predicted_current_price_lower']],
                           [result['predicted_current_price_upper'] - result['predicted_current_price']]],
                     fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                     label=f"Current: ${result['predicted_current_price']:,.0f}")

        # Future prediction point with confidence interval
        ax1.errorbar([future_date], [result['predicted_future_price']],
                     yerr=[[result['predicted_future_price'] - result['predicted_future_price_lower']],
                           [result['predicted_future_price_upper'] - result['predicted_future_price']]],
                     fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                     label=f"Future (1 year): ${result['predicted_future_price']:,.0f}")

        ax1.set_title(
            f'Average Rental Price - {result["total_units_in_geography"]} units, {result["total_buildings_in_geography"]} buildings')
        ax1.set_ylabel('Price ($)')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # Plot 2: Effective Price
        ax2.plot(historical_data['date'], historical_data['effective_price'],
                 'purple', linewidth=2, label='Historical Avg Effective Price', alpha=0.8)

        # Current effective price prediction with confidence interval
        ax2.errorbar([today], [result['predicted_current_effective_price']],
                     yerr=[[result['predicted_current_effective_price'] - result[
                         'predicted_current_effective_price_lower']],
                           [result['predicted_current_effective_price_upper'] - result[
                               'predicted_current_effective_price']]],
                     fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                     label=f"Current: ${result['predicted_current_effective_price']:,.0f}")

        # Future effective price prediction with confidence interval
        ax2.errorbar([future_date], [result['predicted_future_effective_price']],
                     yerr=[[result['predicted_future_effective_price'] - result[
                         'predicted_future_effective_price_lower']],
                           [result['predicted_future_effective_price_upper'] - result[
                               'predicted_future_effective_price']]],
                     fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                     label=f"Future (1 year): ${result['predicted_future_effective_price']:,.0f}")

        ax2.set_title('Average Effective Price Analysis')
        ax2.set_ylabel('Effective Price ($)')
        ax2.set_xlabel('Date')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # Format x-axis
        for ax in [ax1, ax2]:
            ax.tick_params(axis='x', rotation=45)
            # Set reasonable x-axis limits to include future prediction
            min_date = pd.to_datetime(historical_data['date'].min())
            max_date = pd.to_datetime(max(future_date, historical_data['date'].max()))
            ax.set_xlim(min_date - pd.Timedelta(days=30), max_date + pd.Timedelta(days=30))

        plt.tight_layout()

        # Save the plot
        safe_geo_value = str(geography_value).replace('/', '_').replace(' ', '_').replace(',', '')
        filename = building_geo_dir / f"{geography_type}_{safe_geo_value}_predictions.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Building geographic graph saved: {filename}")

    def create_zillow_geographic_visualization(self, geography_type: str, geography_value: str,
                                               output_dir: str = "graphs"):
        """Create visualization for a Zillow geographic area."""

        historical_data, analysis_data = self.get_zillow_geographic_data(geography_type, geography_value)

        if historical_data.empty or analysis_data.empty:
            logger.warning(f"No data found for Zillow geographic area {geography_type}: {geography_value}")
            return

        # Ensure output directory exists
        zillow_geo_dir = Path(output_dir) / "zillow_geographic" / geography_type
        zillow_geo_dir.mkdir(parents=True, exist_ok=True)

        result = analysis_data.iloc[0]

        # Create single plot for Zillow (only has regular price)
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        fig.suptitle(f'{geography_type.title()}: {geography_value} - Zillow Market Analysis',
                     fontsize=14, fontweight='bold')

        # Convert dates
        if not historical_data.empty:
            historical_data['date'] = pd.to_datetime(historical_data['date'])
        today = pd.Timestamp.now().normalize()
        # Assume 365 days for future prediction (can be made configurable)
        future_date = today + pd.Timedelta(days=365)

        # Plot historical price if available
        if not historical_data.empty:
            ax.plot(historical_data['date'], historical_data['price'],
                    'b-', linewidth=2, label='Historical Avg Price', alpha=0.8)

        # Current prediction point with confidence interval
        ax.errorbar([today], [result['predicted_current_price']],
                    yerr=[[result['predicted_current_price'] - result['predicted_current_price_lower']],
                          [result['predicted_current_price_upper'] - result['predicted_current_price']]],
                    fmt='o', color='orange', markersize=8, capsize=8, capthick=2,
                    label=f"Current: ${result['predicted_current_price']:,.0f}")

        # Future prediction point with confidence interval
        ax.errorbar([future_date], [result['predicted_future_price']],
                    yerr=[[result['predicted_future_price'] - result['predicted_future_price_lower']],
                          [result['predicted_future_price_upper'] - result['predicted_future_price']]],
                    fmt='o', color='red', markersize=8, capsize=8, capthick=2,
                    label=f"Future (1 year): ${result['predicted_future_price']:,.0f}")

        # Add market info to title
        market_info = f"{result['total_properties_in_geography']} properties"
        if result['median_property_value'] > 0:
            market_info += f", Median: ${result['median_property_value']:,.0f}"

        ax.set_title(f'Average Property Value - {market_info}')
        ax.set_ylabel('Price ($)')
        ax.set_xlabel('Date')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.tick_params(axis='x', rotation=45)

        # Set reasonable x-axis limits to include future prediction
        if not historical_data.empty:
            min_date = pd.to_datetime(historical_data['date'].min())
            max_date = pd.to_datetime(max(future_date, historical_data['date'].max()))
        else:
            min_date = today - pd.Timedelta(days=365)
            max_date = future_date
        ax.set_xlim(min_date - pd.Timedelta(days=30), max_date + pd.Timedelta(days=30))

        plt.tight_layout()

        # Save the plot
        safe_geo_value = str(geography_value).replace('/', '_').replace(' ', '_').replace(',', '')
        filename = zillow_geo_dir / f"{geography_type}_{safe_geo_value}_predictions.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Zillow geographic graph saved: {filename}")

        # Print summary
        print(f"\nZillow {geography_type.title()}: {geography_value} Summary:")
        print(f"  Market Size: {result['total_properties_in_geography']} properties")
        if result['median_property_value'] > 0:
            print(f"  Median Value: ${result['median_property_value']:,.0f}")
            print(f"  Avg Value: ${result['avg_property_value']:,.0f}")
        print(
            f"  Current Price: ${result['predicted_current_price']:,.0f} [{result['predicted_current_price_lower']:,.0f} - {result['predicted_current_price_upper']:,.0f}]")
        print(
            f"  Future Price: ${result['predicted_future_price']:,.0f} [{result['predicted_future_price_lower']:,.0f} - {result['predicted_future_price_upper']:,.0f}]")
        print(f"  Annual Growth: {result['average_percent_gain_per_year']:+.1f}%")

    def list_available_properties(self):
        """List all available properties and geographic areas for visualization."""

        # Get building IDs
        building_query = "SELECT DISTINCT building_id FROM building_analysis ORDER BY building_id"
        buildings = pd.read_sql(building_query, self.engine)

        # Get Zillow property IDs
        zillow_query = "SELECT DISTINCT id FROM zillow_analysis ORDER BY id"
        zillow_props = pd.read_sql(zillow_query, self.engine)

        # Get building geographic areas
        building_geo_query = """
        SELECT DISTINCT geography_type, geography_value, total_units_in_geography, total_buildings_in_geography
        FROM building_geographic_analysis 
        ORDER BY geography_type, total_units_in_geography DESC
        """
        building_geo = pd.read_sql(building_geo_query, self.engine)

        # Get Zillow geographic areas
        zillow_geo_query = """
        SELECT DISTINCT geography_type, geography_value, total_properties_in_geography, median_property_value
        FROM zillow_geographic_analysis 
        ORDER BY geography_type, total_properties_in_geography DESC
        """
        zillow_geo = pd.read_sql(zillow_geo_query, self.engine)

        print("\nAvailable Properties and Geographic Areas:")
        print("=" * 50)

        if not buildings.empty:
            print(f"Individual Buildings ({len(buildings)}):")
            for i, building_id in enumerate(buildings['building_id'][:10]):  # Show first 10
                print(f"  {building_id}")
            if len(buildings) > 10:
                print(f"  ... and {len(buildings) - 10} more")

        if not zillow_props.empty:
            print(f"\nIndividual Zillow Properties ({len(zillow_props)}):")
            for i, prop_id in enumerate(zillow_props['id'][:10]):  # Show first 10
                print(f"  {prop_id}")
            if len(zillow_props) > 10:
                print(f"  ... and {len(zillow_props) - 10} more")

        if not building_geo.empty:
            print(f"\nBuilding Geographic Areas ({len(building_geo)}):")
            for geo_type in ['state', 'city', 'msa', 'zip_code']:
                geo_areas = building_geo[building_geo['geography_type'] == geo_type]
                if not geo_areas.empty:
                    print(f"  {geo_type.title()}s ({len(geo_areas)}):")
                    for _, row in geo_areas.head(5).iterrows():
                        units = row['total_units_in_geography']
                        buildings = row['total_buildings_in_geography']
                        print(f"    {row['geography_value']} ({units} units, {buildings} buildings)")
                    if len(geo_areas) > 5:
                        print(f"    ... and {len(geo_areas) - 5} more")

        if not zillow_geo.empty:
            print(f"\nZillow Geographic Areas ({len(zillow_geo)}):")
            for geo_type in ['state', 'city', 'msa', 'zip_code']:
                geo_areas = zillow_geo[zillow_geo['geography_type'] == geo_type]
                if not geo_areas.empty:
                    print(f"  {geo_type.title()}s ({len(geo_areas)}):")
                    for _, row in geo_areas.head(5).iterrows():
                        props = row['total_properties_in_geography']
                        median_val = row['median_property_value']
                        if median_val > 0:
                            print(f"    {row['geography_value']} ({props} properties, median: ${median_val:,.0f})")
                        else:
                            print(f"    {row['geography_value']} ({props} properties)")
                    if len(geo_areas) > 5:
                        print(f"    ... and {len(geo_areas) - 5} more")

    def create_batch_visualizations(self, property_type: str = "both", limit: int = 5, output_dir: str = "graphs"):
        """Create visualizations for multiple properties and geographic areas."""

        if property_type in ["both", "building"]:
            # Individual buildings
            building_query = f"SELECT DISTINCT building_id FROM building_analysis ORDER BY building_id LIMIT {limit}"
            buildings = pd.read_sql(building_query, self.engine)

            logger.info(f"Creating visualizations for {len(buildings)} buildings...")
            for _, row in buildings.iterrows():
                try:
                    self.create_building_visualization(row['building_id'], output_dir)
                except Exception as e:
                    logger.error(f"Failed to create visualization for building {row['building_id']}: {e}")

        if property_type in ["both", "zillow"]:
            # Individual Zillow properties
            zillow_query = f"SELECT DISTINCT id FROM zillow_analysis ORDER BY id LIMIT {limit}"
            zillow_props = pd.read_sql(zillow_query, self.engine)

            logger.info(f"Creating visualizations for {len(zillow_props)} Zillow properties...")
            for _, row in zillow_props.iterrows():
                try:
                    self.create_zillow_visualization(row['id'], output_dir)
                except Exception as e:
                    logger.error(f"Failed to create visualization for Zillow property {row['id']}: {e}")

    def create_geographic_batch_visualizations(self, analysis_type: str = "both", geo_types: List[str] = None,
                                               limit_per_type: int = 3, output_dir: str = "graphs"):
        """Create visualizations for multiple geographic areas."""

        if geo_types is None:
            geo_types = ['state', 'city', 'msa']  # Skip zip_code by default as there are too many

        if analysis_type in ["both", "building"]:
            # Building geographic areas
            for geo_type in geo_types:
                building_geo_query = f"""
                SELECT DISTINCT geography_type, geography_value, total_units_in_geography
                FROM building_geographic_analysis 
                WHERE geography_type = '{geo_type}'
                ORDER BY total_units_in_geography DESC
                LIMIT {limit_per_type}
                """
                building_geos = pd.read_sql(building_geo_query, self.engine)

                logger.info(f"Creating building geographic visualizations for {len(building_geos)} {geo_type}s...")
                for _, row in building_geos.iterrows():
                    try:
                        self.create_building_geographic_visualization(
                            row['geography_type'], row['geography_value'], output_dir
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to create building geographic visualization for {geo_type} {row['geography_value']}: {e}")

        if analysis_type in ["both", "zillow"]:
            # Zillow geographic areas
            for geo_type in geo_types:
                zillow_geo_query = f"""
                SELECT DISTINCT geography_type, geography_value, total_properties_in_geography
                FROM zillow_geographic_analysis 
                WHERE geography_type = '{geo_type}'
                ORDER BY total_properties_in_geography DESC
                LIMIT {limit_per_type}
                """
                zillow_geos = pd.read_sql(zillow_geo_query, self.engine)

                logger.info(f"Creating Zillow geographic visualizations for {len(zillow_geos)} {geo_type}s...")
                for _, row in zillow_geos.iterrows():
                    try:
                        self.create_zillow_geographic_visualization(
                            row['geography_type'], row['geography_value'], output_dir
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to create Zillow geographic visualization for {geo_type} {row['geography_value']}: {e}")


@click.command()
@click.option('--property-type',
              type=click.Choice(['building', 'zillow', 'building-geo', 'zillow-geo', 'both', 'geo-both']),
              default='both', help='Type of analysis to visualize')
@click.option('--property-id', help='Specific property ID to visualize')
@click.option('--geography-type', type=click.Choice(['zip_code', 'city', 'state', 'msa']),
              help='Type of geographic area (use with --geography-value)')
@click.option('--geography-value', help='Specific geographic area to visualize (e.g., "California", "New York")')
@click.option('--list-properties', is_flag=True, help='List all available properties and geographic areas')
@click.option('--batch-limit', default=5, help='Number of individual properties to visualize in batch mode')
@click.option('--geo-limit', default=3, help='Number of geographic areas per type to visualize')
@click.option('--geo-types', default='state,city,msa', help='Comma-separated list of geographic types to include')
@click.option('--output-dir', default='graphs', help='Output directory for graphs')
def main(property_type, property_id, geography_type, geography_value, list_properties,
         batch_limit, geo_limit, geo_types, output_dir):
    """Create visualizations for property price predictions and geographic analysis."""

    try:
        visualizer = PropertyVisualizer(get_dbt_database_url())

        if list_properties:
            visualizer.list_available_properties()
            return

        # Parse geo_types
        geo_types_list = [gt.strip() for gt in geo_types.split(',')]

        # Handle specific property or geographic area
        if property_id:
            if property_type == 'building':
                visualizer.create_building_visualization(property_id, output_dir)
            elif property_type == 'zillow':
                visualizer.create_zillow_visualization(property_id, output_dir)
            else:
                # Try to determine type automatically
                try:
                    visualizer.create_building_visualization(property_id, output_dir)
                except:
                    visualizer.create_zillow_visualization(property_id, output_dir)

        elif geography_type and geography_value:
            if property_type in ['building-geo', 'both', 'geo-both']:
                visualizer.create_building_geographic_visualization(geography_type, geography_value, output_dir)
            if property_type in ['zillow-geo', 'both', 'geo-both']:
                visualizer.create_zillow_geographic_visualization(geography_type, geography_value, output_dir)

        else:
            # Batch mode
            if property_type == 'both':
                # Individual properties
                visualizer.create_batch_visualizations('both', batch_limit, output_dir)
                # Geographic areas
                visualizer.create_geographic_batch_visualizations('both', geo_types_list, geo_limit, output_dir)

            elif property_type == 'geo-both':
                # Only geographic areas
                visualizer.create_geographic_batch_visualizations('both', geo_types_list, geo_limit, output_dir)

            elif property_type in ['building', 'zillow']:
                # Individual properties only
                visualizer.create_batch_visualizations(property_type, batch_limit, output_dir)

            elif property_type == 'building-geo':
                # Building geographic only
                visualizer.create_geographic_batch_visualizations('building', geo_types_list, geo_limit, output_dir)

            elif property_type == 'zillow-geo':
                # Zillow geographic only
                visualizer.create_geographic_batch_visualizations('zillow', geo_types_list, geo_limit, output_dir)

        print(f"\nGraphs saved in: {output_dir}/")

    except Exception as e:
        logger.error(f"Visualization failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()