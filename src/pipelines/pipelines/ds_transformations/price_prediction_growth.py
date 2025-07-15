#!/usr/bin/env python3
"""
Complex price prediction and growth analysis for building-level data.
Calculates growth rates and ML predictions for price trends using moving averages.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_absolute_error
import warnings

warnings.filterwarnings('ignore')

from pipelines.pipelines.utils import get_local_data_connection

# CONFIGURATION: Change this to adjust the moving average window
MOVING_AVERAGE_MONTHS = 4  # Change this value to use different moving average periods

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_building_history_data():
    """Load building history data from the database."""
    logger.info("Loading building history data...")

    conn = get_local_data_connection()

    query = """
            SELECT building_id, \
                   week_date, \
                   avg_price, \
                   avg_effective_price, \
                   unit_count
            FROM avg_building_history
            WHERE avg_price IS NOT NULL
              AND avg_effective_price IS NOT NULL
            ORDER BY building_id, week_date \
            """

    df = pd.read_sql(query, conn)
    conn.close()

    # Convert week_date to datetime
    df['week_date'] = pd.to_datetime(df['week_date'])

    logger.info(f"Loaded {len(df)} records for {df['building_id'].nunique()} buildings")
    return df


def calculate_moving_averages(df):
    """Calculate moving averages for price columns."""
    df_sorted = df.sort_values('week_date').copy()

    # Calculate the average interval between data points
    avg_interval_days = calculate_days_between_periods(df_sorted)

    # Calculate moving average window in days
    ma_days = MOVING_AVERAGE_MONTHS * 30  # Convert months to approximate days
    window_periods = max(2, round(ma_days / avg_interval_days))

    logger.info(
        f"Using {window_periods} periods for {MOVING_AVERAGE_MONTHS}-month moving average (avg interval: {avg_interval_days:.1f} days)")

    # Calculate moving averages
    df_sorted[f'avg_price_{MOVING_AVERAGE_MONTHS}m_ma'] = df_sorted['avg_price'].rolling(
        window=window_periods, min_periods=max(1, window_periods // 2)
    ).mean()

    df_sorted[f'avg_effective_price_{MOVING_AVERAGE_MONTHS}m_ma'] = df_sorted['avg_effective_price'].rolling(
        window=window_periods, min_periods=max(1, window_periods // 2)
    ).mean()

    return df_sorted
    """Calculate the average number of days between data points."""
    df_sorted = df.sort_values('week_date')
    if len(df_sorted) < 2:
        return 7  # Default to weekly if insufficient data

    date_diffs = df_sorted['week_date'].diff().dt.days
    avg_days = date_diffs.mean()

    # Return the average, defaulting to 7 if calculation fails
    return avg_days if pd.notna(avg_days) else 7


def calculate_moving_averages(df):
    """Calculate moving averages for price columns."""
    df_sorted = df.sort_values('week_date').copy()

    # Calculate the average interval between data points
    avg_interval_days = calculate_days_between_periods(df_sorted)

    # Calculate moving average window in days
    ma_days = MOVING_AVERAGE_MONTHS * 30  # Convert months to approximate days
    window_periods = max(2, round(ma_days / avg_interval_days))

    logger.info(
        f"Using {window_periods} periods for {MOVING_AVERAGE_MONTHS}-month moving average (avg interval: {avg_interval_days:.1f} days)")

    # Calculate moving averages
    df_sorted[f'avg_price_{MOVING_AVERAGE_MONTHS}m_ma'] = df_sorted['avg_price'].rolling(
        window=window_periods, min_periods=max(1, window_periods // 2)
    ).mean()

    df_sorted[f'avg_effective_price_{MOVING_AVERAGE_MONTHS}m_ma'] = df_sorted['avg_effective_price'].rolling(
        window=window_periods, min_periods=max(1, window_periods // 2)
    ).mean()

    return df_sorted


def calculate_days_between_periods(df):
    """Calculate the average number of days between data points."""
    df_sorted = df.sort_values('week_date')
    if len(df_sorted) < 2:
        return 7  # Default to weekly if insufficient data

    date_diffs = df_sorted['week_date'].diff().dt.days
    avg_days = date_diffs.mean()

    # Return the average, defaulting to 7 if calculation fails
    return avg_days if pd.notna(avg_days) else 7


def calculate_simple_growth_rates(df, price_col, target_days):
    """Calculate simple average growth rates over a target period in days using moving averages."""
    df_sorted = df.sort_values('week_date').copy()

    # Use the moving average version of the price column
    ma_col = f'{price_col}_{MOVING_AVERAGE_MONTHS}m_ma'

    # Calculate the average interval between data points
    avg_interval_days = calculate_days_between_periods(df_sorted)

    # Calculate how many periods back we need to look to approximate target_days
    periods_back = max(1, round(target_days / avg_interval_days))

    # Calculate period-over-period growth using moving averages
    df_sorted[f'{ma_col}_lag'] = df_sorted[ma_col].shift(periods_back)

    # Calculate actual time difference for the lag
    df_sorted['date_lag'] = df_sorted['week_date'].shift(periods_back)
    df_sorted['actual_days_diff'] = (df_sorted['week_date'] - df_sorted['date_lag']).dt.days

    # Calculate growth rate as percentage and normalize to target period
    growth_rate = ((df_sorted[ma_col] - df_sorted[f'{ma_col}_lag']) / df_sorted[f'{ma_col}_lag'] * 100)

    # Normalize growth rate to the target period
    normalized_growth = growth_rate * (target_days / df_sorted['actual_days_diff'])

    # Return average growth rate, filtering out infinite/null values
    valid_growth = normalized_growth[np.isfinite(normalized_growth)]

    if len(valid_growth) == 0:
        return 0.0

    return valid_growth.mean()


def prepare_ml_features(df, price_col):
    """Prepare features for machine learning model using moving averages."""
    df_sorted = df.sort_values('week_date').copy()

    # Use the moving average version of the price column
    ma_col = f'{price_col}_{MOVING_AVERAGE_MONTHS}m_ma'

    # Calculate the average interval between data points
    avg_interval_days = calculate_days_between_periods(df_sorted)

    # Create time-based features
    df_sorted['week_number'] = df_sorted['week_date'].dt.isocalendar().week
    df_sorted['month'] = df_sorted['week_date'].dt.month
    df_sorted['quarter'] = df_sorted['week_date'].dt.quarter
    df_sorted['year'] = df_sorted['week_date'].dt.year

    # Create lag features based on moving averages
    lag_days = [30, 60, 90, 180, 270]  # 1, 2, 3, 6, 9 months

    for target_days in lag_days:
        periods_back = max(1, round(target_days / avg_interval_days))
        df_sorted[f'{ma_col}_lag_{target_days}d'] = df_sorted[ma_col].shift(periods_back)

    # Create additional moving averages of the main MA (longer-term trends)
    # Calculate windows based on multiples of the main MA window
    extended_ma_months = [MOVING_AVERAGE_MONTHS * 1.5, MOVING_AVERAGE_MONTHS * 3]  # 1.5x and 3x the main MA

    for months in extended_ma_months:
        ma_periods = max(2, round((months * 30) / avg_interval_days))
        df_sorted[f'{ma_col}_ma_{months:.1f}m'] = df_sorted[ma_col].rolling(
            window=ma_periods, min_periods=1
        ).mean()

    # Create growth rate features using moving averages
    df_sorted[f'{ma_col}_growth_recent'] = df_sorted[ma_col].pct_change(1) * 100

    # Calculate longer-term growth normalized to monthly rate
    monthly_lag = max(2, round(30 / avg_interval_days))
    if monthly_lag < len(df_sorted):
        df_sorted[f'{ma_col}_growth_monthly'] = df_sorted[ma_col].pct_change(monthly_lag) * 100
        # Normalize to 30-day rate
        actual_days = df_sorted['week_date'].diff(monthly_lag).dt.days
        df_sorted[f'{ma_col}_growth_monthly'] = df_sorted[f'{ma_col}_growth_monthly'] * (30 / actual_days)
    else:
        df_sorted[f'{ma_col}_growth_monthly'] = 0

    # Create volatility feature using moving averages
    volatility_periods = max(3, round(60 / avg_interval_days))
    df_sorted[f'{ma_col}_volatility'] = df_sorted[ma_col].rolling(
        window=volatility_periods, min_periods=2
    ).std()

    # Add interval information as a feature
    df_sorted['avg_interval_days'] = avg_interval_days

    return df_sorted


def train_prediction_model(df, price_col, target_days):
    """Train ML model to predict future price changes using moving averages."""
    df_features = prepare_ml_features(df, price_col)

    # Use the moving average version of the price column
    ma_col = f'{price_col}_{MOVING_AVERAGE_MONTHS}m_ma'

    # Calculate how many periods forward to predict based on target days
    avg_interval_days = calculate_days_between_periods(df)
    periods_forward = max(1, round(target_days / avg_interval_days))

    # Create target variable (future moving average price change)
    df_features['target'] = df_features[ma_col].shift(-periods_forward)

    # Create dynamic feature column names based on the MA window
    extended_mas = [MOVING_AVERAGE_MONTHS * 1.5, MOVING_AVERAGE_MONTHS * 3]

    # Define feature columns (updated for dynamic moving average naming scheme)
    feature_cols = [
        'week_number', 'month', 'quarter', 'unit_count',
        f'{ma_col}_lag_30d', f'{ma_col}_lag_60d', f'{ma_col}_lag_90d', f'{ma_col}_lag_180d', f'{ma_col}_lag_270d',
        f'{ma_col}_ma_{extended_mas[0]:.1f}m', f'{ma_col}_ma_{extended_mas[1]:.1f}m',
        f'{ma_col}_growth_recent', f'{ma_col}_growth_monthly',
        f'{ma_col}_volatility',
        'avg_interval_days'
    ]

    # Remove rows with missing values
    df_clean = df_features.dropna(subset=feature_cols + ['target'])

    if len(df_clean) < 10:  # Need minimum data for training
        return None, None, None

    X = df_clean[feature_cols]
    y = df_clean['target']

    # Train Random Forest model
    model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
    model.fit(X, y)

    # Calculate prediction and uncertainty estimates using cross-validation
    cv_scores = cross_val_score(model, X, y, cv=min(5, len(df_clean) // 2), scoring='neg_mean_absolute_error')

    # Get recent data for prediction
    recent_data = df_features.iloc[-1:][feature_cols]

    if recent_data.isna().any().any():
        return None, None, None

    # Make prediction
    prediction = model.predict(recent_data)[0]

    # Estimate uncertainty using cross-validation MAE
    mae = -cv_scores.mean()

    # Calculate confidence intervals (approximations)
    uncertainty_90 = mae * 1.645  # ~90% confidence interval
    uncertainty_60 = mae * 0.674  # ~60% confidence interval

    return prediction, uncertainty_90, uncertainty_60


def analyze_building(building_data):
    """Analyze a single building's price trends using configurable moving averages."""
    building_id = building_data['building_id'].iloc[0]
    logger.info(f"Analyzing building {building_id}")

    # Calculate moving averages first
    building_data_with_ma = calculate_moving_averages(building_data)

    results = {'building_id': building_id}

    # Define target periods in days
    monthly_days = 30
    yearly_days = 365

    for price_col in ['avg_price', 'avg_effective_price']:
        prefix = price_col.replace('avg_', '')
        ma_col = f'{price_col}_{MOVING_AVERAGE_MONTHS}m_ma'

        # Simple growth calculations using moving averages
        monthly_growth = calculate_simple_growth_rates(building_data_with_ma, price_col, monthly_days)
        yearly_growth = calculate_simple_growth_rates(building_data_with_ma, price_col, yearly_days)

        results[f'{prefix}_simple_monthly_growth_pct'] = monthly_growth
        results[f'{prefix}_simple_yearly_growth_pct'] = yearly_growth

        # ML predictions for monthly using moving averages
        ml_monthly_pred, ml_monthly_90, ml_monthly_60 = train_prediction_model(
            building_data_with_ma, price_col, monthly_days
        )

        if ml_monthly_pred is not None:
            # Convert to growth percentage using current moving average
            current_ma_price = building_data_with_ma[ma_col].iloc[-1]
            if pd.notna(current_ma_price) and current_ma_price > 0:
                monthly_growth_pred = ((ml_monthly_pred - current_ma_price) / current_ma_price) * 100
                monthly_90_pct = (ml_monthly_90 / current_ma_price) * 100
                monthly_60_pct = (ml_monthly_60 / current_ma_price) * 100
            else:
                monthly_growth_pred = None
                monthly_90_pct = None
                monthly_60_pct = None
        else:
            monthly_growth_pred = None
            monthly_90_pct = None
            monthly_60_pct = None

        results[f'{prefix}_ml_monthly_growth_pred_pct'] = monthly_growth_pred
        results[f'{prefix}_ml_monthly_90_uncertainty_pct'] = monthly_90_pct
        results[f'{prefix}_ml_monthly_60_uncertainty_pct'] = monthly_60_pct

        # ML predictions for yearly using moving averages
        ml_yearly_pred, ml_yearly_90, ml_yearly_60 = train_prediction_model(
            building_data_with_ma, price_col, yearly_days
        )

        if ml_yearly_pred is not None:
            # Convert to growth percentage using current moving average
            current_ma_price = building_data_with_ma[ma_col].iloc[-1]
            if pd.notna(current_ma_price) and current_ma_price > 0:
                yearly_growth_pred = ((ml_yearly_pred - current_ma_price) / current_ma_price) * 100
                yearly_90_pct = (ml_yearly_90 / current_ma_price) * 100
                yearly_60_pct = (ml_yearly_60 / current_ma_price) * 100
            else:
                yearly_growth_pred = None
                yearly_90_pct = None
                yearly_60_pct = None
        else:
            yearly_growth_pred = None
            yearly_90_pct = None
            yearly_60_pct = None

        results[f'{prefix}_ml_yearly_growth_pred_pct'] = yearly_growth_pred
        results[f'{prefix}_ml_yearly_90_uncertainty_pct'] = yearly_90_pct
        results[f'{prefix}_ml_yearly_60_uncertainty_pct'] = yearly_60_pct

    return results


def create_predictions_table():
    """Main function to create price predictions table."""
    # Load data
    df = load_building_history_data()

    if df.empty:
        logger.error("No data found in avg_building_history table")
        return False

    # Analyze each building
    results = []
    buildings = df['building_id'].unique()

    for i, building_id in enumerate(buildings, 1):
        logger.info(f"Processing building {i}/{len(buildings)}: {building_id}")

        building_data = df[df['building_id'] == building_id].copy()

        # Skip buildings with insufficient data (need enough for meaningful time-based analysis)
        if len(building_data) < 8:  # Need at least 8 data points for time series analysis
            logger.warning(f"Skipping building {building_id} - insufficient data ({len(building_data)} periods)")
            continue

        try:
            building_results = analyze_building(building_data)
            results.append(building_results)
        except Exception as e:
            logger.error(f"Error analyzing building {building_id}: {e}")
            continue

    if not results:
        logger.error("No buildings could be analyzed")
        return False

    # Create results DataFrame
    results_df = pd.DataFrame(results)

    # Add metadata
    results_df['analysis_date'] = datetime.now()
    results_df['total_buildings_analyzed'] = len(results)

    logger.info(f"Analysis complete for {len(results)} buildings")

    # Upload to database
    conn = get_local_data_connection()

    # Drop existing table and create new one
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS price_prediction_growth")
    conn.commit()

    # Upload data
    results_df.to_sql(
        'price_prediction_growth',
        conn,
        if_exists='replace',
        index=False,
        method='multi'
    )

    logger.info(f"Uploaded {len(results_df)} records to price_prediction_growth table")

    conn.close()
    return True


def main():
    """Main function."""
    logger.info("Starting price prediction and growth analysis...")

    success = create_predictions_table()

    if success:
        logger.info("Price prediction analysis completed successfully!")
    else:
        logger.error("Price prediction analysis failed!")
        exit(1)


if __name__ == "__main__":
    main()