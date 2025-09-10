# scripts/05_price_prediction_growth.py
"""
Advanced Price Forecasting Pipeline for Rental and Real Estate Properties

Unified pipeline for building rental prices and Zillow property predictions
with automated ML model selection and database integration.
"""

import os
import sys
import click
import logging
import warnings
import json
from datetime import datetime, timedelta, date as datetime_date
from sklearn.linear_model import LinearRegression
from typing import List, Tuple, Dict, Optional
from scipy import stats

import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from sqlalchemy import create_engine, text

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, ExtraTreesRegressor
from sklearn.linear_model import Ridge, ElasticNet, LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from prophet import Prophet

from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_dbt_database_url():
    """Extract database URL from dbt profiles."""

    # Try to find profiles.yml
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

        # Get the profile name (usually matches your project name)
        profile_name = 'my_real_estate_project'  # Update this to match your profile name
        target = 'dev'  # or whatever your default target is

        if profile_name in profiles:
            profile = profiles[profile_name]
            if 'outputs' in profile and target in profile['outputs']:
                output = profile['outputs'][target]

                # Build connection string
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


def calculate_prediction_interval(X, y, X_pred, model, confidence_level=0.95):
    """Calculate prediction intervals using regression statistics (most statistically sound method)."""
    n = len(y)
    if n <= 2:
        return np.zeros(len(X_pred)), np.zeros(len(X_pred))

    # Calculate residuals and MSE
    y_pred_train = model.predict(X)
    residuals = y - y_pred_train
    mse = np.mean(residuals ** 2)  # Use MSE, not sum of squares

    # Degrees of freedom
    p = X.shape[1] + 1  # number of parameters (including intercept)
    dof = max(n - p, 1)

    # Standard error of prediction
    s_err = np.sqrt(mse)

    # Calculate prediction standard error for each prediction point
    X_mean = np.mean(X, axis=0)
    X_centered = X - X_mean

    # Get predictions
    y_pred = model.predict(X_pred)

    # Calculate standard errors for each prediction
    se_pred = []
    for x_new in X_pred:
        X_new_centered = x_new - X_mean

        # Calculate leverage (hat matrix diagonal)
        try:
            XtX_inv = np.linalg.inv(X_centered.T @ X_centered)
            leverage = 1 + X_new_centered @ XtX_inv @ X_new_centered.T
        except:
            leverage = 1.1  # Fallback for singular matrices

        # Standard error of prediction
        se = s_err * np.sqrt(leverage)
        se_pred.append(se)

    se_pred = np.array(se_pred)

    # t-critical value for confidence interval
    alpha = 1 - confidence_level
    t_crit = stats.t.ppf(1 - alpha / 2, dof)

    # Calculate confidence bounds
    margin = t_crit * se_pred
    lower_bound = y_pred - margin
    upper_bound = y_pred + margin

    return lower_bound, upper_bound


def calculate_price_predictions(df, price_column, forecast_days, confidence_level=0.95):
    """Calculate price predictions with confidence intervals for a given price column."""
    try:
        # Prepare data for linear regression
        df_clean = df.dropna(subset=[price_column])
        df_clean = df_clean[
            (df_clean[price_column] > 0) &
            (np.isfinite(df_clean[price_column]))
            ]

        if len(df_clean) < 2:
            return {
                'predicted_current': 0, 'predicted_future': 0,
                'current_lower': 0, 'current_upper': 0,
                'future_lower': 0, 'future_upper': 0
            }

        # Calculate days since start for regression
        earliest_date = df_clean['date'].min()
        df_clean = df_clean.copy()
        df_clean['days_since_start'] = (df_clean['date'] - earliest_date).dt.days

        # Today's date and future date for predictions
        today = pd.Timestamp.now().normalize()
        future_date = today + pd.Timedelta(days=forecast_days)
        days_to_today = (today - earliest_date).days
        days_to_future = (future_date - earliest_date).days

        # Train linear regression model
        model = LinearRegression()
        X_train = df_clean[['days_since_start']].values
        y_train = df_clean[price_column].values
        model.fit(X_train, y_train)

        # Prediction points
        X_pred = np.array([[days_to_today], [days_to_future]])

        # Current and future price predictions
        predictions = model.predict(X_pred)
        predicted_current = predictions[0]
        predicted_future = predictions[1]

        # Calculate confidence intervals
        lower_bounds, upper_bounds = calculate_prediction_interval(
            X_train, y_train, X_pred, model, confidence_level
        )

        return {
            'predicted_current': predicted_current,
            'predicted_future': predicted_future,
            'current_lower': lower_bounds[0],
            'current_upper': upper_bounds[0],
            'future_lower': lower_bounds[1],
            'future_upper': upper_bounds[1]
        }

    except Exception as e:
        # Return latest known price if prediction fails
        latest_price = df[price_column].iloc[-1] if len(df) > 0 else 0
        return {
            'predicted_current': latest_price,
            'predicted_future': latest_price,
            'current_lower': latest_price,
            'current_upper': latest_price,
            'future_lower': latest_price,
            'future_upper': latest_price
        }


def calculate_trend_metrics(price_series, cagr, time_span_years):
    """Calculate trend strength and variance metrics."""
    if len(price_series) < 3:
        return 0.0, 0.0

    # Calculate period-over-period changes
    price_changes = price_series.pct_change().dropna()

    if len(price_changes) == 0:
        return 0.0, 0.0

    # Calculate expected periodic growth rate from CAGR
    periods_per_year = len(price_series) / time_span_years if time_span_years > 0 else 1
    expected_periodic_growth = (1 + cagr / 100) ** (1 / periods_per_year) - 1

    # Calculate trend strength as percentage of moves in same direction as overall trend
    overall_direction = 1 if cagr > 0 else -1
    consistent_moves = (np.sign(price_changes) == overall_direction).sum()
    trend_strength = (consistent_moves / len(price_changes)) * 100

    # Calculate variance from expected trend
    deviations = price_changes - expected_periodic_growth
    trend_variance = np.std(deviations) * 100

    return trend_strength, trend_variance


class UnifiedPriceForecastingPipeline:
    """
    Complete price forecasting pipeline for both rental buildings and Zillow properties.
    Handles data loading, ML model optimization, predictions, and database uploads.
    """

    def __init__(self, random_state: int = 42, db_url: str = None):
        self.random_state = random_state
        self.initial_param_iterations = 8
        self.intensive_param_iterations = 30
        self.db_url = db_url
        self.engine = None

        # Initialize database connection
        if not self.db_url:
            # Try to get from environment
            self.db_url = os.environ.get('DATABASE_URL')

            # If still not found, try dbt profiles
            if not self.db_url:
                self.db_url = get_dbt_database_url()
                if self.db_url:
                    logger.info("Using database URL from dbt profiles")

            if not self.db_url:
                logger.error(
                    "No database URL provided. Please set DATABASE_URL environment variable, use --db-url, or configure dbt profiles.")
                sys.exit(1)

        self.engine = create_engine(self.db_url)

        self.models = {
            'RandomForest': RandomForestRegressor(random_state=random_state, n_jobs=-1),
            'GradientBoosting': GradientBoostingRegressor(random_state=random_state),
            'ExtraTrees': ExtraTreesRegressor(random_state=random_state, n_jobs=-1),
            'Ridge': Ridge(random_state=random_state),
            'ElasticNet': ElasticNet(random_state=random_state),
            'KNeighbors': KNeighborsRegressor(n_jobs=-1),
            'DecisionTree': DecisionTreeRegressor(random_state=random_state)
        }

    def load_building_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load rental building data from database."""

        building_history = pd.read_sql("""
                                       SELECT building_id, price, effective_price, date
                                       FROM units_history_grouped_by_building
                                       ORDER BY building_id, date
                                       """, self.engine)

        return building_history

    def load_zillow_data(self, forecast_days: int = 365, confidence_level: float = 0.95) -> pd.DataFrame:
        """Load and process Zillow property data - SIMPLIFIED VERSION using dbt models."""

        # Use the new dbt model instead of raw JSON parsing
        zillow_query = """
                       SELECT zh.id, \
                              zh.date, \
                              zh.price, \
                              zh.price_change_rate, \
                              z.url
                       FROM zillow_history zh
                                JOIN stg_zillow z ON zh.id = z.id
                       ORDER BY zh.id, zh.date \
                       """

        zillow_data = pd.read_sql(zillow_query, self.engine)
        initial_count = len(zillow_data['id'].unique())
        logger.info(f"Loaded {initial_count} Zillow properties from database")

        if zillow_data.empty:
            return pd.DataFrame()

        # Group by property and calculate metrics
        results = []

        for property_id, property_data in zillow_data.groupby('id'):
            try:
                # Data is already cleaned by dbt model
                df = property_data.sort_values('date').copy()

                if len(df) < 2:
                    continue

                # Basic metrics
                min_date = df['date'].min()
                max_date = df['date'].max()
                latest_price = df['price'].iloc[-1]
                earliest_price = df['price'].iloc[0]
                span_days = (max_date - min_date).days

                # Calculate CAGR
                time_span_years = span_days / 365.25
                if time_span_years > 0 and earliest_price > 0:
                    cagr = ((latest_price / earliest_price) ** (1 / time_span_years) - 1) * 100
                else:
                    cagr = 0.0

                # Calculate price predictions using shared function
                price_results = calculate_price_predictions(df, 'price', forecast_days, confidence_level)

                # Calculate trend metrics
                trend_strength, trend_variance = calculate_trend_metrics(df['price'], cagr, time_span_years)

                results.append({
                    'id': property_id,
                    'latest_price': latest_price,
                    'predicted_current_price': round(price_results['predicted_current'], 2),
                    'predicted_current_price_lower': round(price_results['current_lower'], 2),
                    'predicted_current_price_upper': round(price_results['current_upper'], 2),
                    'predicted_future_price': round(price_results['predicted_future'], 2),
                    'predicted_future_price_lower': round(price_results['future_lower'], 2),
                    'predicted_future_price_upper': round(price_results['future_upper'], 2),
                    'average_percent_gain_per_year': round(cagr, 4),
                    'trend_strength_pct': round(trend_strength, 4),
                    'trend_variance_pct': round(trend_variance, 4),
                    'latest_date': max_date,
                    'min_date': min_date,
                    'max_date': max_date,
                    'data_span_days': span_days,
                    'url': df['url'].iloc[0]
                })

            except Exception as e:
                logger.warning(f"Error processing property {property_id}: {e}")
                continue

        if not results:
            logger.warning("No valid Zillow properties processed")
            return pd.DataFrame()

        # Create DataFrame and filter recent data
        processed_df = pd.DataFrame(results)
        processed_df['latest_date'] = pd.to_datetime(processed_df['latest_date'])
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=7 * 30)
        recent_data = processed_df[processed_df['latest_date'] >= cutoff_date].copy()

        if len(recent_data) == 0:
            logger.warning("No Zillow properties with recent data (last 7 months)")
            return pd.DataFrame()

        # Add remaining metadata columns
        recent_data['prediction_date'] = pd.Timestamp.now().normalize()

        logger.info(f"Final processed Zillow dataset: {len(recent_data)} properties")
        return recent_data

    def _predict_zillow_price(self, price_history: List[Dict], days_ahead: int) -> Optional[Dict]:
        """Predict future price using linear regression on historical data."""
        if not price_history or len(price_history) < 2:
            return None

        try:
            df = pd.DataFrame(price_history)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').dropna(subset=['price'])
            df = df[(df['price'] > 0) & (np.isfinite(df['price']))]

            if len(df) < 2:
                return None

            # Linear regression setup
            earliest_date = df['date'].min()
            df['days_since_start'] = (df['date'] - earliest_date).dt.days

            model = LinearRegression()
            model.fit(df[['days_since_start']], df['price'])

            # Prediction
            latest_date = df['date'].max()
            future_date = latest_date + timedelta(days=days_ahead)
            days_to_predict = (future_date - earliest_date).days
            predicted_price = model.predict([[days_to_predict]])[0]

            latest_price = df['price'].iloc[-1]
            price_change_pct = ((predicted_price - latest_price) / latest_price) * 100

            return {
                'latest_price': latest_price,
                'predicted_price': round(predicted_price, 2),
                'predicted_change_pct': round(price_change_pct, 2),
                'latest_date': latest_date,
                'prediction_date': future_date
            }
        except Exception:
            return None

    def _predict_zillow_current_price(self, price_history: List[Dict]) -> Optional[float]:
        """Predict current price for today's date based on historical trends."""
        if not price_history or len(price_history) < 2:
            return None

        try:
            df = pd.DataFrame(price_history)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').dropna(subset=['price'])
            df = df[(df['price'] > 0) & (np.isfinite(df['price']))]

            if len(df) < 2:
                return None

            # Use all available historical data for training
            earliest_date = df['date'].min()
            df['days_since_start'] = (df['date'] - earliest_date).dt.days

            model = LinearRegression()
            model.fit(df[['days_since_start']], df['price'])

            # Predict for today's date
            today = pd.Timestamp.now().normalize()  # Get today's date at midnight
            days_to_today = (today - earliest_date).days
            predicted_current_price = model.predict([[days_to_today]])[0]

            return round(predicted_current_price, 2)
        except Exception:
            return None

    def process_building_data(self, building_history: pd.DataFrame) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
        """Process and prepare building data for ML pipeline with outlier removal."""
        # Ensure date column is datetime
        building_history = building_history.copy()
        building_history['date'] = pd.to_datetime(building_history['date'])

        # Group by building and filter for sufficient data
        filtered_data = building_history.groupby('building_id').filter(lambda x: len(x) >= 8)
        building_list = [group for _, group in filtered_data.groupby('building_id')]

        if not building_list:
            return pd.DataFrame(), []

        # Combine and process
        processed = pd.concat(building_list, ignore_index=True)
        # Ensure date is datetime after concatenation
        processed['date'] = pd.to_datetime(processed['date'])


        # Remove outliers using IQR method for each building separately
        def remove_outliers_iqr(group, columns=['price', 'effective_price'], multiplier=1.5):
            """Remove outliers using the IQR method for specified columns."""
            group_clean = group.copy()

            for col in columns:
                if col in group_clean.columns and not group_clean[col].isna().all():
                    Q1 = group_clean[col].quantile(0.25)
                    Q3 = group_clean[col].quantile(0.75)
                    IQR = Q3 - Q1

                    # Define outlier bounds
                    lower_bound = Q1 - multiplier * IQR
                    upper_bound = Q3 + multiplier * IQR

                    # Remove outliers
                    group_clean = group_clean[
                        (group_clean[col] >= lower_bound) &
                        (group_clean[col] <= upper_bound)
                        ]

            return group_clean

        # Apply outlier removal per building
        processed = processed.groupby('building_id').apply(
            lambda x: remove_outliers_iqr(x)
        ).reset_index(drop=True)

        # Re-filter buildings to ensure they still have sufficient data after outlier removal
        processed = processed.groupby('building_id').filter(lambda x: len(x) >= 8)

        # Handle missing values
        processed = processed.sort_values(['building_id', 'date']).reset_index(drop=True)
        for col in ['price', 'effective_price']:
            if col in processed.columns:
                processed[col] = processed.groupby('building_id')[col].transform(
                    lambda x: x.interpolate(method='linear', limit=2)
                )
        processed = processed.dropna(subset=['price', 'effective_price'])

        # Update building_list to reflect outlier removal
        updated_building_list = [group for _, group in processed.groupby('building_id')]

        return processed, updated_building_list

    def engineer_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create comprehensive features for ML models using vectorized operations."""
        feature_data = data.copy().sort_values(['building_id', 'date']).reset_index(drop=True)

        # Ensure date column is datetime
        feature_data['date'] = pd.to_datetime(feature_data['date'])

        # Vectorized time series features using groupby transform
        grouped = feature_data.groupby('building_id')

        # Rolling features - even more optimized with single transform call
        def compute_rolling_features(series):
            """Compute all rolling features in one pass."""
            result = pd.DataFrame(index=series.index)
            for window in [4, 8, 12]:
                rolling = series.rolling(window, min_periods=1)
                result[f'price_rolling_mean_{window}w'] = rolling.mean()
                result[f'price_rolling_std_{window}w'] = rolling.std()
            return result

        # Apply rolling features
        rolling_features = grouped['price'].apply(compute_rolling_features).reset_index(level=0, drop=True)
        feature_data = pd.concat([feature_data, rolling_features], axis=1)

        # Price changes - vectorized
        feature_data['price_change_1w'] = grouped['price'].diff(1)
        feature_data['price_change_4w'] = grouped['price'].diff(4)

        # Fill NaN values
        feature_cols = [col for col in feature_data.columns
                        if col not in ['date', 'building_id', 'price', 'effective_price']]
        feature_data[feature_cols] = feature_data[feature_cols].fillna(0)

        return feature_data

    def _get_param_grid(self, model_name: str) -> Dict:
        """Get parameter grid for model optimization."""
        grids = {
            'RandomForestRegressor': {
                'n_estimators': [50, 75, 100, 150, 200, 250, 300],
                'max_depth': [3, 5, 7, 10, 15, 20, None],
                'min_samples_split': [2, 3, 5, 7, 10],
                'min_samples_leaf': [1, 2, 3, 4, 5],
                'max_features': ['sqrt', 'log2', 0.3, 0.5, 0.7, None]
            },
            'GradientBoostingRegressor': {
                'n_estimators': [50, 75, 100, 150, 200, 250, 300],
                'learning_rate': [0.01, 0.05, 0.1, 0.15, 0.2, 0.25],
                'max_depth': [2, 3, 4, 5, 6, 7, 8],
                'min_samples_split': [2, 3, 5, 7, 10],
                'subsample': [0.7, 0.8, 0.85, 0.9, 0.95, 1.0]
            },
            'ExtraTreesRegressor': {
                'n_estimators': [50, 75, 100, 150, 200, 250, 300],
                'max_depth': [3, 5, 7, 10, 15, 20, None],
                'min_samples_split': [2, 3, 5, 7, 10],
                'min_samples_leaf': [1, 2, 3, 4, 5],
                'max_features': ['sqrt', 'log2', 0.3, 0.5, 0.7, None]
            },
            'Ridge': {
                'alpha': [0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
                'solver': ['auto', 'svd', 'cholesky', 'lsqr', 'sparse_cg'],
                'fit_intercept': [True, False]
            },
            'ElasticNet': {
                'alpha': [0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0],
                'l1_ratio': [0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 0.95],
                'fit_intercept': [True, False],
                'max_iter': [1000, 2000, 3000, 5000]
            },
            'KNeighborsRegressor': {
                'n_neighbors': [2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20],
                'weights': ['uniform', 'distance'],
                'algorithm': ['auto', 'ball_tree', 'kd_tree'],
                'leaf_size': [10, 20, 30, 40, 50]
            },
            'DecisionTreeRegressor': {
                'max_depth': [2, 3, 4, 5, 6, 7, 8, 10, 12, 15, None],
                'min_samples_split': [2, 3, 4, 5, 7, 10, 15, 20],
                'min_samples_leaf': [1, 2, 3, 4, 5, 7, 10],
                'criterion': ['squared_error', 'friedman_mse'],
                'splitter': ['best', 'random']
            }
        }
        return grids.get(model_name, {})

    def generate_building_predictions_to_date(self, feature_data: pd.DataFrame, models: Dict,
                                              target_dates: List[datetime.date]) -> pd.DataFrame:
        """Generate price predictions for all buildings at a specific target date using Prophet."""

        # Unpack the models tuple
        trained_models, effective_price_models, feature_cols = models

        predictions = []

        # Convert target_date to pandas Timestamp for Prophet
        target_timestamp = [pd.Timestamp(el) for el in target_dates]

        # Get unique building IDs
        building_ids = feature_data['building_id'].unique()

        for building_id in building_ids:
            # Check if we have trained models for this building
            if building_id in trained_models and building_id in effective_price_models:
                try:
                    # Create future dataframe for the target date
                    future_df = pd.DataFrame({'ds': target_timestamp})

                    # Make predictions
                    price_model = trained_models[building_id]
                    effective_price_model = effective_price_models[building_id]

                    price_forecast = price_model.predict(future_df)
                    effective_price_forecast = effective_price_model.predict(future_df)

                    for i in range(len(target_timestamp)):
                        predictions.append(
                            {
                                'date': target_timestamp[i],
                                'building_id': building_id,
                                'predicted_price': price_forecast['yhat'].iloc[i],
                                'predicted_effective_price': effective_price_forecast['yhat'].iloc[i],
                                'price_lower_bound': price_forecast['yhat_lower'].iloc[i],
                                'price_upper_bound': price_forecast['yhat_upper'].iloc[i],
                                'effective_price_lower_bound': effective_price_forecast['yhat_lower'].iloc[i],
                                'effective_price_upper_bound': effective_price_forecast['yhat_upper'].iloc[i]
                            }
                        )
                except Exception as e:
                    print(f"Error predicting for building {building_id}: {e}")
                    continue

        return pd.DataFrame(predictions)

    def generate_building_predictions(self, feature_data: pd.DataFrame, models: Dict,
                                      forecast_days: int) -> pd.DataFrame:
        """Generate future predictions for buildings using vectorized operations."""
        forecast_weeks = int(np.ceil(forecast_days / 7))

        # Get the latest row for each building
        latest_rows = feature_data.groupby('building_id').last().reset_index()
        n_buildings = len(latest_rows)

        # Create arrays for broadcasting (much faster than DataFrame operations)
        weeks_array = np.arange(1, forecast_weeks + 1)
        building_ids = np.repeat(latest_rows['building_id'].values, forecast_weeks)
        weeks_ahead = np.tile(weeks_array, n_buildings)

        # Replicate latest rows for each forecast week
        prediction_indices = np.repeat(np.arange(n_buildings), forecast_weeks)
        all_prediction_data = latest_rows.iloc[prediction_indices].copy()
        all_prediction_data = all_prediction_data.reset_index(drop=True)

        # Update temporal features using numpy operations (faster)
        base_dates = all_prediction_data['date'].values
        weeks_to_add = pd.to_timedelta(weeks_ahead, unit='W')
        future_dates = pd.to_datetime(base_dates) + weeks_to_add

        all_prediction_data['date'] = future_dates
        all_prediction_data['year'] = future_dates.year
        all_prediction_data['month'] = future_dates.month
        all_prediction_data['quarter'] = (future_dates.month - 1) // 3 + 1
        all_prediction_data['weeks_ahead'] = weeks_ahead

        # Handle categorical features efficiently
        feature_columns = models['feature_columns']
        missing_cols = set(feature_columns) - set(all_prediction_data.columns)
        if missing_cols:
            # Add missing dummy variables as zeros
            for col in missing_cols:
                all_prediction_data[col] = 0

        # Get prediction features
        X_pred = all_prediction_data[feature_columns]

        # Batch predictions
        price_predictions = models['price_model'].predict(X_pred)
        effective_price_predictions = models['effective_price_model'].predict(X_pred)

        # Create final results DataFrame
        predictions_df = pd.DataFrame({
            'building_id': building_ids,
            'weeks_ahead': weeks_ahead,
            'predicted_price': price_predictions,
            'predicted_effective_price': effective_price_predictions
        })

        return predictions_df

    def generate_current_price_predictions(self, feature_data: pd.DataFrame, models: Dict) -> pd.DataFrame:
        """Generate predictions for today's date for buildings."""
        # Get the latest row for each building as baseline
        latest_rows = feature_data.groupby('building_id').last().reset_index()

        # Create prediction data for today's date
        today = pd.Timestamp.now().normalize()  # Get today's date at midnight
        current_prediction_data = latest_rows.copy()

        # Update date to today and recalculate time-based features
        current_prediction_data['date'] = today
        current_prediction_data['year'] = today.year
        current_prediction_data['month'] = today.month
        current_prediction_data['quarter'] = (today.month - 1) // 3 + 1

        # Update season based on today's month
        season_map = {
            12: 'winter', 1: 'winter', 2: 'winter',
            3: 'spring', 4: 'spring', 5: 'spring',
            6: 'summer', 7: 'summer', 8: 'summer',
            9: 'fall', 10: 'fall', 11: 'fall'
        }
        current_prediction_data['season'] = today.month
        # Handle season encoding - set all season columns to 0 first
        season_cols = [col for col in current_prediction_data.columns if col.startswith('season_')]
        for col in season_cols:
            current_prediction_data[col] = 0
        # Set the correct season column to 1
        current_season = season_map[today.month]
        season_col = f'season_{current_season}'
        if season_col in current_prediction_data.columns:
            current_prediction_data[season_col] = 1

        # Handle categorical features efficiently
        feature_columns = models['feature_columns']
        missing_cols = set(feature_columns) - set(current_prediction_data.columns)
        if missing_cols:
            # Add missing dummy variables as zeros
            for col in missing_cols:
                current_prediction_data[col] = 0

        # Get prediction features for current state
        X_current = current_prediction_data[feature_columns]

        # Generate current price predictions for today
        predicted_current_price = models['price_model'].predict(X_current)
        predicted_current_effective_price = models['effective_price_model'].predict(X_current)

        # Create results DataFrame
        current_predictions_df = pd.DataFrame({
            'building_id': current_prediction_data['building_id'],
            'predicted_current_price': predicted_current_price,
            'predicted_current_effective_price': predicted_current_effective_price,
            'prediction_date': today
        })

        return current_predictions_df

    def calculate_growth_metrics(self, processed_data: pd.DataFrame, forecast_days: int = 365,
                                 confidence_level: float = 0.95) -> pd.DataFrame:
        """Calculate historical growth metrics for buildings using vectorized operations with price predictions and confidence intervals."""

        # Sort data once
        sorted_data = processed_data.sort_values(['building_id', 'date'])

        # Get first and last rows for each building using groupby
        first_rows = sorted_data.groupby('building_id').first()
        last_rows = sorted_data.groupby('building_id').last()

        # Get min and max dates for each building
        date_ranges = sorted_data.groupby('building_id')['date'].agg(['min', 'max']).reset_index()
        date_ranges.columns = ['building_id', 'min_date', 'max_date']

        # Count rows per building to filter out buildings with < 2 observations
        building_counts = sorted_data.groupby('building_id').size()
        valid_buildings = building_counts[building_counts >= 2].index

        # Filter to only valid buildings
        first_rows = first_rows.loc[valid_buildings]
        last_rows = last_rows.loc[valid_buildings]

        # Calculate time spans vectorized (in years)
        time_spans_days = (last_rows['date'] - first_rows['date']).dt.days
        time_span_years = time_spans_days / 365.25

        # Get price values
        earliest_prices = first_rows['price']
        earliest_effective_prices = first_rows['effective_price']
        latest_prices = last_rows['price']
        latest_effective_prices = last_rows['effective_price']

        # Calculate CAGR vectorized for both price types
        valid_mask = (time_span_years > 0) & (earliest_prices > 0)
        valid_effective_mask = (time_span_years > 0) & (earliest_effective_prices > 0)

        # Initialize CAGR arrays with zeros
        price_cagr = np.zeros(len(valid_buildings))
        effective_price_cagr = np.zeros(len(valid_buildings))

        # Calculate CAGR only for valid entries
        if valid_mask.any():
            price_ratios = latest_prices[valid_mask] / earliest_prices[valid_mask]
            time_powers = 1 / time_span_years[valid_mask]
            price_cagr[valid_mask] = ((price_ratios ** time_powers) - 1) * 100

        if valid_effective_mask.any():
            effective_ratios = latest_effective_prices[valid_effective_mask] / earliest_effective_prices[
                valid_effective_mask]
            time_powers_eff = 1 / time_span_years[valid_effective_mask]
            effective_price_cagr[valid_effective_mask] = ((effective_ratios ** time_powers_eff) - 1) * 100

        # Initialize result arrays
        results = []

        for i, building_id in enumerate(valid_buildings):
            building_data = sorted_data[sorted_data['building_id'] == building_id].copy()

            # Calculate price predictions for regular price
            price_results = calculate_price_predictions(
                building_data, 'price', forecast_days, confidence_level
            )

            # Calculate price predictions for effective price
            effective_results = calculate_price_predictions(
                building_data, 'effective_price', forecast_days, confidence_level
            )

            # Calculate trend metrics
            trend_strength, trend_variance = calculate_trend_metrics(
                building_data['price'], price_cagr[i], time_span_years.iloc[i]
            )
            trend_strength_eff, trend_variance_eff = calculate_trend_metrics(
                building_data['effective_price'], effective_price_cagr[i], time_span_years.iloc[i]
            )

            results.append({
                'building_id': building_id,
                'latest_price': latest_prices.iloc[i],
                'latest_effective_price': latest_effective_prices.iloc[i],
                'predicted_current_price': round(price_results['predicted_current'], 2),
                'predicted_current_price_lower': round(price_results['current_lower'], 2),
                'predicted_current_price_upper': round(price_results['current_upper'], 2),
                'predicted_current_effective_price': round(effective_results['predicted_current'], 2),
                'predicted_current_effective_price_lower': round(effective_results['current_lower'], 2),
                'predicted_current_effective_price_upper': round(effective_results['current_upper'], 2),
                'predicted_future_price': round(price_results['predicted_future'], 2),
                'predicted_future_price_lower': round(price_results['future_lower'], 2),
                'predicted_future_price_upper': round(price_results['future_upper'], 2),
                'predicted_future_effective_price': round(effective_results['predicted_future'], 2),
                'predicted_future_effective_price_lower': round(effective_results['future_lower'], 2),
                'predicted_future_effective_price_upper': round(effective_results['future_upper'], 2),
                'average_percent_gain_per_year': price_cagr[i],
                'average_percent_gain_per_year_effective': effective_price_cagr[i],
                'trend_strength_pct': trend_strength,
                'trend_strength_effective_pct': trend_strength_eff,
                'trend_variance_pct': trend_variance,
                'trend_variance_effective_pct': trend_variance_eff,
                'latest_date': last_rows['date'].iloc[i],
                'prediction_date': pd.Timestamp.now().normalize()
            })

        # Create DataFrame from results
        metrics_df = pd.DataFrame(results)

        # Merge with date ranges to add min/max dates
        date_ranges_dict = date_ranges.set_index('building_id')[['min_date', 'max_date']].to_dict('index')
        metrics_df['min_date'] = metrics_df['building_id'].map(lambda x: date_ranges_dict.get(x, {}).get('min_date'))
        metrics_df['max_date'] = metrics_df['building_id'].map(lambda x: date_ranges_dict.get(x, {}).get('max_date'))

        # Calculate data span in days for additional context
        metrics_df['data_span_days'] = (metrics_df['max_date'] - metrics_df['min_date']).dt.days

        return metrics_df

    def upload_to_database(self, data: pd.DataFrame, table_name: str,
                           id_col: str = 'building_id') -> bool:
        """Upload results to PostgreSQL database."""
        try:
            # Prepare data
            upload_data = data.copy()
            if id_col in upload_data.columns:
                upload_data[id_col] = upload_data[id_col].astype(str)
            upload_data['created_at'] = pd.Timestamp.now()

            # Upload
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                connection.commit()

            upload_data.to_sql(table_name, self.engine, if_exists='replace', index=False, method='multi')

            logger.info(f"Successfully uploaded {len(upload_data)} records to {table_name}")
            return True

        except Exception as e:
            logger.error(f"Database upload failed: {str(e)}")
            return False

    def _convert_for_json(self, obj):
        """Convert numpy/pandas types to JSON serializable format."""
        if obj is None or pd.isna(obj):
            return None
        elif isinstance(obj, dict):
            return {k: self._convert_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_for_json(item) for item in obj]
        elif isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        else:
            return obj

    def run_building_pipeline(self, forecast_days: int = 28) -> bool:
        """Complete pipeline for building rental price forecasting."""
        logger.info("Starting building price forecasting pipeline...")

        try:
            # Load and process data
            building_history = self.load_building_data()
            processed_data, building_list = self.process_building_data(building_history)

            final_output = self.calculate_growth_metrics(processed_data)
            # Upload to database
            success = self.upload_to_database(final_output, 'building_analysis')
            logger.info(f"Building pipeline completed. Upload success: {success}")
            return success

        except Exception as e:
            logger.error(f"Building pipeline failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def run_zillow_pipeline(self, prediction_days: int = 365) -> bool:
        """Complete pipeline for Zillow property price forecasting."""
        logger.info("Starting Zillow price forecasting pipeline...")

        try:
            # Load and process data - now returns the same format as building analysis
            final_output = self.load_zillow_data(prediction_days)

            if final_output.empty:
                logger.warning("No Zillow data to process")
                return False

            # Add created_at timestamp for database upload
            final_output['created_at'] = pd.Timestamp.now()

            # Upload to database
            success = self.upload_to_database(final_output, 'zillow_analysis', 'id')
            logger.info(f"Zillow pipeline completed. Upload success: {success}")
            return success

        except Exception as e:
            logger.error(f"Zillow pipeline failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def load_geographic_data(self) -> pd.DataFrame:
        """Load geographic building data from database."""

        geographic_data = pd.read_sql("""
                                      SELECT geography_type,
                                             geography_value, date, price, effective_price, unit_count
                                      FROM units_history_grouped_by_geography
                                      ORDER BY geography_type, geography_value, date
                                      """, self.engine)

        logger.info(f"Loaded {len(geographic_data)} geographic data points")
        return geographic_data

    def get_geographic_metadata(self) -> pd.DataFrame:
        """Get metadata about geographic areas (total units and buildings)."""

        metadata_query = """
                         WITH geographic_stats AS (SELECT uh.zip_code, \
                                                          uh.city, \
                                                          uh.state, \
                                                          uh.msa, \
                                                          COUNT(DISTINCT uh.unit_id)     as total_units, \
                                                          COUNT(DISTINCT uh.building_id) as total_buildings \
                                                   FROM units_history uh \
                                                   WHERE uh.zip_code IS NOT NULL \
                                                      OR uh.city IS NOT NULL \
                                                      OR uh.state IS NOT NULL \
                                                      OR uh.msa IS NOT NULL \
                                                   GROUP BY uh.zip_code, uh.city, uh.state, uh.msa)
                         SELECT 'zip_code' as geography_type, \
                                zip_code   as geography_value, \
                                total_units, \
                                total_buildings
                         FROM geographic_stats
                         WHERE zip_code IS NOT NULL

                         UNION ALL

                         SELECT 'city'               as geography_type, \
                                city                 as geography_value, \
                                SUM(total_units)     as total_units, \
                                SUM(total_buildings) as total_buildings
                         FROM geographic_stats
                         WHERE city IS NOT NULL
                         GROUP BY city

                         UNION ALL

                         SELECT 'state'              as geography_type, \
                                state                as geography_value, \
                                SUM(total_units)     as total_units, \
                                SUM(total_buildings) as total_buildings
                         FROM geographic_stats
                         WHERE state IS NOT NULL
                         GROUP BY state

                         UNION ALL

                         SELECT 'msa'                as geography_type, \
                                msa                  as geography_value, \
                                SUM(total_units)     as total_units, \
                                SUM(total_buildings) as total_buildings
                         FROM geographic_stats
                         WHERE msa IS NOT NULL
                         GROUP BY msa \
                         """

        metadata = pd.read_sql(metadata_query, self.engine)
        logger.info(f"Loaded metadata for {len(metadata)} geographic areas")
        return metadata

    def calculate_geographic_growth_metrics(self, geographic_data: pd.DataFrame, metadata: pd.DataFrame,
                                            forecast_days: int = 365, confidence_level: float = 0.95) -> pd.DataFrame:
        """Calculate growth metrics for geographic areas using the same methodology as buildings."""

        results = []

        # Get unique geography combinations
        geographic_groups = geographic_data.groupby(['geography_type', 'geography_value'])

        logger.info(f"Processing {len(geographic_groups)} geographic areas...")

        for (geo_type, geo_value), group_data in geographic_groups:
            try:
                # Sort by date
                group_data = group_data.sort_values('date').copy()

                # Skip if insufficient data
                if len(group_data) < 2:
                    logger.debug(f"Skipping {geo_type} {geo_value}: insufficient data")
                    continue

                # Convert date to datetime if needed
                group_data['date'] = pd.to_datetime(group_data['date'])

                # Basic metrics
                min_date = group_data['date'].min()
                max_date = group_data['date'].max()
                latest_price = group_data['price'].iloc[-1]
                latest_effective_price = group_data['effective_price'].iloc[-1]
                earliest_price = group_data['price'].iloc[0]
                earliest_effective_price = group_data['effective_price'].iloc[0]

                # Calculate time span
                time_span_days = (max_date - min_date).days
                time_span_years = time_span_days / 365.25

                # Calculate CAGR for both price types
                price_cagr = 0.0
                effective_price_cagr = 0.0

                if time_span_years > 0 and earliest_price > 0:
                    price_cagr = ((latest_price / earliest_price) ** (1 / time_span_years) - 1) * 100

                if time_span_years > 0 and earliest_effective_price > 0:
                    effective_price_cagr = ((latest_effective_price / earliest_effective_price) ** (
                            1 / time_span_years) - 1) * 100

                # Calculate price predictions using shared functions
                price_results = calculate_price_predictions(
                    group_data, 'price', forecast_days, confidence_level
                )

                effective_results = calculate_price_predictions(
                    group_data, 'effective_price', forecast_days, confidence_level
                )

                # Calculate trend metrics
                trend_strength, trend_variance = calculate_trend_metrics(
                    group_data['price'], price_cagr, time_span_years
                )
                trend_strength_eff, trend_variance_eff = calculate_trend_metrics(
                    group_data['effective_price'], effective_price_cagr, time_span_years
                )

                # Get metadata for this geography
                geo_metadata = metadata[
                    (metadata['geography_type'] == geo_type) &
                    (metadata['geography_value'] == geo_value)
                    ]

                total_units = geo_metadata['total_units'].iloc[0] if not geo_metadata.empty else 0
                total_buildings = geo_metadata['total_buildings'].iloc[0] if not geo_metadata.empty else 0

                # Compile results
                result = {
                    'geography_type': geo_type,
                    'geography_value': geo_value,
                    'latest_price': latest_price,
                    'latest_effective_price': latest_effective_price,
                    'predicted_current_price': round(price_results['predicted_current'], 2),
                    'predicted_current_price_lower': round(price_results['current_lower'], 2),
                    'predicted_current_price_upper': round(price_results['current_upper'], 2),
                    'predicted_current_effective_price': round(effective_results['predicted_current'], 2),
                    'predicted_current_effective_price_lower': round(effective_results['current_lower'], 2),
                    'predicted_current_effective_price_upper': round(effective_results['current_upper'], 2),
                    'predicted_future_price': round(price_results['predicted_future'], 2),
                    'predicted_future_price_lower': round(price_results['future_lower'], 2),
                    'predicted_future_price_upper': round(price_results['future_upper'], 2),
                    'predicted_future_effective_price': round(effective_results['predicted_future'], 2),
                    'predicted_future_effective_price_lower': round(effective_results['future_lower'], 2),
                    'predicted_future_effective_price_upper': round(effective_results['future_upper'], 2),
                    'average_percent_gain_per_year': round(price_cagr, 4),
                    'average_percent_gain_per_year_effective': round(effective_price_cagr, 4),
                    'trend_strength_pct': round(trend_strength, 4),
                    'trend_strength_effective_pct': round(trend_strength_eff, 4),
                    'trend_variance_pct': round(trend_variance, 4),
                    'trend_variance_effective_pct': round(trend_variance_eff, 4),
                    'total_units_in_geography': int(total_units),
                    'total_buildings_in_geography': int(total_buildings),
                    'latest_date': max_date,
                    'prediction_date': pd.Timestamp.now().normalize(),
                    'min_date': min_date,
                    'max_date': max_date,
                    'data_span_days': time_span_days
                }

                results.append(result)

            except Exception as e:
                logger.warning(f"Error processing {geo_type} {geo_value}: {e}")
                continue

        if not results:
            logger.warning("No geographic results generated")
            return pd.DataFrame()

        # Create DataFrame from results
        metrics_df = pd.DataFrame(results)

        logger.info(f"Generated {len(metrics_df)} geographic analysis results")
        return metrics_df

    def run_geographic_pipeline(self, forecast_days: int = 365) -> bool:
        """Complete pipeline for geographic building price forecasting."""
        logger.info("Starting geographic price forecasting pipeline...")

        try:
            # Load geographic data
            geographic_data = self.load_geographic_data()

            if geographic_data.empty:
                logger.warning("No geographic data found")
                return False

            # Load metadata
            metadata = self.get_geographic_metadata()

            # Calculate growth metrics
            final_output = self.calculate_geographic_growth_metrics(
                geographic_data, metadata, forecast_days
            )

            if final_output.empty:
                logger.warning("No geographic analysis results generated")
                return False

            # Upload to database
            success = self.upload_to_database(final_output, 'building_geographic_analysis', 'geography_type')
            logger.info(f"Geographic pipeline completed. Upload success: {success}")
            return success

        except Exception as e:
            logger.error(f"Geographic pipeline failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def load_zillow_geographic_data(self) -> pd.DataFrame:
        """Load Zillow property data grouped by geographic areas - SIMPLIFIED VERSION."""

        # Use the new dbt model directly instead of parsing JSON in Python
        geographic_query = """
                           SELECT geography_type, \
                                  geography_value, date, price, price_change_rate, property_count
                           FROM zillow_history_grouped_by_geography
                           ORDER BY geography_type, geography_value, date \
                           """

        geographic_data = pd.read_sql(geographic_query, self.engine)
        logger.info(f"Loaded {len(geographic_data)} Zillow geographic price points")

        return geographic_data

    def get_zillow_geographic_metadata(self) -> pd.DataFrame:
        """Get metadata about Zillow geographic areas - SIMPLIFIED VERSION."""

        # Much simpler query using the dbt models
        metadata_query = """
                         WITH property_counts AS (SELECT geography_type, \
                                                         geography_value, \
                                                         COUNT(DISTINCT date) as data_points, \
                                                         AVG(property_count)  as avg_properties_per_date, \
                                                         MAX(property_count)  as max_properties_per_date, \
                                                         AVG(price)           as avg_price, \
                                                         PERCENTILE_CONT(0.5)    WITHIN \
                         GROUP (ORDER BY price) as median_price,
                             STDDEV(price) as price_std_dev,
                             MIN (date) as earliest_date,
                             MAX (date) as latest_date
                         FROM zillow_history_grouped_by_geography
                         GROUP BY geography_type, geography_value
                             )
                         SELECT geography_type, \
                                geography_value, \
                                ROUND(avg_properties_per_date) as total_properties, \
                                avg_price                      as avg_property_value, \
                                median_price                   as median_property_value, \
                                price_std_dev                  as property_value_std_dev, \
                                data_points, \
                                earliest_date, \
                                latest_date
                         FROM property_counts \
                         """

        metadata = pd.read_sql(metadata_query, self.engine)
        logger.info(f"Loaded Zillow metadata for {len(metadata)} geographic areas")
        return metadata

    def calculate_zillow_geographic_growth_metrics(self, geographic_data: pd.DataFrame, metadata: pd.DataFrame,
                                                   forecast_days: int = 365,
                                                   confidence_level: float = 0.95) -> pd.DataFrame:
        """Calculate growth metrics for Zillow geographic areas - SIMPLIFIED VERSION."""

        results = []
        geographic_groups = geographic_data.groupby(['geography_type', 'geography_value'])

        logger.info(f"Processing {len(geographic_groups)} Zillow geographic areas...")

        for (geo_type, geo_value), group_data in geographic_groups:
            try:
                # Data is already cleaned and aggregated by dbt
                group_data = group_data.sort_values('date').copy()

                if len(group_data) < 2:
                    continue

                # Basic metrics - no need to convert dates, already clean
                min_date = group_data['date'].min()
                max_date = group_data['date'].max()
                latest_price = group_data['price'].iloc[-1]
                earliest_price = group_data['price'].iloc[0]
                time_span_days = (pd.to_datetime(max_date) - pd.to_datetime(min_date)).days
                time_span_years = time_span_days / 365.25

                # Calculate CAGR
                price_cagr = 0.0
                if time_span_years > 0 and earliest_price > 0:
                    price_cagr = ((latest_price / earliest_price) ** (1 / time_span_years) - 1) * 100

                # Convert date column for predictions function
                group_data['date'] = pd.to_datetime(group_data['date'])

                # Calculate predictions and trends
                price_results = calculate_price_predictions(group_data, 'price', forecast_days, confidence_level)
                trend_strength, trend_variance = calculate_trend_metrics(group_data['price'], price_cagr,
                                                                         time_span_years)

                # Get metadata
                geo_metadata = metadata[
                    (metadata['geography_type'] == geo_type) &
                    (metadata['geography_value'] == geo_value)
                    ]

                total_properties = geo_metadata['total_properties'].iloc[0] if not geo_metadata.empty else 0
                avg_property_value = geo_metadata['avg_property_value'].iloc[
                    0] if not geo_metadata.empty else latest_price
                median_property_value = geo_metadata['median_property_value'].iloc[
                    0] if not geo_metadata.empty else latest_price
                property_value_std_dev = geo_metadata['property_value_std_dev'].iloc[0] if not geo_metadata.empty else 0

                result = {
                    'geography_type': geo_type,
                    'geography_value': geo_value,
                    'latest_price': latest_price,
                    'predicted_current_price': round(price_results['predicted_current'], 2),
                    'predicted_current_price_lower': round(price_results['current_lower'], 2),
                    'predicted_current_price_upper': round(price_results['current_upper'], 2),
                    'predicted_future_price': round(price_results['predicted_future'], 2),
                    'predicted_future_price_lower': round(price_results['future_lower'], 2),
                    'predicted_future_price_upper': round(price_results['future_upper'], 2),
                    'average_percent_gain_per_year': round(price_cagr, 4),
                    'trend_strength_pct': round(trend_strength, 4),
                    'trend_variance_pct': round(trend_variance, 4),
                    'total_properties_in_geography': int(total_properties),
                    'median_property_value': round(median_property_value, 2) if pd.notna(median_property_value) else 0,
                    'avg_property_value': round(avg_property_value, 2) if pd.notna(avg_property_value) else 0,
                    'property_value_std_dev': round(property_value_std_dev, 2) if pd.notna(
                        property_value_std_dev) else 0,
                    'latest_date': max_date,
                    'prediction_date': pd.Timestamp.now().normalize(),
                    'min_date': min_date,
                    'max_date': max_date,
                    'data_span_days': time_span_days
                }

                results.append(result)

            except Exception as e:
                logger.warning(f"Error processing Zillow {geo_type} {geo_value}: {e}")
                continue

        if not results:
            return pd.DataFrame()

        metrics_df = pd.DataFrame(results)

        # Filter for recent data
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=7 * 30)
        recent_data = metrics_df[pd.to_datetime(metrics_df['latest_date']) >= cutoff_date].copy()

        logger.info(f"Generated {len(recent_data)} Zillow geographic analysis results")
        return recent_data

    def run_zillow_geographic_pipeline(self, forecast_days: int = 365) -> bool:
        """Complete pipeline for Zillow geographic price forecasting."""
        logger.info("Starting Zillow geographic price forecasting pipeline...")

        try:
            # Load Zillow geographic data
            geographic_data = self.load_zillow_geographic_data()

            if geographic_data.empty:
                logger.warning("No Zillow geographic data found")
                return False

            # Load metadata
            metadata = self.get_zillow_geographic_metadata()

            # Calculate growth metrics
            final_output = self.calculate_zillow_geographic_growth_metrics(
                geographic_data, metadata, forecast_days
            )

            if final_output.empty:
                logger.warning("No Zillow geographic analysis results generated")
                return False

            # Upload to database
            success = self.upload_to_database(final_output, 'zillow_geographic_analysis', 'geography_type')
            logger.info(f"Zillow geographic pipeline completed. Upload success: {success}")
            return success

        except Exception as e:
            logger.error(f"Zillow geographic pipeline failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    # Update the complete pipeline to include Zillow geographic analysis
    def run_complete_pipeline(self, building_forecast_days: int = 28,
                              zillow_prediction_days: int = 365,
                              geographic_forecast_days: int = 365,
                              zillow_geographic_forecast_days: int = 365) -> bool:
        """Run all forecasting pipelines: building, Zillow, building geographic, and Zillow geographic."""
        logger.info("=" * 60)
        logger.info("STARTING UNIFIED PRICE FORECASTING PIPELINE")
        logger.info("=" * 60)

        building_success = self.run_building_pipeline(building_forecast_days)
        zillow_success = self.run_zillow_pipeline(zillow_prediction_days)
        geographic_success = self.run_geographic_pipeline(geographic_forecast_days)
        zillow_geographic_success = self.run_zillow_geographic_pipeline(zillow_geographic_forecast_days)

        overall_success = building_success and zillow_success and geographic_success and zillow_geographic_success
        logger.info(f"Pipeline completed. Overall success: {overall_success}")
        logger.info(f"  - Building pipeline: {'✓' if building_success else '✗'}")
        logger.info(f"  - Zillow pipeline: {'✓' if zillow_success else '✗'}")
        logger.info(f"  - Building geographic pipeline: {'✓' if geographic_success else '✗'}")
        logger.info(f"  - Zillow geographic pipeline: {'✓' if zillow_geographic_success else '✗'}")

        return overall_success


# Update the CLI to include Zillow geographic options
@click.command()
@click.option('--db-url', help='Database URL')
@click.option('--building-forecast-days', default=28, help='Number of days to forecast for buildings')
@click.option('--zillow-prediction-days', default=365, help='Number of days to predict for Zillow properties')
@click.option('--geographic-forecast-days', default=365, help='Number of days to forecast for geographic areas')
@click.option('--zillow-geographic-forecast-days', default=365,
              help='Number of days to forecast for Zillow geographic areas')
@click.option('--pipeline', default='complete',
              type=click.Choice(['complete', 'building', 'zillow', 'geographic', 'zillow-geographic']),
              help='Which pipeline to run')
def main(db_url, building_forecast_days, zillow_prediction_days, geographic_forecast_days,
         zillow_geographic_forecast_days, pipeline):
    """Main execution function with CLI interface."""

    try:
        # Initialize pipeline
        forecasting_pipeline = UnifiedPriceForecastingPipeline(db_url=get_dbt_database_url())

        # Run selected pipeline
        if pipeline == 'complete':
            success = forecasting_pipeline.run_complete_pipeline(
                building_forecast_days, zillow_prediction_days, geographic_forecast_days,
                zillow_geographic_forecast_days
            )
        elif pipeline == 'building':
            success = forecasting_pipeline.run_building_pipeline(building_forecast_days)
        elif pipeline == 'zillow':
            success = forecasting_pipeline.run_zillow_pipeline(zillow_prediction_days)
        elif pipeline == 'geographic':
            success = forecasting_pipeline.run_geographic_pipeline(geographic_forecast_days)
        elif pipeline == 'zillow-geographic':
            success = forecasting_pipeline.run_zillow_geographic_pipeline(zillow_geographic_forecast_days)
        else:
            logger.error(f"Unknown pipeline type: {pipeline}")
            sys.exit(1)

        if not success:
            logger.warning("Pipeline completed with some failures")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
