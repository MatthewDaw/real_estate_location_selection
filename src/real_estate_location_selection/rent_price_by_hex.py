#!/usr/bin/env python3

from real_estate_location_selection.connection import connection
import pandas as pd
import folium
from folium.plugins import HeatMap
import branca.colormap as cm
import os

# ----------------------------------------
# Configuration
# ----------------------------------------
OUTPUT_HTML = "rent_per_bed_heatmap.html"
CACHE_FILE  = "rent_per_bed_cache.pkl"

SQL = """
WITH primary_source as (
SELECT
    ANY_VALUE(b.lat)            AS lat,
    ANY_VALUE(b.lon)            AS lon,
    SUM(u.effective_price) / SUM(u.bed) AS price_per_bed
FROM   buildings b
JOIN   units u ON u.building_id = b.id
WHERE  b.is_single_family = FALSE
  AND  b.created_on    > DATE '2025-04-01'
  AND  (u.exit_market IS NULL OR u.exit_market > DATE '2025-01-01')
  AND  u.bath  > 0
  AND  u.bed   >= 3
  AND u.effective_price > 0
  AND b.state = 'UT'
GROUP BY b.id
)
    select  * from primary_source where price_per_bed < 1500
"""

def fetch_price_per_bed(cache_file: str = CACHE_FILE) -> pd.DataFrame:
    """
    Load from cache if available, otherwise run the query and cache the result.
    """
    if os.path.exists(cache_file):
        print(f"Loading data from cache: {cache_file}")
        return pd.read_pickle(cache_file)

    print("Cache not found, querying database...")
    conn = connection()
    try:
        df = pd.read_sql(SQL, conn)
    finally:
        conn.close()

    # cache for next time
    df.to_pickle(cache_file)
    print(f"Query result cached to {cache_file}")
    return df

def create_heatmap(df: pd.DataFrame, output_file: str = OUTPUT_HTML):
    """
    Given a DataFrame with 'lat', 'lon', and 'price_per_bed',
    create and save an HTML heatmap (Leaflet) to output_file.
    """
    df = df.dropna(subset=["lat", "lon", "price_per_bed"])
    heat_data = df[["lat", "lon", "price_per_bed"]].values.tolist()

    # center map on continental US
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4)

    # value bounds for colormap
    vmin = df["price_per_bed"].min()
    vmax = df["price_per_bed"].max()

    # gradient: low→blue, mid→yellow, high→red
    gradient = {0.0: "blue", 0.5: "yellow", 1.0: "red"}

    # add heatmap layer
    HeatMap(
        heat_data,
        radius=10,
        blur=10,
        min_opacity=0.6,
        max_zoom=6,
        gradient=gradient
    ).add_to(m)

    # add legend
    colormap = cm.LinearColormap(
        colors=["blue", "yellow", "red"],
        vmin=vmin,
        vmax=vmax,
        caption="Average Price per Bed (USD)"
    )
    colormap.add_to(m)

    m.save(output_file)
    print(f"Heatmap saved to {output_file}")

def main():
    df = fetch_price_per_bed()
    if df.empty:
        print("No data returned from query. Check your filters or database contents.")
        return
    create_heatmap(df)

if __name__ == "__main__":
    main()
