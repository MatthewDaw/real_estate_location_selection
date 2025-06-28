import folium
from folium.plugins import HeatMap
import branca.colormap as cm
import pandas as pd

def create_heatmap(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    value_col: str,
    output_file: str = "heatmap.html"
):
    """
    Generalized heatmap creator.

    Args:
        df: DataFrame with at least lat_col, lon_col, and value_col.
        lat_col: Name of latitude column.
        lon_col: Name of longitude column.
        value_col: Name of column for heatmap intensity.
        output_file: Where to save the HTML heatmap.
    """
    # Drop rows with missing lat/lon/value
    df = df.dropna(subset=[lat_col, lon_col, value_col])

    # Prepare data for HeatMap
    heat_data = df[[lat_col, lon_col, value_col]].values.tolist()

    # center map on continental US (or first row's coordinates as fallback)
    if not df.empty:
        center_lat = df[lat_col].iloc[0]
        center_lon = df[lon_col].iloc[0]
    else:
        center_lat, center_lon = 39.8283, -98.5795

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # Value bounds for colormap
    vmin = df[value_col].min()
    vmax = df[value_col].max()

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
        caption=f"Heatmap of {value_col}"
    )
    colormap.add_to(m)

    m.save(output_file)
    print(f"Heatmap saved to {output_file}")

# Example usage
# create_heatmap(df=my_df, lat_col="latitude", lon_col="longitude", value_col="price_per_bed", output_file="heatmap.html")
