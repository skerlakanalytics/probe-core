"""MAXI derivate: per-event GeoTIFF/envelope export for probe_explorer.

Pure geometry/raster computation over an already-fetched gold DataFrame — no
data access lives here. probe_explorer's data_interface.py (S3-backed,
GoldNotReadyError-gated) fetches the raw rows; app.py hands the resulting
DataFrame to the functions below to turn it into bytes/GeoJSON for the UI.
The two repos are coupled by design (probe_explorer imports this module via
sys.path, same convention as the older maxi_ifk_and_raster.py import); no
probabilities are used here since gold-SIM doesn't carry them yet (see
data_interface.py's module docstring — pending the enrich phase).
"""

import io
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
from rasterio.transform import from_origin
from shapely.geometry import shape

PIXEL_SIZE = 5
CRS_LV95 = "EPSG:2056"
CACHE_DIR = Path("~/probe_explorer/.cache_maxi").expanduser()
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def event_layer_geotiff_bytes(df: pd.DataFrame, col: str) -> bytes:
    """In-memory GeoTIFF (LV95) of one value column of an event simulation df."""
    res = PIXEL_SIZE
    xmin, xmax = df['x'].min() - res / 2, df['x'].max() + res / 2
    ymin, ymax = df['y'].min() - res / 2, df['y'].max() + res / 2
    w, h = int((xmax - xmin) / res), int((ymax - ymin) / res)
    grid = np.full((h, w), np.nan, dtype=np.float32)
    ix = ((df['x'].to_numpy() - xmin) / res).astype(int)
    iy = ((ymax - df['y'].to_numpy()) / res).astype(int)
    mask = (ix >= 0) & (ix < w) & (iy >= 0) & (iy < h)
    grid[iy[mask], ix[mask]] = df[col].to_numpy()[mask]

    buf = io.BytesIO()
    with rasterio.open(buf, 'w', driver='GTiff', height=h, width=w, count=1, dtype='float32',
                       crs=CRS_LV95, transform=from_origin(xmin, ymax, res, res),
                       compress='lzw', tiled=True) as dst:
        dst.write(grid, 1)
    return buf.getvalue()


def export_event_envelope_geojson(df: pd.DataFrame, id_anriss: int):
    """Envelope of every simulated scenario of one event, built from
    data_interface.get_anriss_all_scenarios_gold_data's raw rows.
    Returns (geojson_dict_wgs84, cache_path) — GeoJSON is WGS84 for web maps,
    the cached file on disk stays LV95. Returns (None, None) if df is empty."""
    cache_path = CACHE_DIR / f"event_{id_anriss}_envelope.geojson"
    if cache_path.exists():
        gdf = gpd.read_file(str(cache_path))
        return json.loads(gdf.to_crs('EPSG:4326').to_json()), str(cache_path)

    if df.empty:
        return None, None
    df = df[['x', 'y']].drop_duplicates()

    x_min, x_max = df['x'].min(), df['x'].max()
    y_min, y_max = df['y'].min(), df['y'].max()

    west = x_min - (PIXEL_SIZE / 2)
    north = y_max + (PIXEL_SIZE / 2)

    width = int(round((x_max - x_min) / PIXEL_SIZE)) + 1
    height = int(round((y_max - y_min) / PIXEL_SIZE)) + 1

    grid = np.zeros((height, width), dtype=np.uint8)
    df = df.assign(
        col=((df['x'] - x_min) / PIXEL_SIZE).round().astype(int),
        row=((y_max - df['y']) / PIXEL_SIZE).round().astype(int),
    )
    grid[df['row'], df['col']] = 1

    transform = from_origin(west, north, PIXEL_SIZE, PIXEL_SIZE)
    shapes = features.shapes(grid, transform=transform)
    polygons = [shape(s) for s, v in shapes if v == 1]

    if not polygons:
        return None, None

    envelope_geom = gpd.GeoSeries(polygons, crs=CRS_LV95).union_all()
    envelope_gdf_lv95 = gpd.GeoDataFrame(
        {'id_anriss': [id_anriss]},
        geometry=[envelope_geom],
        crs=CRS_LV95
    )

    envelope_gdf_lv95.to_file(str(cache_path), driver='GeoJSON')
    geojson_data = json.loads(envelope_gdf_lv95.to_crs('EPSG:4326').to_json())
    return geojson_data, str(cache_path)
