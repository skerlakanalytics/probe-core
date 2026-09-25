"""The derivate lake (Data-Lake-Derivate/) layout -- single source of truth for BOTH sides:
ProBE_control_center's derivate/run_derivate.py producers write with these paths and band
names, pgr-atlas reads with them. Hive paths, band names (build + parse), read-only S3
discovery, and cropping a canton mosaic to a drawn selection.

Layout (one file per (metric, partition, kachel); thresholds / return periods are BANDS):

    <metric>/<key>=<value>/id_kachel=<id>/data.tif        partitioned metrics (variable / combo)
    <metric>/id_kachel=<id>/data.tif                      unpartitioned (affected_mask, sim_count)
    <metric>/_mosaic/[<key>=<value>/]canton.tif           canton COG, BAND-interleaved
    <metric>/_mosaic/[<key>=<value>/]canton_bands.json    {'bands': [{index, name, vmin, vmax}]}

No config hash: what exists on S3 is the truth, so readers discover everything by listing,
never from a config file. Band descriptions are the column names built below
('depth_0_25m_rp', 'depth_0_25m_hitrate', 'depth_rp100y', '<combo>_hitrate',
'<combo>_hitrate80y'); single-band metrics have no description.
"""

import json
import re

import numpy as np
import rasterio
import rasterio.windows
from rasterio.features import geometry_mask
from shapely.geometry import Polygon

from probe_core.data_lake.data_interface import S3_BUCKET_GOLD
from probe_core.data_lake.data_lake_schema import (
    DATA_LAKE_DIR_DERIVATE, DATA_LAKE_SUBDIR_MOSAIC,
    DERIVATE_METRIC_RETURN_PERIOD, DERIVATE_METRIC_INTENSITY, DERIVATE_METRIC_HIT_RATE,
    DERIVATE_METRIC_COMBINED_HIT_RATE, DERIVATE_METRIC_SIM_COUNT, DERIVATE_METRIC_AFFECTED_MASK,
)
from probe_core.derivate.maxi_ifk_and_raster import _selection_tags, normalize_selection, selection_bounds
from probe_core.s3 import get_s3_client, s3_key_exists, s3_presigned_url

# Raster metrics -> their Hive partition key (None = unpartitioned).
PARTITION_KEYS = {
    DERIVATE_METRIC_RETURN_PERIOD: 'variable',
    DERIVATE_METRIC_HIT_RATE: 'variable',
    DERIVATE_METRIC_INTENSITY: 'variable',
    DERIVATE_METRIC_COMBINED_HIT_RATE: 'combo',
    DERIVATE_METRIC_AFFECTED_MASK: None,
    DERIVATE_METRIC_SIM_COUNT: None,
}
UNIT_FILE_STEM = 'data'                  # <unit_kind>=<id>/data.<ext>
KACHEL_FILENAME = f'{UNIT_FILE_STEM}.tif'
CANTON_FILENAME = 'canton.tif'
STATS_FILENAME = 'canton_bands.json'

# Presigned-GET-only access (no HEAD, no directory listing) -- what the Hosttech endpoint needs
# for /vsicurl/ reads of a presigned URL.
VSICURL_ENV = {'CPL_VSIL_CURL_USE_HEAD': 'NO', 'GDAL_DISABLE_READDIR_ON_OPEN': 'EMPTY_DIR'}


# ── Paths ────────────────────────────────────────────────────────────────────
# Generic form (any metric, any ordered ((key, value), ...) partitions, any root -- the runner's
# --root redirects the whole tree to a scratch prefix), then the per-raster-metric shorthands.

def _hive_parts(partitions) -> list[str]:
    return [f"{k}={v}" for k, v in partitions]


def hive_prefix(metric: str, partitions: tuple = (), root: str = DATA_LAKE_DIR_DERIVATE) -> str:
    """<root>/<metric>/<k>=<v>/..."""
    return '/'.join([root, metric] + _hive_parts(partitions))


def hive_mosaic_prefix(metric: str, partitions: tuple = (), root: str = DATA_LAKE_DIR_DERIVATE) -> str:
    """<root>/<metric>/_mosaic/<k>=<v>/... -- underscore-prefixed, so a hive_partitioning glob over
    <metric>/ never trips over it."""
    return '/'.join([root, metric, DATA_LAKE_SUBDIR_MOSAIC] + _hive_parts(partitions))


def hive_unit_key(metric: str, partitions: tuple, unit_kind: str, unit: int, ext: str = 'tif',
                  root: str = DATA_LAKE_DIR_DERIVATE) -> str:
    """<root>/<metric>/<k>=<v>/.../<unit_kind>=<unit>/data.<ext>"""
    return f"{hive_prefix(metric, partitions, root)}/{unit_kind}={unit}/{UNIT_FILE_STEM}.{ext}"


def partitions_for(metric: str, partition: str | None) -> tuple:
    """(metric, partition value) -> the ordered Hive partitions tuple of that raster metric."""
    key = PARTITION_KEYS[metric]
    if (key is None) != (partition is None):
        raise ValueError(f"{metric}: partition must be {'None' if key is None else f'a {key!r} value'}, got {partition!r}")
    return () if key is None else ((key, partition),)


def dataset_prefix(metric: str, partition: str | None = None) -> str:
    return hive_prefix(metric, partitions_for(metric, partition))


def kachel_key(metric: str, id_kachel: int, partition: str | None = None) -> str:
    return hive_unit_key(metric, partitions_for(metric, partition), 'id_kachel', int(id_kachel))


def mosaic_prefix(metric: str, partition: str | None = None) -> str:
    return hive_mosaic_prefix(metric, partitions_for(metric, partition))


def mosaic_key(metric: str, partition: str | None = None) -> str:
    return f"{mosaic_prefix(metric, partition)}/{CANTON_FILENAME}"


def mosaic_stats_key(metric: str, partition: str | None = None) -> str:
    return f"{mosaic_prefix(metric, partition)}/{STATS_FILENAME}"


def file_stem(metric: str, partition: str | None = None) -> str:
    """'return_period_depth' / 'sim_count' -- readable, unique name fragment for downloads."""
    return metric if partition is None else f"{metric}_{partition}"


# ── Band names ───────────────────────────────────────────────────────────────
# Built by the pipeline's exceedance producer (as SQL column names, then GeoTIFF band
# descriptions), parsed back by readers. return_period/intensity names are identical to the
# legacy raster/cfg_<hash>/ layout's, so migrated and freshly computed files match.

# Identifier-safe unit suffix per variable (the display units are m, m/s, kPa).
VAR_UNIT_SLUG = {'depth': 'm', 'velocity': 'ms', 'pressure': 'kpa'}


def fmt_num(x) -> str:
    """Numeric value -> SQL-identifier-safe, readable fragment: 1.0 -> '1', 0.25 -> '0_25',
    1000000 -> '1000000' (never ':g' -- that gives '1e+06', and '+' is not identifier-safe)."""
    xf = float(x)
    s = str(int(xf)) if xf == int(xf) else f"{xf:g}"
    return s.replace('.', '_').replace('-', 'neg')


def return_period_column(variable: str, threshold) -> str:
    """('depth', 1.0) -> 'depth_1m_rp': threshold+unit upfront, '_rp' marks a return-period OUTPUT (years)."""
    return f"{variable}_{fmt_num(threshold)}{VAR_UNIT_SLUG[variable]}_rp"


def hit_rate_column(variable: str, threshold) -> str:
    """('depth', 1.0) -> 'depth_1m_hitrate': same threshold+unit as return_period_column, '_hitrate'
    marks a rate OUTPUT (events/year) -- the same `p` return_period inverts, persisted directly."""
    return f"{variable}_{fmt_num(threshold)}{VAR_UNIT_SLUG[variable]}_hitrate"


def intensity_column(variable: str, return_period) -> str:
    """('depth', 10000) -> 'depth_rp10000y': return period upfront, output is an intensity in the variable's unit."""
    return f"{variable}_rp{fmt_num(return_period)}y"


def combined_hit_rate_columns(name: str) -> tuple[str, str]:
    """combo name -> (annual-rate column, 80-year-probability column)."""
    return f"{name}_hitrate", f"{name}_hitrate80y"


_VARS_RE = '|'.join(VAR_UNIT_SLUG)
_SLUGS_RE = '|'.join(sorted(VAR_UNIT_SLUG.values(), key=len, reverse=True))
_THRESHOLD_BAND = re.compile(rf'^(?P<variable>{_VARS_RE})_(?P<value>[0-9_]+)(?:{_SLUGS_RE})_(?:rp|hitrate)$')
_INTENSITY_BAND = re.compile(rf'^(?P<variable>{_VARS_RE})_rp(?P<value>[0-9_]+)y$')


def _slug_to_number(s: str) -> float:
    return float(s.replace('_', '.'))


def parse_band(metric: str, name: str | None) -> dict:
    """Band description -> the parameter it stands for (inverse of the *_column builders above):
    return_period / hit_rate: {'threshold': 0.25}   (in the variable's display unit: m, m/s, kPa)
    intensity:                {'return_period': 100}
    combined_hit_rate:        {'stat': 'rate'} or {'stat': 'p80'}
    affected_mask / sim_count: {}
    Raises ValueError for a name that doesn't match the metric's naming scheme."""
    if metric in (DERIVATE_METRIC_RETURN_PERIOD, DERIVATE_METRIC_HIT_RATE):
        m = _THRESHOLD_BAND.match(name or '')
        if m:
            return {'threshold': _slug_to_number(m['value'])}
    elif metric == DERIVATE_METRIC_INTENSITY:
        m = _INTENSITY_BAND.match(name or '')
        if m:
            return {'return_period': int(_slug_to_number(m['value']))}
    elif metric == DERIVATE_METRIC_COMBINED_HIT_RATE:
        if name and name.endswith('_hitrate80y'):
            return {'stat': 'p80'}
        if name and name.endswith('_hitrate'):
            return {'stat': 'rate'}
    elif metric in (DERIVATE_METRIC_AFFECTED_MASK, DERIVATE_METRIC_SIM_COUNT):
        return {}
    raise ValueError(f"{metric}: unrecognized band name {name!r}")


# ── Discovery (read-only S3 listings) ────────────────────────────────────────

def _list_subdirs(prefix: str, bucket: str) -> list[str]:
    """Immediate 'sub/' names under prefix/ (without the trailing slash)."""
    names = []
    for page in get_s3_client().get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=f"{prefix}/", Delimiter='/'):
        names += [c['Prefix'][len(prefix) + 1:-1] for c in page.get('CommonPrefixes', [])]
    return names


def list_partitions(metric: str, bucket: str = S3_BUCKET_GOLD) -> list[str]:
    """Partition values present for a partitioned metric (e.g. ['depth', 'pressure', 'velocity']),
    [] if the metric has no data yet. [None] for an unpartitioned metric that has any data."""
    key = PARTITION_KEYS[metric]
    subdirs = _list_subdirs(f"{DATA_LAKE_DIR_DERIVATE}/{metric}", bucket)
    if key is None:
        return [None] if any(s.startswith('id_kachel=') for s in subdirs) else []
    return sorted(s.split('=', 1)[1] for s in subdirs if s.startswith(f"{key}="))


def list_kacheln(metric: str, partition: str | None = None, bucket: str = S3_BUCKET_GOLD) -> set[int]:
    """id_kachel values with a file for (metric, partition). One listing of ~8 pages for the
    full canton; the caller caches it."""
    return {int(s.split('=', 1)[1]) for s in _list_subdirs(dataset_prefix(metric, partition), bucket)
            if s.startswith('id_kachel=')}


def load_mosaic_bands(metric: str, partition: str | None = None, bucket: str = S3_BUCKET_GOLD) -> list[dict] | None:
    """The canton mosaic's per-band stats [{index, name, vmin, vmax}], or None if no mosaic has
    been built for (metric, partition) yet."""
    key = mosaic_stats_key(metric, partition)
    if not s3_key_exists(bucket, key):
        return None
    return json.loads(get_s3_client().get_object(Bucket=bucket, Key=key)['Body'].read())['bands']


# ── Crop a canton mosaic to a selection ──────────────────────────────────────

def snapped_window(ds, xmin: float, ymin: float, xmax: float, ymax: float) -> rasterio.windows.Window:
    """LV95 bounds -> whole-pixel window of ds, snapped OUTWARD so it covers the bounds fully,
    clipped to the dataset. Whatever comes back lies exactly on the source grid (pixel centres
    at k*5+2.5 -- never re-derived from the request). Zero-size window if there's no overlap."""
    raw = rasterio.windows.from_bounds(xmin, ymin, xmax, ymax, transform=ds.transform)
    col_off, row_off = int(np.floor(raw.col_off)), int(np.floor(raw.row_off))
    col_end, row_end = int(np.ceil(raw.col_off + raw.width)), int(np.ceil(raw.row_off + raw.height))
    window = rasterio.windows.Window(col_off, row_off, col_end - col_off, row_end - row_off)
    try:
        return window.intersection(rasterio.windows.Window(0, 0, ds.width, ds.height))
    except rasterio.errors.WindowError:
        return rasterio.windows.Window(0, 0, 0, 0)


def crop_mosaic_to_selection(metric: str, partition: str | None, selection, out_path: str,
                             bucket: str = S3_BUCKET_GOLD) -> bool:
    """Write the part of (metric, partition)'s canton mosaic that covers `selection` (the
    {'type': 'bbox'|'polygon', 'ring': [[E, N], ...]} shape, see maxi_ifk_and_raster.
    normalize_selection) to out_path as a GeoTIFF with all bands. For a polygon, pixels whose
    centre lies outside it are set to nodata. Streams band by band, so peak memory is one band of
    the window. Returns False (nothing written) if there is no mosaic or it doesn't overlap."""
    key = mosaic_key(metric, partition)
    if not s3_key_exists(bucket, key):
        return False
    selection = normalize_selection(selection)
    xmin, ymin, xmax, ymax = selection_bounds(selection)
    with rasterio.Env(**VSICURL_ENV), rasterio.open(f"/vsicurl/{s3_presigned_url(bucket, key)}") as src:
        window = snapped_window(src, xmin, ymin, xmax, ymax)
        if window.width <= 0 or window.height <= 0:
            return False
        transform = src.window_transform(window)
        shape = (int(window.height), int(window.width))
        outside = None
        if selection['type'] == 'polygon':
            # geometry_mask: True = outside the shape (pixel centre test, all_touched=False)
            outside = geometry_mask([Polygon(selection['ring'])], out_shape=shape, transform=transform)
        nodata = src.nodata if src.nodata is not None else (np.nan if src.dtypes[0].startswith('float') else None)
        profile = {
            'driver': 'GTiff', 'dtype': src.dtypes[0], 'count': src.count, 'crs': src.crs,
            'transform': transform, 'width': shape[1], 'height': shape[0], 'nodata': nodata,
            'compress': 'deflate', 'tiled': True, 'blockxsize': 256, 'blockysize': 256,
            'interleave': 'band', 'bigtiff': 'if_safer',
        }
        with rasterio.open(out_path, 'w', **profile) as dst:
            for i in range(1, src.count + 1):
                arr = src.read(i, window=window)
                if outside is not None and nodata is not None:
                    arr[outside] = nodata
                dst.write(arr, i)
                if src.descriptions[i - 1]:
                    dst.set_band_description(i, src.descriptions[i - 1])
            dst.update_tags(**_selection_tags(selection, xmin, ymin, xmax, ymax),
                            metric=metric, **({PARTITION_KEYS[metric]: partition} if partition else {}))
    return True
