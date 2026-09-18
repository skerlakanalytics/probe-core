"""One-shot canton-wide mosaic builder for build_raster_derivate.py's
per-kachel COGs -- merges every precomputed kachel's return_period.tif and
intensity.tif into ONE canton-wide multi-band COG per kind, for probe_
explorer's Raster-Derivate view.

2026-08-27: source COGs for probe_explorer/derivate_bbox_server.py, a small
Tornado service that crops+colorizes a viewport-sized window directly from
these files on every pan/zoom (window snapped to the COG's own native pixel
grid, native LV95, no reprojection -- see that module's docstring). This is
the fourth map-layer approach tried for this view; the first three were
abandoned and their code deleted (2026-08-29): OpenLayers ol/source/GeoTIFF
+ WebGLTileLayer reading a COG directly client-side (hit an unresolved
upstream geotiff.js bug), per-precompute-combo canton PMTiles archives
reprojected to Web Mercator (worked, but visibly misaligned against the
true LV95 5m grid -- Web Mercator's tile lattice isn't LV95's), and
per-kachel exact-alignment "chips" (exact alignment, but a hard viewport cap
meant no usable zoomed-out view). This canton-COG approach fixes both
problems at once: cropping/coloring server-side in native LV95 keeps exact
alignment, and one dynamically-resized image (not kachel-capped) renders
full coverage at any zoom level. See project memory for the full decision
history.

Built via gdalbuildvrt (spatial mosaic, same band count/order as the sources
-- NOT gdalbuildvrt -separate, which stacks different single-band files into
new bands; here every source already has the full band set, just a different
spatial footprint) + rasterio.shutil.copy(..., driver='COG'), rather than an
in-memory rasterio.merge array: canton Bern's real precomputed-kachel extent
is ~86km x 100km, which at 5m resolution is ~344M pixels -- a dense in-memory
array across 18 float32 bands would be ~25GB, not something to hold in RAM.
gdalbuildvrt's VRT is pure XML (references source files, no pixel data);
rasterio.shutil.copy/GDAL's COG driver stream the output block-by-block from
it, bounded by GDAL's own working-set cache, not the whole raster. (Verified
separately: the per-kachel COG driver output already gets real overview
levels built automatically for a large-enough raster -- confirmed against a
synthetic 4000x4000 test file -- so this canton mosaic needs no extra step
beyond the same driver='COG' + overview_resampling profile build_raster_
derivate.py's own writer (maxi_ifk_and_raster._write_multiband) already uses.)

Downloads each kachel's small per-kachel COG locally first (source files
average ~500KB/160KB, thousands of kacheln -> low single-digit GB total)
rather than VRT-referencing S3 presigned URLs directly -- simpler, avoids a
presigned URL expiring mid-run, and keeps gdalbuildvrt working against plain
local paths.

Run this LOCALLY wrapped in a memory/IO cgroup per CLAUDE.md's hard rule for
heavy local IO jobs -- e.g.:
    systemd-run --user --scope -p MemoryHigh=4500M -p MemoryMax=6G \\
      -p "IOReadBandwidthMax=/dev/sdd 150M" -p "IOWriteBandwidthMax=/dev/sdd 150M" \\
      setsid nohup python -m probe_core.derivate.build_raster_derivate_mosaic --upload \\
      > mosaic.log 2>&1 &

Usage:
    python -m probe_core.derivate.build_raster_derivate_mosaic                # local-only smoke test
    python -m probe_core.derivate.build_raster_derivate_mosaic --upload        # + S3
    python -m probe_core.derivate.build_raster_derivate_mosaic --cfg-hash a93cb36a --upload
"""

import argparse
import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import numpy as np
import rasterio
import rasterio.shutil as rio_shutil

from probe_core.s3 import get_s3_client, configure_s3_for_duckdb
from probe_core.derivate.build_raster_derivate import (
    geotiff_prefix, geotiff_filename, config_prefix, list_precomputed_kacheln,
    latest_config_manifest, S3_BUCKET_GOLD,
)

_DOWNLOAD_WORKERS = 16


def canton_mosaic_prefix(cfg_hash: str) -> str:
    return f"{config_prefix(cfg_hash)}/canton_mosaic"


def canton_mosaic_filename(kind: str) -> str:
    return f"canton_{kind}.tif"


def canton_mosaic_stats_filename(kind: str) -> str:
    return f"canton_{kind}_bands.json"


def _compute_band_stats(cog_path, kind):
    """Real per-band min/max across the finished canton mosaic -- probe_
    explorer's OpenLayers preview (2026-08-26 decision, see derivate_map_
    component.py) reads this once to set a fixed color-ramp domain per band,
    since a single global domain across all 33 precompute-matrix combos
    would be meaningless (a 0.25m depth threshold's return periods and a
    2m one's live on completely different scales, same for velocity/
    pressure vs depth). One band read at a time (not the whole multi-band
    array at once) to keep this bounded regardless of canton-mosaic size."""
    stats = []
    with rasterio.open(cog_path) as ds:
        for i, name in enumerate(ds.descriptions, start=1):
            arr = ds.read(i)
            valid = np.isfinite(arr)
            if kind == 'return_period':
                valid &= arr > 0  # matches app.py's _raster_overlay_payload mode 'a'
            if not np.any(valid):
                stats.append({'index': i, 'name': name, 'vmin': None, 'vmax': None})
                continue
            stats.append({
                'index': i, 'name': name,
                'vmin': float(arr[valid].min()), 'vmax': float(arr[valid].max()),
            })
    return {'kind': kind, 'bands': stats}


def _list_precomputed(cfg_hash, has_return_periods, bucket):
    con = duckdb.connect(':memory:', config={'memory_limit': '2GB'})
    configure_s3_for_duckdb(con)
    try:
        return list_precomputed_kacheln(con, cfg_hash, has_return_periods, bucket)
    finally:
        con.close()


def _download_kacheln(bucket, cfg_hash, kind, ids, dest_dir):
    dest_dir.mkdir(parents=True, exist_ok=True)
    s3 = get_s3_client()  # boto3 clients are thread-safe for this kind of shared use

    def _one(id_kachel):
        key = f"{geotiff_prefix(cfg_hash, kind)}/{geotiff_filename(id_kachel, kind)}"
        local = dest_dir / f"kachel_{id_kachel}.tif"
        s3.download_file(bucket, key, str(local))
        return local

    paths = []
    with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(_one, k) for k in ids]
        for i, fut in enumerate(as_completed(futures), start=1):
            paths.append(fut.result())
            if i % 200 == 0 or i == len(ids):
                print(f"  [{kind}] downloaded {i}/{len(ids)}", flush=True)
    return paths


def _build_vrt(src_paths, vrt_path):
    # -input_file_list, not a giant argv of paths -- thousands of files
    # stays well clear of any command-line length concern and matches the
    # documented gdalbuildvrt pattern for many-source mosaics.
    file_list = vrt_path.with_suffix('.filelist.txt')
    file_list.write_text('\n'.join(str(p) for p in src_paths))
    subprocess.run(
        ['gdalbuildvrt', '-input_file_list', str(file_list), str(vrt_path)],
        check=True, capture_output=True, text=True,
    )


def _vrt_to_canton_cog(vrt_path, out_path, band_names):
    # Band descriptions MUST be set on the VRT before conversion, not on the
    # COG after (2026-08-26, caught by a real error): gdalbuildvrt does NOT
    # carry band descriptions from its sources, and a written COG refuses
    # in-place edits at all ("Updating it will generally result in losing
    # part of the optimizations") -- GDAL protects the tiled/overview layout
    # from exactly the kind of reopen-and-patch this first tried to do. A
    # VRT has no such restriction (it's just XML).
    with rasterio.open(vrt_path, 'r+') as vrt_ds:
        for i, name in enumerate(band_names, start=1):
            vrt_ds.set_band_description(i, name)

    # TWO passes, not a direct VRT -> BAND-interleaved-COG in one shot
    # (2026-08-27, caught by three real near-OOM/near-crash incidents on
    # the WSL box in a row -- see CLAUDE.md's hard rule on this). Asking
    # gdal_translate (either driver='COG' with INTERLEAVE=BAND, OR even a
    # flat non-COG GTiff with no overviews at all) directly from a mosaic
    # VRT with ~1,100+ small per-kachel sources is the expensive step no
    # matter the destination format -- it was slow (30+ min, still not
    # done) and memory-hungry (climbing throughout) in every CLI variant
    # tried, while rasterio.shutil.copy() -> driver='COG' from the exact
    # same VRT (the ORIGINAL call, pass 1 below, PIXEL interleave -- the
    # default before BAND was ever introduced) reliably takes ~1-2 minutes
    # with no memory issue. So: keep pass 1 exactly as it always was
    # (proven fast/safe against the many-source VRT), and do the BAND
    # reinterleave as its own pass 2 against that ONE already-merged,
    # already-compressed COG -- a single-source operation, the same cheap
    # cost class as a direct synthetic single-file test (near-instant).
    pixel_path = out_path.with_name(out_path.stem + '_pixel_tmp.tif')
    rio_shutil.copy(
        str(vrt_path), str(pixel_path), driver='COG',
        compress='zstd', blocksize=512, overview_resampling='average',
        bigtiff='IF_SAFER',
    )
    # subprocess'd gdal_translate, NOT rasterio.shutil.copy, for this
    # second pass: copy() silently ignores an explicit INTERLEAVE creation
    # option for the COG driver -- verified directly (a synthetic multi-
    # band file written both ways: copy() with interleave='band' OR
    # INTERLEAVE='BAND' still produced INTERLEAVE=PIXEL, while the CLI tool
    # with the identical -co INTERLEAVE=BAND produced the correct layout
    # AND still propagated band descriptions correctly). BAND, not the COG
    # driver's PIXEL default, matters here: derivate_bbox_server.py always
    # reads ONE band at a time (a single precompute-matrix combo per
    # request) -- under PIXEL every touched block stores all bands
    # together, so a single-band windowed read had to fetch (and GDAL
    # fanned out into many small vsicurl range requests for) all 18/15
    # bands' worth of data per block, measured ~7-11s per crop; under BAND
    # each band's blocks are separate and contiguous, measured ~0.6-0.7s
    # for the same crops, ~15x faster. `--config GDAL_CACHEMAX 512` caps
    # GDAL's own internal block cache explicitly, kept here as a cheap
    # extra safety margin even though pass 2's single-source input makes
    # it far less likely to matter than it did against the raw VRT.
    #
    # ZSTD, not DEFLATE (2026-08-27, both passes): decode speed matters
    # here specifically because derivate_bbox_server.py decompresses on
    # EVERY pan/zoom request, not just once at build time -- ZSTD
    # decompresses meaningfully faster than DEFLATE at a similar-or-better
    # ratio, so this is a strict improvement for the live read path, not a
    # size/speed tradeoff. (LERC's lossy MAX_Z_ERROR tolerance would shrink
    # return_period specifically even further, given its poor ~7x DEFLATE
    # ratio traces to genuinely high-entropy values -- e.g. an observed
    # legend vmax of 1e14 -- not band count; not adopted here since it
    # trades exactness for size, a call worth making deliberately later if
    # storage becomes a real constraint, not a default to reach for now.)
    try:
        subprocess.run(
            ['gdal_translate', '-of', 'COG',
             '--config', 'GDAL_CACHEMAX', '512',
             '-co', 'COMPRESS=ZSTD', '-co', 'BLOCKSIZE=512',
             '-co', 'OVERVIEW_RESAMPLING=AVERAGE', '-co', 'BIGTIFF=IF_SAFER',
             '-co', 'INTERLEAVE=BAND',
             str(pixel_path), str(out_path)],
            check=True, capture_output=True, text=True,
        )
    finally:
        pixel_path.unlink(missing_ok=True)


def build_canton_mosaic(bucket, cfg_hash, kind, ids, work_dir, out_path):
    src_dir = work_dir / f'src_{kind}'
    print(f"[{kind}] downloading {len(ids)} kacheln...", flush=True)
    src_paths = _download_kacheln(bucket, cfg_hash, kind, ids, src_dir)
    with rasterio.open(src_paths[0]) as ds0:
        band_names = list(ds0.descriptions)

    vrt_path = work_dir / f'canton_{kind}.vrt'
    print(f"[{kind}] building VRT mosaic ({len(src_paths)} sources)...", flush=True)
    _build_vrt(src_paths, vrt_path)

    print(f"[{kind}] writing canton COG -> {out_path} ...", flush=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _vrt_to_canton_cog(vrt_path, out_path, band_names)

    size_mb = out_path.stat().st_size / 1e6
    print(f"[{kind}] done: {out_path} ({size_mb:.1f} MB, {len(band_names)} bands)", flush=True)

    print(f"[{kind}] computing per-band min/max...", flush=True)
    stats = _compute_band_stats(out_path, kind)
    stats_path = out_path.with_name(canton_mosaic_stats_filename(kind))
    stats_path.write_text(json.dumps(stats, indent=2))
    print(f"[{kind}] wrote {stats_path}", flush=True)

    # Only the canton mosaic (+ its stats JSON, + the tiny VRT) are useful
    # past this point -- free the redundant local per-kachel source copies.
    shutil.rmtree(src_dir, ignore_errors=True)
    return out_path, stats_path


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=S3_BUCKET_GOLD)
    p.add_argument("--cfg-hash", default=None,
                    help="Default: latest precompute config on S3 (latest_config_manifest)")
    p.add_argument("--work-dir", default="~/probe_data/raster_derivate_mosaic")
    p.add_argument("--limit", type=int, default=None,
                    help="Only mosaic the first N precomputed kacheln (smoke test)")
    p.add_argument("--upload", action="store_true",
                    help="Also write to s3://<bucket>/Data-Lake-Derivate/raster/cfg_<hash>/canton_mosaic/ "
                         "(opt-in, off by default)")
    args = p.parse_args()
    work_dir = Path(args.work_dir).expanduser()
    work_dir.mkdir(parents=True, exist_ok=True)

    manifest = latest_config_manifest(args.bucket)
    if manifest is None:
        raise SystemExit("No raster_derivate config manifest found on S3 -- "
                          "run build_raster_derivate.py --upload first.")
    cfg_hash = args.cfg_hash or manifest['config_hash']
    has_thresholds = bool(manifest['thresholds'])
    has_return_periods = bool(manifest['return_periods'])

    ids = sorted(_list_precomputed(cfg_hash, has_return_periods, args.bucket))
    print(f"cfg_hash={cfg_hash}: {len(ids)} precomputed kacheln", flush=True)
    if not ids:
        raise SystemExit("Nothing precomputed yet for this config.")
    if args.limit:
        ids = ids[:args.limit]
        print(f"--limit {args.limit}: using {len(ids)} kacheln", flush=True)

    results = {}
    for kind, configured in (('return_period', has_thresholds), ('intensity', has_return_periods)):
        if not configured:
            continue
        out_path = work_dir / canton_mosaic_filename(kind)
        results[kind] = build_canton_mosaic(args.bucket, cfg_hash, kind, ids, work_dir, out_path)

    if args.upload:
        s3 = get_s3_client()
        for kind, (cog_path, stats_path) in results.items():
            for local_path in (cog_path, stats_path):
                key = f"{canton_mosaic_prefix(cfg_hash)}/{local_path.name}"
                print(f"uploading -> s3://{args.bucket}/{key} ...", flush=True)
                s3.upload_file(str(local_path), args.bucket, key)


if __name__ == "__main__":
    main()
