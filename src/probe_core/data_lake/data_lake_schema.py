# ── Data lake top-level folder names (disk paths and S3 prefixes) ──────────────
DATA_LAKE_DIR_RAW_SIMS  = "Raw-Sim-Dirs"
DATA_LAKE_DIR_BRONZE    = "Data-Lake-Bronze"
DATA_LAKE_DIR_SILVER    = "Data-Lake-Silver"
DATA_LAKE_DIR_GOLD      = "Data-Lake-Gold"
DATA_LAKE_DIR_RUN_LOGS_RESULTS  = "Run-Logs-Results"
DATA_LAKE_DIR_LOCKS     = "locks"   # batch claim locks (locks/batch_XXXXXXX.lock); kept on success, so
                                    # "in progress" always means: lock exists AND silver parquet doesn't

# ── Done-range markers (Data-Lake-Range-Done/batch_XXXX.marker) ────────────────
# locks/ and Data-Lake-Silver/ both grow forever (locks are never deleted, even
# on success — see DATA_LAKE_DIR_LOCKS above), so any full listing of them
# (manage_jobs.py's stale-lock scan, the progress service's done/wip counts)
# gets slower as the campaign progresses — the opposite of what you want.
# batch dir names are 7-digit zero-padded (batch_XXXXXXX, see batch_worker.py's
# batch_dir_name), so a bare 4-digit prefix (batch_XXXX) already covers exactly
# 1000 consecutive batch IDs with no separate bucketing math — S3's own prefix
# listing does the grouping for free. Once a range is confirmed fully done (all
# its batch IDs have silver — checked by data_lake/mark_done_batch_ranges.py,
# the only writer), a marker lets every listing consumer skip that range
# entirely: cost shrinks as the campaign completes instead of growing forever.
# Markers are permanently valid once written (silver files are never deleted),
# so no staleness/invalidation to worry about.
DATA_LAKE_DIR_RANGE_DONE = "Data-Lake-Range-Done"
BATCH_RANGE_SIZE = 1000

# ── QA fleet coordination locks/results ─────────────────────────────────────
# Data-Lake-QA-Locks/<job_name>/kachel_<id>.lock,
# Data-Lake-QA-Results/<job_name>/kachel_<id>_<status>_r<retry_count>.json —
# generic S3 claim/result coordination for fleet-wide QA sweeps
# (data_lake/qa_s3_lock.py), used by qa_compare_sim_spatial_vs_sim_anriss.py's
# --distributed mode (2026-09-11) and meant to be reused by future QA jobs
# (a planned silver<->gold parameter-combination check) under their own
# job_name sub-namespace, sharing the same bucket/module.
#
# Same claim/verify race-detection pattern as DATA_LAKE_DIR_LOCKS above
# (_claim_s3_batch_lock) — a check-then-put lock isn't atomic on this storage
# backend the same way, so a worker writes a unique token, waits, reads back,
# and only trusts the claim if its own token is still there. Same "kept on
# success, growing forever" semantics too: a lock is released ONLY on a
# non-terminal result (to allow retry); PASS/FAIL locks are left in place
# forever, harmless since the result object (not the lock) is what blocks
# re-picking a terminal kachel. No staleness/expiry — deliberate choice
# (2026-09-11): a dead node's lock sits forever until manually cleared
# (data_lake/qa_s3_lock.py's clear_all_locks); recovery is "delete all
# locks, rerun" rather than an automatic timeout, matching the main
# pipeline's own lack of lock expiry.
#
# Status and retry_count are encoded in the result filename itself (not just
# the JSON body) so the fleet-wide picker can determine every kachel's
# eligibility from one cheap LIST call, with zero GETs on the hot picking
# path — only inspecting a specific failure's full JSON body needs a GET.
DATA_LAKE_DIR_QA_LOCKS   = "Data-Lake-QA-Locks"
DATA_LAKE_DIR_QA_RESULTS = "Data-Lake-QA-Results"

# ── Derivate artifacts (Data-Lake-Derivate/<name>.parquet) ─────────────────────
# Products computed FROM the lake but not part of it (spatial lookups, exports,
# ...). Opt-in upload only, like everything else that writes to the bucket —
# see derivate/create_hull_all_anrisse.py for the one-shot/smoke-test hull
# lookup and derivate/build_hull_fragments.py for the incremental per-range
# one data_interface.py's get_anriss_hull_fragment() reads.
DATA_LAKE_DIR_DERIVATE = "Data-Lake-Derivate"
DATA_LAKE_DIR_DERIVATE_HULLS = f"{DATA_LAKE_DIR_DERIVATE}/anriss_umhuellende"
# Batch-precomputed return-period/intensity raster derivate (derivate/build_raster_derivate.py),
# Hive-partitioned by a config-hash prefix then id_kachel — see that module's
# docstring for why the config hash is part of the path (no ledger; a changed
# precompute matrix is a new prefix, not an in-place rewrite of an old one).
DATA_LAKE_DIR_DERIVATE_RASTER = f"{DATA_LAKE_DIR_DERIVATE}/raster"
# Canton-wide binary "affected by any simulation" mask (derivate/build_affected_
# mask.py + derivate/build_affected_mask_mosaic.py) — per-kachel full 200x200 px
# uint8 0/1 GeoTIFFs (1 = pixel present in SIM_SPATIAL, i.e. hit by at least one
# scenario, 0 = confirmed not hit within that finalized kachel) plus a canton
# mosaic, same gdalbuildvrt approach as the raster derivate's canton_mosaic. No
# config hash (there's nothing to configure) — a flat prefix, not Hive-partitioned
# by id_kachel like DERIVATE_RASTER (see build_affected_mask.py's docstring).
DATA_LAKE_DIR_DERIVATE_AFFECTED_MASK = f"{DATA_LAKE_DIR_DERIVATE}/affected_mask"

# Compacted Run-Logs-Results (data_lake/compact_run_logs_results.py,
# 2026-09-15): DATA_LAKE_DIR_RUN_LOGS_RESULTS has 2 files per batch (a
# results.csv + a .log), ~1.23M batches -> ~2.45M small objects, expensive
# to even list let alone analyze. Compacted per 1000-batch range (same
# batch_range_prefix() unit as everywhere else in this pipeline) into its
# own top-level bucket folder (a sibling of Run-Logs-Results, not nested
# under Data-Lake-Derivate -- decision Bojan 2026-09-15, folder created by
# hand ahead of the job):
#   results/batch_XXXX_results.parquet -- every batch's results.csv in that
#     range unioned into one Parquet file (DuckDB read_csv, handles the
#     quoted-comma stop-criteria messages correctly, unlike a naive awk
#     split -- see CLAUDE.md's CSV-parsing gotcha) -- this is the real
#     analysis payload (per-sim duration/status/stop_criterion).
#   logs/batch_XXXX.log.gz -- that range's .log files concatenated and
#     gzipped, not restructured into rows (free-form DEBUG-heavy text, not
#     tabular) -- kept for archival/deep-debugging, still zgrep-able.
# Turns ~2.45M objects into ~2,454 (1,227 ranges x 2 outputs).
DATA_LAKE_DIR_RUN_LOGS_RESULTS_COMPACTED = "Run-Logs-Results-Compacted"
DATA_LAKE_DIR_RUN_LOGS_RESULTS_COMPACTED_PARQUET = f"{DATA_LAKE_DIR_RUN_LOGS_RESULTS_COMPACTED}/results"
DATA_LAKE_DIR_RUN_LOGS_RESULTS_COMPACTED_LOGS_GZ = f"{DATA_LAKE_DIR_RUN_LOGS_RESULTS_COMPACTED}/logs"

# ── Probabilities/rates (Data-Lake-Probabilities-Rates/<name>) ─────────────────
# Not derivates of the simulation lake (they don't come FROM gold) — the
# per-id_anriss probability lookup and the rheology-rate table are both
# inputs the gold join consumes, so they get their own top-level folder
# rather than living under Data-Lake-Derivate (moved out 2026-08-20).
#
# probability_lookup.parquet (derivate/build_probability_lookup.py):
# lambda_Hangmuren, p_raeumlich, p_A straight from the Xurce delivery — a
# single small parquet (~61M rows x 3 floats), NOT a Hive/range-partitioned
# artifact like the hull fragments or SIM_ANRISS. Unlike those, this has no
# incremental dependency on simulation progress (the Xurce delivery is
# already fully and statically delivered), so it's a plain one-shot
# rebuild, not a trailing worker. p_h needs no lookup at all — it's a CASE
# on gold's own h/d columns (see GOLD_P_H_MEAN/GOLD_P_H_MAX below) — and
# p_Ablauf is ablaufwahrscheinlichkeiten.csv below (mu/xsi/tau0-keyed).
# Consumers join gold SIM_SPATIAL/SIM_ANRISS against this lookup (reduced to
# the touched id_anriss first, same pattern as data_lake_gold_worker.py's
# kachel_lookup) to compute lambda_Ereignis at query time — decision 2026-08-13:
# rejected a fully materialized ENRICHED gold lake (would duplicate these
# anriss-level scalars across every pixel row and force a full lake rewrite
# on every probability correction) in favor of this small, cheaply-
# rebuildable lookup. GOLD_SCHEMA_ENRICHED_DUCKDB below stays unused.
#
# ablaufwahrscheinlichkeiten.csv: the tiny rheology-combination probability
# table (mu/xsi/tau0-keyed, 36 rows, sums to 1). Was git-tracked under
# input/ with this S3 copy as a mere "convenience mirror" until 2026-09-13
# -- unified to S3 being the single source of truth (decision: every gold
# bucket needs its own copy anyway, since it's read alongside the
# per-bucket gold data via the same read path as probability_lookup.parquet;
# a second local copy just meant remembering to keep both in sync, which
# already caused a real bug for a region-scoped test bucket that had the
# S3 copy missing). Edit by uploading a new version directly; there's no
# local file to edit anymore.
DATA_LAKE_DIR_PROBABILITIES_RATES = "Data-Lake-Probabilities-Rates"
DATA_LAKE_PROBABILITIES_RATES_LOOKUP = f"{DATA_LAKE_DIR_PROBABILITIES_RATES}/probability_lookup.parquet"
DATA_LAKE_PROBABILITIES_RATES_ABLAUF = f"{DATA_LAKE_DIR_PROBABILITIES_RATES}/ablaufwahrscheinlichkeiten.csv"


# ── Gebäudeschatten (2026-09-04) ────────────────────────────────────────────
# Second, much simpler simulation campaign alongside MAXI: release areas come
# directly from geo7-delivered building-footprint polygons (input/geo7_*_
# gebaeudeschatten.gdb, one or more "starts_<municipality>" layers, auto-
# discovered by that prefix) instead of a circle derived from an (X, Y, area)
# anriss point. Same 800m DEM/simulation-domain cap as MAXI (workers/
# gebaeudeschatten_manager.py, unchanged MAX_BUFFER_FROM_CENTER logic just
# anchored on the release polygon's bbox instead of a point+radius) and same
# bronze/silver/gold *shape* (GEBAEUDESCHATTEN_SCHEMA_SIM_DUCKDB mirrors
# GOLD_SCHEMA_SIM_DUCKDB), but a fully separate lake root — not mixed into
# MAXI's Data-Lake-* trees, per Bojan (two very separate outputs).
DATA_LAKE_DIR_GEBAEUDESCHATTEN_RAW_SIMS = "Gebaeudeschatten-Raw-Sim-Dirs"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_BRONZE = "Gebaeudeschatten-Data-Lake-Bronze"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_SILVER = "Gebaeudeschatten-Data-Lake-Silver"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD   = "Gebaeudeschatten-Data-Lake-Gold"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD_STAGING = "Gebaeudeschatten-Data-Lake-Gold-Staging"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD_LEDGER  = "Gebaeudeschatten-Data-Lake-Gold-Ledger"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_RUN_LOGS_RESULTS = "Gebaeudeschatten-Run-Logs-Results"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_LOCKS = "gebaeudeschatten-locks"  # separate prefix from DATA_LAKE_DIR_LOCKS -- MAXI's batch_XXXXXXX.lock names could otherwise collide with this campaign's own batch numbering
DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE = "Gebaeudeschatten-Data-Lake-Derivate"
DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_AFFECTED_MASK = f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE}/affected_mask"
# Three "affected" definitions per reach variant (2026-09-08, decision Bojan),
# each its own sibling prefix under affected_mask/ -- same "keep derived
# views apart" reasoning as SIM_SPATIAL vs SIM_SPATIAL_ZEROED:
#   raw                          -- any row present in SIM_SPATIAL, no filter
#   shadowing_building           -- SIM_SPATIAL_ZEROED, own_building_mask != 1
#                                    (a building's own footprint doesn't count
#                                    as affected, but its neighbor ring does)
#   shadowing_building_surrounding -- SIM_SPATIAL_ZEROED, own_building_mask = 0
#                                    (footprint AND neighbor ring excluded)
# All three are evaluated PER ROW (per id_start) -- a pixel excluded by one
# building's own_building_mask still counts as affected if a DIFFERENT
# building's row at that same pixel isn't excluded (SELECT DISTINCT x, y
# WHERE ... naturally has this property: it filters rows, not locations, so
# any one passing row at a location is enough). See workers/gebaeudeschatten_
# gold_worker.py's own_building_mask docstring for the AvaFrame-fallback
# rationale behind the mask values themselves.
GEBAEUDESCHATTEN_AFFECTED_MASK_VARIANTS = ("raw", "shadowing_building", "shadowing_building_surrounding")

# Minimum Fliesstiefe (gold's rounded-to-cm depth column) for a row to count
# as "affected" at all, across all three variants above (2026-09-10, decision
# Bojan). Before this, "affected" meant "any SIM_SPATIAL row present, however
# shallow" -- caught live via a real QGIS inspection: a neighboring building's
# flow reaching a pixel at 1.38mm depth (silver's raw float, rounds to
# Fliesstiefe=0) still counted as "affected", a numerically-real but
# practically-negligible trace, not a meaningful hazard signal. 1cm sits well
# below Swiss Hangmuren intensity-class boundaries (low intensity typically
# starts closer to 10cm) -- it filters numerical noise/wetting at the flow's
# leading edge without reframing "affected" into a hazard-severity threshold.
# Measured against the full campaign's real depth distribution before
# picking this value: 11.7% of previously-"affected" (id_start, x, y) rows
# fall under 1cm (430,211 of 3,674,797 silver rows).
GEBAEUDESCHATTEN_AFFECTED_MASK_MIN_DEPTH_CM = 1

# Deliverable to geo7 needs the reach capped much tighter than MAXI's runouts
# (buildings, not torrent Anrisse) at two different distances they want to
# compare — simulation itself is unchanged (still the full 800m domain), only
# gold's reach filter differs, so this is three parallel SIM_SPATIAL/SIM_ANRISS
# pairs under one gold root rather than separate campaigns (decision
# Bojan 2026-09-04; third 800m/uncapped variant added 2026-09-07 -- 800
# equals the simulation domain cap itself, so it acts as "no filter"). Add to
# this tuple, don't replace, if a further variant is ever requested — every
# consumer keyed off it (gold compact, the affected-mask derivate) iterates
# it rather than hardcoding specific values.
GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M = (800, 100, 50)


def gebaeudeschatten_gold_reach_dir(reach_m: int) -> str:
    """S3 prefix segment for one gold reach variant, e.g. 100 -> 'REACH_100M'."""
    assert reach_m in GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M, (
        f"reach_m={reach_m} not in GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M={GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M}")
    return f"REACH_{reach_m}M"


def gebaeudeschatten_gold_sim_spatial_dir(reach_m: int) -> str:
    return f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD}/{gebaeudeschatten_gold_reach_dir(reach_m)}/SIM_SPATIAL"


def gebaeudeschatten_gold_sim_spatial_zeroed_dir(reach_m: int) -> str:
    """Own-building-masked variant of SIM_SPATIAL (2026-09-07) -- same rows,
    same reach filter, but Fliesstiefe/Fliessgeschwindigkeit/Druck zeroed
    wherever own_building_mask (see GEBAEUDESCHATTEN_SCHEMA_SIM_ZEROED_DUCKDB)
    is 1 (pixel inside the row's OWN release building) or 2 (touches it).
    Deliberately a SEPARATE dataset, not an in-place SIM_SPATIAL rewrite --
    SIM_SPATIAL stays the pure, never-rewritten physics table (same
    philosophy as MAXI's SIM/ENRICHED split, see CLAUDE.md), so a flow from
    one building that geographically reaches a DIFFERENT building stays
    visible there (own-building only, not any-building -- see
    DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_BUILDING_MASK for the separate
    any-building visualization raster, which is unrelated to this zeroing)."""
    return f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD}/{gebaeudeschatten_gold_reach_dir(reach_m)}/SIM_SPATIAL_ZEROED"


def gebaeudeschatten_gold_sim_anriss_dir(reach_m: int) -> str:
    return f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_GOLD}/{gebaeudeschatten_gold_reach_dir(reach_m)}/SIM_ANRISS"


# Building-polygon manifest (ingestion output, one row per release polygon
# across every auto-discovered starts_* layer -- see pre_processing/
# ingest_gebaeudeschatten_starts.py). id_start is campaign-wide unique, built
# from a hash of (layer_name, ORIG_FID) rather than a hand-maintained
# municipality code, so a future delivery with new municipality layers needs
# no registration step -- decision Bojan 2026-09-04, and it's fine for these
# IDs to differ between this pilot and any later full-canton rebuild since
# the manifest is the only place they're defined (no cross-run identity to
# preserve). x_start/y_start are the release polygon's centroid, kept
# alongside its full geometry (WKB) for tooling that only needs a point (DEM
# cache keying, coarse spatial filters); geometry itself is what's actually
# rasterized into the release area, not a circle derived from area.
GEBAEUDESCHATTEN_MANIFEST_SCHEMA_DUCKDB = {
    "id_start": "BIGINT",
    "layer": "VARCHAR",       # source gdb layer, e.g. "starts_krauchtal"
    "orig_fid": "INTEGER",    # ORIG_FID within that layer -- NOT unique: geo7's
                              # -2m buffer can split one building into several
                              # disjoint polygons sharing the same ORIG_FID
                              # (confirmed 2026-09-04); id_start disambiguates
    "area": "DOUBLE",         # m^2, from the polygon geometry itself (SHAPE_Area)
    "x_start": "DOUBLE",      # polygon centroid
    "y_start": "DOUBLE",
    "xmin": "DOUBLE",         # polygon bbox, LV95
    "xmax": "DOUBLE",
    "ymin": "DOUBLE",
    "ymax": "DOUBLE",
    "geometry_wkb": "BLOB",
}
GEBAEUDESCHATTEN_MIN_AREA_M2 = 5.0  # below this, the delivered (already -2m-buffered) polygon is a sliver -- skipped, not simulated

# Same physical-pixel columns as GOLD_SCHEMA_SIM_DUCKDB, minus columns that
# don't apply here: no id_prozessquelle/d (no Xurce manifest join, no
# Bodengruendigkeit -- this campaign has no probability/enrich phase planned
# at all, unlike MAXI's SIM/ENRICHED split).
GEBAEUDESCHATTEN_SCHEMA_SIM_DUCKDB = {
    "id_start": "BIGINT",
    "x_start": "DOUBLE",                 # silver release-polygon centroid
    "y_start": "DOUBLE",
    "A": "INTEGER",                      # release polygon area [m^2]
    "h": "INTEGER",                      # Anrissmaechtigkeit [cm] (relTh)
    "mu": "DECIMAL(7,3)",
    "xsi": "INTEGER",
    "tau0": "INTEGER",
    "x": "FLOAT",
    "y": "FLOAT",
    "Fliesstiefe": "INTEGER",            # max. Fliesstiefe [cm] = ROUND(depth*100)
    "Fliessgeschwindigkeit": "INTEGER",  # max. Fliessgeschwindigkeit [cm/s] = ROUND(velocity*100)
    "Druck": "INTEGER",                  # Scheidegger [kPa], same PRESSURE_RHO/GRAVITY as MAXI
}

# SIM_SPATIAL_ZEROED schema (2026-09-07): same columns as
# GEBAEUDESCHATTEN_SCHEMA_SIM_DUCKDB plus own_building_mask -- checked
# against the row's OWN id_start's release polygon only (not any building),
# so a flow reaching a DIFFERENT building stays visible/unzeroed there.
# 0 = outside the building and not touching it, 1 = pixel center inside the
# building's own footprint, 2 = one of its 8 grid-neighbors (5m lattice) is
# inside the footprint (own cell isn't) -- both 1 and 2 get Fliesstiefe/
# Fliessgeschwindigkeit/Druck zeroed in this dataset; SIM_SPATIAL itself is
# never rewritten. See gebaeudeschatten_gold_sim_spatial_zeroed_dir().
GEBAEUDESCHATTEN_SCHEMA_SIM_ZEROED_DUCKDB = {
    **GEBAEUDESCHATTEN_SCHEMA_SIM_DUCKDB,
    "own_building_mask": "TINYINT",
}

# Any-building visualization raster (2026-09-07): a per-kachel GeoTIFF answering
# "is this 5m cell inside/touching ANY building" (unlike own_building_mask
# above, which is scoped per id_start for the zeroing use case) -- purely a
# map backdrop for probe_explorer, unrelated to gold's zeroing logic. Same
# 0/1/2 encoding, 1 supersedes 2 where both any-building conditions would
# apply. See derivate/build_gebaeudeschatten_building_mask.py.
DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_BUILDING_MASK = f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE}/building_mask"

# Silver-side sim key (silver column names: area/relTh) vs. gold-side
# (renamed A/h) -- same distinction as GOLD_SIM_KEY vs GOLD_SIM_KEY_FROM_GOLD
# above; scatter reads/aggregates silver, compact writes gold, so each needs
# its own column names for the same logical key.
GEBAEUDESCHATTEN_GOLD_SIM_KEY = ["id_start", "area", "relTh", "mu", "xsi", "tau0"]
GEBAEUDESCHATTEN_GOLD_SIM_KEY_FROM_GOLD = "id_start, A AS area, h AS relTh, mu, xsi, tau0"

# Silver-level schema (workers/gebaeudeschatten_silver_worker.py's own
# silver_staging table) -- LAKE_SCHEMA_DUCKDB's analog. id_start/x_start/
# y_start instead of id_anriss/X_rel_center/Y_rel_center; otherwise
# identical shape (same bronze source -- avaframe/in2Trans/parquetUtils.py's
# AVAFRAME_SCHEMA/Hive layout is shared code, see gebaeudeschatten_manager.
# sim_dirname's docstring).
GEBAEUDESCHATTEN_LAKE_SCHEMA_DUCKDB = {
    "id_kachel": "INTEGER",
    "id_start": "BIGINT",
    "x_start": "DOUBLE",
    "y_start": "DOUBLE",
    "area": "INTEGER",
    "relTh": "INTEGER",
    "mu": "DECIMAL(7,3)",
    "xsi": "INTEGER",
    "tau0": "INTEGER",
    "x": "FLOAT",
    "y": "FLOAT",
    "depth": "FLOAT",
    "velocity": "FLOAT",
    "spatial_checksum": "BIGINT",
}


def batch_range_id(batch_id: int) -> int:
    """Which 1000-wide range a batch ID falls into (0-based)."""
    return batch_id // BATCH_RANGE_SIZE


def batch_range_prefix(range_id: int) -> str:
    """S3 key prefix covering one range's batch IDs (4-digit zero-padded —
    batch dir names are 7-digit, so this leaves exactly 3 digits = 1000 IDs)."""
    return f"batch_{range_id:04d}"

# ── Per-batch staging names (under Simulations/batch_XXXX/) ───────────────────
BATCH_BRONZE_DIRNAME  = "batch_data_lake_bronze"
BATCH_SILVER_DIRNAME  = "batch_data_lake_silver"
BATCH_SILVER_FILENAME = "batch_merged_silver.parquet"

# ── Shipped-file naming conventions ───────────────────────────────────────────
BATCH_SILVER_S3_SUFFIX = "silver.parquet"   # each batch ships as {batch_name}_silver.parquet

# ── Per-batch log and results files (local name inside batch dir, renamed on ship) ──
BATCH_LOG_FILENAME     = "batch.log"          # shipped as {batch_name}.log
BATCH_RESULTS_FILENAME = "batch_results.csv"  # shipped as {batch_name}_results.csv

# ── Gold: two SIM layouts, same schema (GOLD_SCHEMA_SIM_DUCKDB), same rows —
# just sorted/partitioned differently for two different access patterns.
# SIM_SPATIAL (Data-Lake-Gold/SIM_SPATIAL/id_kachel=NNNNNNNN/data.parquet):
# per-km² Hive lake, written continuously by the finalize trailing worker
# (data_lake/run_silver_to_gold_loop.sh, workers/data_lake_gold_worker.py) as
# kacheln close — the bbox/raster access path (get_area_gold_data,
# get_pixel_gold_data, and get_anriss_all_scenarios_gold_data's bbox filter).
# SIM_ANRISS (Data-Lake-Gold/SIM_ANRISS/batch_XXXX_sim_anriss.parquet):
# id_anriss-first sorted, one file per 1000-batch range (data_lake/
# build_silver_to_gold_anriss.py, same batch-range keying as anriss_umhuellende
# hull fragments) — the "open one specific event" access path. Decision
# 2026-08-12 (see docs/claude-memory/project_gold_row_group_size_benchmark.md):
# measured 33.7x less data / 5.2x faster than SIM_SPATIAL for exact-id_anriss
# lookups, growing to ~76x for large-footprint events, at the cost of
# roughly doubling total Gold-SIM storage (a real second copy, not an index).
# Renamed from the original bare "SIM" 2026-08-12 — safe to do as a clean
# rename (not a live-data migration) because the existing SIM data is being
# discarded/rebuilt anyway following the 800m boundary-creep fix (see
# GOLD_PIXEL_MAX_REACH_M below) and the p_A Xurce delivery fix.
# Both immutable forever once written — physics never changes once computed,
# no probabilities involved. A future ENRICHED_SPATIAL/ENRICHED_ANRISS pair
# (GOLD_SCHEMA_ENRICHED_DUCKDB) will join probabilities onto SIM_ANRISS
# first (its sort key already matches the join key), then re-derive
# ENRICHED_SPATIAL from that rather than re-joining independently — decision
# 2026-08-12, keeps probability-correction logic in one place. Still built as
# a separate later stage, not baked into either SIM build inline: probability
# inputs (Xurce delivery) are external and correctable, SIM output isn't —
# same rationale as the original 2026-07-19 SIM/ENRICHED split, see
# docs/claude-memory/project_gold_kachel_design.md.
DATA_LAKE_DIR_GOLD_STAGING = "Data-Lake-Gold-Staging"   # scatter fragments
DATA_LAKE_DIR_GOLD_LEDGER  = "Data-Lake-Gold-Ledger"    # resume + audit state
DATA_LAKE_DIR_GOLD_SIM_SPATIAL = f"{DATA_LAKE_DIR_GOLD}/SIM_SPATIAL"
DATA_LAKE_DIR_GOLD_SIM_ANRISS  = f"{DATA_LAKE_DIR_GOLD}/SIM_ANRISS"
GOLD_HIVE_KEY        = "id_kachel"
GOLD_KACHEL_FILENAME = "data.parquet"
# 500_000, not the original 50_000 (decision 2026-08-11, see benchmark in
# docs/claude-memory/project_gold_row_group_size_benchmark.md): below ~150k rows/group,
# per-row-group Parquet footer/stats overhead and worse compression dominate query cost
# regardless of event footprint size (measured ~4-5x slower, more bytes touched, than
# 500k, across tiny/small/medium/large events on real production kacheln). Only
# near-full-tile events get worse past ~150k (bounded ~2x over-fetch by ~500k-750k before
# partially recovering at 1M+) -- 500k is past the overhead cliff without being deep into
# that regime. NOT yet applied to already-finalized Gold-SIM kacheln on S3 -- this only
# takes effect for kacheln finalized after this change ships; rewriting existing kacheln
# is a separate, explicit migration decision.
GOLD_ROW_GROUP_SIZE  = 500_000
GOLD_ZSTD_LEVEL      = 9      # final kachel files are write-once/read-many

# Scheidegger (1975) total pressure in kN/m²: RHO*G*(depth + v²/(2G))/1000.
# Source of truth for both constants is the com1DFA config template (the same
# values the simulations ran with); data_lake_gold_worker.py imports them from
# here so gold's stored Druck is computed with the same constants everywhere.
import configparser
from importlib.resources import files

_com1dfa_cfg = configparser.ConfigParser(interpolation=None)
# Shipped inside the package (probe_core/data/); read_string instead of read()
# because read() silently ignores a missing file and would leave rho/gravAcc
# undefined until the getfloat below fails with a less obvious error.
_com1dfa_cfg.read_string(
    files("probe_core").joinpath("data", "cfgCom1DFA_template.ini").read_text(encoding="utf-8"))
PRESSURE_RHO = _com1dfa_cfg.getfloat("GENERAL", "rho")      # kg/m³ (1800: debris flow)
GRAVITY      = _com1dfa_cfg.getfloat("GENERAL", "gravAcc")  # m/s²

# In-file gold columns; id_kachel lives only in the Hive directory name.
# Gold speaks the delivery language (decision Bojan 2026-07-16): silver names
# (area, relTh, depth, …) end at silver; gold renames to A, h, Fliesstiefe, …
# Intensities are quantized: Fliesstiefe [cm] and Fliessgeschwindigkeit [cm/s]
# as integers (ROUND(depth*100) etc.); Druck [kPa] is Scheidegger computed
# FROM THE ROUNDED values, so the file is recomputable from its own columns.
# P-columns: lambda_Hangmuren / p_raeumlich / p_A come from the Xurce delivery
# (p_A still has ~1.07M NULLs pending their fix), p_h from GOLD_P_H below;
# p_Ablauf and lambda_Ereignis (= product of the five factors) stay NULL until the
# runout weights are defined — the enrich phase fills them.

# Column-width choices below aren't arbitrary -- both schemas are read
# together against the same silver/gold pipeline, so the two width decisions
# below (float precision, int width) apply identically to GEBAEUDESCHATTEN's
# own copy of this shape (GEBAEUDESCHATTEN_SCHEMA_SIM_DUCKDB above) too:
#
# x_anriss/y_anriss are DOUBLE, x/y are FLOAT -- not an oversight, the two
# pairs are used differently. x_anriss/y_anriss (silver X_rel_center/
# Y_rel_center) are compared with exact float equality as join/identity keys
# throughout workers/data_lake_silver_worker.py (e.g. "AND d.X_rel_center =
# v.X_rel_center", "WHERE X_rel_center = {p['X']}") -- at LV95 magnitudes
# (~2,600,000) FLOAT's ~7 significant digits already carries ~0.1-0.3m of
# representable error, enough to silently break such a join; DOUBLE's 53-bit
# mantissa doesn't. x/y (per-pixel raster coords) are never compared this
# way -- they're read off the fixed 5m LV95 grid (see LV95 grid-phase note,
# docs/claude-memory/project_lv95_grid_phase_bug.md) purely for spatial
# filtering/plotting, where FLOAT's sub-meter precision is already more than
# enough. Pixel rows vastly outnumber anriss rows per simulation, so this is
# also where the width actually matters for storage.
#
# h is INTEGER, not SMALLINT, even though it's bounded well inside SMALLINT's
# range in practice: h = d (h_type 'mean') or round(1.5*d) (h_type 'max'),
# see pre_processing_MAXI.py, and d itself is already stored as SMALLINT
# (:440/:461 below) -- so h could be narrowed too. Left as INTEGER because
# SIM_SPATIAL/SIM_ANRISS is documented immutable-forever once written and the
# MAXI campaign's gold lake is already fully finalized and QA-passed
# (docs/claude-memory/project_maxi_qa_campaign_complete_2026_09_16.md) --
# narrowing the constant here wouldn't retroactively narrow the already-
# written parquet, so it wouldn't save anything without a full rewrite of
# finished production data, for a marginal ZSTD-9 win given d is already
# narrow next to it. Revisit for GEBAEUDESCHATTEN specifically (still an
# active, non-finalized campaign) if storage becomes a concern there.

# Future schema after enriched
GOLD_SCHEMA_ENRICHED_DUCKDB = {
    "id_prozessquelle": "SMALLINT",      # manifest join on id_anriss
    "lambda_Hangmuren": "FLOAT",         # Xurce delivery, per anriss
    "id_anriss": "BIGINT",
    "x_anriss": "DOUBLE",                # silver X_rel_center (release centroid)
    "y_anriss": "DOUBLE",                # silver Y_rel_center
    "p_raeumlich": "FLOAT",              # Xurce delivery, per anriss
    "A": "INTEGER",                      # Anrissfläche [m²] (silver area)
    "p_A": "FLOAT",                      # Xurce delivery p_a, per anriss (NULLs pending fix)
    "d": "SMALLINT",                     # Bodengründigkeit [cm] (manifest bodengruendigkeit)
    "h": "INTEGER",                      # Anrissmächtigkeit [cm] (silver relTh = manifest h_cm)
    "p_h": "FLOAT",                      # GOLD_P_H weight: h == d → mean, else max
    "mu": "DECIMAL(7,3)",
    "xsi": "INTEGER",
    "tau0": "INTEGER",
    "p_Ablauf": "FLOAT",                 # NULL until runout weights defined (enrich)
    "lambda_Ereignis": "FLOAT",          # NULL until p_Ablauf exists (enrich)
    "x": "FLOAT",
    "y": "FLOAT",
    "Fliesstiefe": "INTEGER",            # max. Fliesstiefe [cm] = ROUND(depth*100)
    "Fliessgeschwindigkeit": "INTEGER",  # max. Fliessgeschwindigkeit [cm/s] = ROUND(velocity*100)
    "Druck": "INTEGER",                    # Scheidegger [kPa] from the rounded cm values, itself rounded
}

GOLD_SCHEMA_SIM_DUCKDB = {
    "id_prozessquelle": "SMALLINT",      # manifest join on id_anriss
    "id_anriss": "BIGINT",
    "x_anriss": "DOUBLE",                # silver X_rel_center (release centroid)
    "y_anriss": "DOUBLE",                # silver Y_rel_center
    "A": "INTEGER",                      # Anrissfläche [m²] (silver area)
    "d": "SMALLINT",                     # Bodengründigkeit [cm] (manifest bodengruendigkeit)
    "h": "INTEGER",                      # Anrissmächtigkeit [cm] (silver relTh = manifest h_cm)
    "mu": "DECIMAL(7,3)",
    "xsi": "INTEGER",
    "tau0": "INTEGER",
    "x": "FLOAT",
    "y": "FLOAT",
    "Fliesstiefe": "INTEGER",            # max. Fliesstiefe [cm] = ROUND(depth*100)
    "Fliessgeschwindigkeit": "INTEGER",  # max. Fliessgeschwindigkeit [cm/s] = ROUND(velocity*100)
    "Druck": "INTEGER",                    # Scheidegger [kPa] from the rounded cm values, itself rounded
}

# h_type weights for MAXI (mean/max only — decision Bojan 2026-07-16).
# MAXI manifest: h == d ⇔ h_type 'mean'. Not valid for MIDI-era data (3 h_types).
GOLD_P_H_MEAN = 0.8
GOLD_P_H_MAX  = 0.2

# ── Silver ↔ Gold content checksum ─────────────────────────────────────────────
# Proves "every silver row is in gold exactly once, content-identical" without
# ever comparing the two datasets row by row:
#   1. hash(col1, col2, ...) maps one row's passthrough columns to a 64-bit value.
#   2. SUM(hash(...)) over rows fingerprints the row *multiset*: the sum doesn't
#      care about row order or how rows are split across silver batches / gold
#      kacheln — sums of parts recombine to the same total.
#   3. Grouped per simulation (GOLD_SIM_KEY), the silver-side aggregates
#      (computed during scatter, while silver is being read anyway) must equal
#      the gold-side aggregates (computed from the written kachel files).
#      Equal COUNT + equal hash-sum per sim ⇒ same rows (collision ~2^-64/sim).
# Hashed are the columns gold carries over from silver, expressed in each
# side's own dialect: silver hashes the SAME rounding gold will store
# (ROUND(depth*100) → Fliesstiefe [cm] etc.), so equal hash sums prove the
# rounded content — derived columns (Druck, manifest/P joins) are instead
# verified by recomputation at compact time.
# 2026-08-20: extended to also hash the sim-identity columns (id_anriss/area
# resp. A/relTh resp. h/mu/xsi/tau0), not just the pixel-physics columns —
# closes a gap where a bug that mis-attributed a pixel's x/y/depth/velocity
# to the WRONG simulation's parameters (while leaving pixel content correct)
# could pass this fingerprint unnoticed, since those identity columns didn't
# used to affect the hash at all. Triggered by a QA design review of
# data_lake/qa_compare_sim_spatial_vs_sim_anriss.py, which reuses this same
# expression to cross-check SIM_SPATIAL against SIM_ANRISS. This changes the
# hash VALUE, not just its scope — any ledger with fragments computed under
# the old formula must be fully wiped and rebuilt, never mixed (see
# docs/claude-memory/project_gold_kachel_design.md).
# hash() is type-sensitive (hash(1.5::FLOAT) != hash(1.5::DOUBLE)), hence the
# explicit casts, identical on both sides. The sum is stored modulo a prime
# < 2^63 because parquet has no int128; partial sums recombine under the same
# modulus ((a+b) mod p == ((a mod p) + (b mod p)) mod p).
GOLD_AUDIT_HASH_MODULUS = 9223372036854775783
_GOLD_AUDIT_HASH_SUM = "(SUM(hash({cols})::HUGEINT) %% %d)::BIGINT" % GOLD_AUDIT_HASH_MODULUS
GOLD_AUDIT_HASH_EXPR_SILVER = _GOLD_AUDIT_HASH_SUM.format(cols=(
    "X_rel_center::DOUBLE, Y_rel_center::DOUBLE, x::FLOAT, y::FLOAT, "
    "CAST(ROUND(depth * 100) AS INTEGER), CAST(ROUND(velocity * 100) AS INTEGER), "
    "id_anriss::BIGINT, area::INTEGER, relTh::INTEGER, mu::DECIMAL(7,3), xsi::INTEGER, tau0::INTEGER"))
GOLD_AUDIT_HASH_EXPR_GOLD = _GOLD_AUDIT_HASH_SUM.format(cols=(
    "x_anriss::DOUBLE, y_anriss::DOUBLE, x::FLOAT, y::FLOAT, "
    "Fliesstiefe::INTEGER, Fliessgeschwindigkeit::INTEGER, "
    "id_anriss::BIGINT, A::INTEGER, h::INTEGER, mu::DECIMAL(7,3), xsi::INTEGER, tau0::INTEGER"))

# Audit ledger parquets use the silver-side names as the canonical sim key;
# the gold-side aggregates alias their renamed columns back to these.
GOLD_SIM_KEY = ["id_anriss", "area", "relTh", "mu", "xsi", "tau0"]
GOLD_SIM_KEY_FROM_GOLD = "id_anriss, A AS area, h AS relTh, mu, xsi, tau0"

# In-file sort for SIM_SPATIAL: naive spatial-first order (decision Bojan
# 2026-07-15 — evaluate plain x,y row-group pruning first; only bring back a
# Hilbert curve if needed).
GOLD_SORT_COLS = ["x", "y", "id_anriss", "A", "h", "mu", "xsi", "tau0"]

# In-file sort for SIM_ANRISS (data_lake/build_silver_to_gold_anriss.py) — follows
# the actual drill-down access pattern (anriss -> A -> h -> mu/xsi/tau0),
# not GOLD_SORT_COLS' spatial one: x,y last since they're not a filter
# dimension today (probe_explorer fetches one whole anriss and filters the
# parameter combo client-side, see app.py's anriss_all_scenarios_cache) —
# this ordering costs nothing now and sets up cleanly for a future version
# that pushes the full (id_anriss, A, h, mu, xsi, tau0) filter into SQL
# instead. Decision 2026-08-12, see
# docs/claude-memory/project_gold_row_group_size_benchmark.md.
GOLD_SORT_COLS_ANRISS = ["id_anriss", "A", "h", "mu", "xsi", "tau0", "x", "y"]

# Maximum distance a written pixel can lie from its anriss center. The nominal
# DEM cap is ±800 m (MAX_BUFFER_FROM_CENTER), but pixels reach farther in
# practice: extraction snapping, the 15 m safety buffer and AvaFrame's buffer
# zone stack on top of the clamp — measured max 931.6 m over 3.67M sims
# (2026-07-16, none > 950). A 2026-08-09 root-cause investigation found this
# was worse than that measurement suggested: a boundary-creep bug (the 15 m
# safety buffer re-applied on every re-derivation of an already-capped DEM
# extent, compounding across the up to 72 relTh/mu/xsi/tau0 combinations that
# share one anriss's cached DEM) let real production events reach up to
# 1271 m — see docs/claude-memory/project_800m_dtm_logic.md. Fixed at the
# source in workers/anriss_manager.py (_clamp_extent_to_buffer); still
# defended against here (GOLD_PIXEL_MAX_REACH_M below) for silver written
# before that fix was deployed, and as defense-in-depth. Bound set to 1500 m
# (decision Bojan 2026-07-16: "better slow and safe than fast and risky") —
# generous margin over the observed max; implies a 5×5 kachel neighborhood
# for event queries and gold-finality gating. Everything that reasons
# spatially about "which kacheln can an event touch" must use THIS bound, and
# compact hard-fails any kachel that exceeds it — if that ever fires, bump it
# here consciously.
GOLD_MAX_REACH_M = 1500

# Hard per-pixel cap (decision Bojan 2026-08-09), enforced everywhere a raw
# silver pixel is turned into a client-facing or gold-lake artifact: gold
# scatter (workers/data_lake_gold_worker.py's GoldScatterWorker.scatter_group)
# AND the anriss hull-fragment builder (derivate/build_hull_fragments.py's
# build_fragment) both filter on this before their own downstream processing.
# Both need it, independently — hull fragments are their own consumer of raw
# silver (never gold), so gold's filter alone doesn't reach them; the two must
# stay consistent since the hulls are what backs _event_relevant_kacheln()'s
# bbox and probe_explorer's client-facing map display.
# Pixels farther than this from their anriss (Euclidean distance from pixel
# center (x, y) to anriss center (X_rel_center, Y_rel_center) — decision
# Bojan 2026-08-10: deliberately a circle, not the actual square DEM domain
# shape. A correctly-capped sim can legitimately produce pixels up to
# ~800*sqrt(2)≈1131 m Euclidean in a domain corner; those ARE dropped by this
# filter even though they're not boundary-creep artifacts — traded away on
# purpose for a simpler, symmetric cap) are dropped before they reach either
# artifact. This is the physical simulation-domain limit (MAX_BUFFER_FROM_CENTER in
# workers/anriss_manager.py) mirrored here as a plain constant rather than an
# import, deliberately — anriss_manager.py pulls in avaframe/rasterio, which
# neither the gold pipeline nor the hull builder should depend on just for
# this constant (see data_interface.py's module docstring). Keep the two
# values in sync by hand if MAX_BUFFER_FROM_CENTER ever changes. Distinct
# from GOLD_MAX_REACH_M above: that bound governs which KACHELN a query must
# consider (deliberately generous, 1500 m); this one governs which PIXELS
# are legitimate at all (the true physical cap).
GOLD_PIXEL_MAX_REACH_M = 800

LAKE_SCHEMA_DUCKDB = {
    "id_kachel": "INTEGER",
    "id_anriss": "BIGINT",
    "X_rel_center": "DOUBLE",
    "Y_rel_center": "DOUBLE",
    "area": "INTEGER",
    "relTh": "INTEGER",       # Anrissmächtigkeit in cm (manifest h_cm, e.g. 25); AvaFrame gets m (relTh/100) only inside the com1DFA config
    "mu": "DECIMAL(7,3)",          # Fixed precision (e.g., 0.050)
    "xsi": "INTEGER",
    "tau0": "INTEGER",
    "x": "FLOAT",
    "y": "FLOAT",
    "depth": "FLOAT",
    "velocity": "FLOAT",
    "spatial_checksum": "BIGINT",
}
