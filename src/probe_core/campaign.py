"""Facts about the MAXI simulation campaign that the data lake was built with.

These are properties of the data, not settings of a deployment, so they are
constants here instead of configuration. Changing one means the data changed.
Until 2026-09-18 they came from config/app.yaml (pgr-atlas) and
config/jobs.yaml (ProBE_control_center); the pipeline's jobs.yaml keeps its own
copy for running jobs and must agree with these values.
"""

# Physics grid applied uniformly to every anriss; every anriss is simulated
# with each (mu, xsi, tau0) combination.  = jobs.yaml physics_parameter_sets.maxi
PHYSICS_GRID = {
    "mu": [0.05, 0.15, 0.3],
    "xsi": [100, 400, 1000],
    "tau0": [0, 600, 1000, 1500],
}

# Events per simulation batch; batch ids and hull-fragment S3 keys are derived
# from it: batch_id = (sort_key - 1) // EVENTS_PER_BATCH + 1.
# = jobs.yaml defaults.batch_size
EVENTS_PER_BATCH = 100

# The upfront simulation plan (every anriss, simulated or not): its key at the
# gold bucket's root (a copy of the fleet's input/ original).  = basename of
# jobs.yaml silver_to_gold.manifest
EVENT_MANIFEST_KEY = "maxi_event_manifest.parquet"

# The event catalog (2026-09-27): the manifest re-sorted by id_anriss with the
# columns the app looks up per anriss, plus the precomputed catalog totals.
# Built once by ProBE_control_center's data_lake/build_event_catalog.py and
# stored next to the manifest at the gold bucket's root. Replaces the app's
# local catalog.db (the manifest is sorted by sort_key, so a per-anriss lookup
# against it can't skip row groups; this copy can).
EVENT_CATALOG_KEY = "maxi_event_manifest_by_id_anriss.parquet"
EVENT_CATALOG_STATS_KEY = "maxi_event_manifest_stats.json"
