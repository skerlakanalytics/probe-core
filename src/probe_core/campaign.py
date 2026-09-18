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

# The upfront simulation plan (every anriss, simulated or not): its key in the
# input bucket.  = basename of jobs.yaml silver_to_gold.manifest
EVENT_MANIFEST_KEY = "maxi_event_manifest.parquet"

# Default name of the bucket holding static reference inputs (event manifest,
# prozessquelle.parquet, downloadable files); PROBE_S3_BUCKET_INPUT overrides it.
# = infra.yaml s3.input_bucket
DEFAULT_INPUT_BUCKET = "input"
