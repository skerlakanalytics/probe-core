# probe-core

Shared code of the Pro<sup>BE</sup> Äxplorer / MAXI projects: S3 access, the data-lake
schema and read layer, and the derivate computations (IFK, rasters, event export).

Used by:

- [`pgr-atlas`](https://github.com/skerlakanalytics/pgr-atlas) — the Streamlit app
- `ProBE_control_center` — the simulation and data-lake pipeline

Both depend on a tagged version of this package, e.g.

```toml
dependencies = ["probe-core @ git+https://github.com/skerlakanalytics/probe-core@v0.1.0"]
```

## Origin

Extracted on 2026-09-18 from `pgr-atlas` (`app/probe_control_center/`, commit
`f9a1f74`), whose copy carries the fixes for running in containers behind a proxy
(DuckDB proxy routing, cgroup-based memory/CPU sizing, spill cap) on top of
`ProBE_control_center`.

## Modules

| Module | Contents |
|---|---|
| `probe_core.s3` | S3 settings from the environment, boto3 clients, `configure_s3_for_duckdb` (incl. egress proxy), key helpers |
| `probe_core.resources` | DuckDB memory/thread/spill limits from the container's cgroup, else the host |
| `probe_core.campaign` | facts about the MAXI data: physics grid, events per batch, manifest key |
| `probe_core.data_lake.data_lake_schema` | lake layout, column names, constants (reads `data/cfgCom1DFA_template.ini`) |
| `probe_core.data_lake.data_interface` | read layer: gold, catalog, hull fragments, stats |
| `probe_core.derivate.*` | IFK and rasters, event export, raster derivate (+ canton mosaic) |

## Configuration

Nothing is read from config files, and importing has no side effects. The
environment is read when a value is used:

| Variable | Default | Used for |
|---|---|---|
| `PROBE_S3_ENDPOINT_URL` | `https://f712.gos3.io` | S3 endpoint |
| `HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY` / `_KEY_SECRET` | — | S3 credentials |
| `PROBE_S3_BUCKET_GOLD` | `maxi` | gold / derivate bucket |
| `PROBE_S3_BUCKET_INPUT` | `input` | static reference inputs |
| `PROBE_LOCAL_STATE_DIR` | `~/probe_explorer/local_state` | local catalog / manifest indexes |
| `PROBE_DUCKDB_MAX_TEMP_SIZE` | DuckDB's own | cap for DuckDB spill files |
| `https_proxy`, `http_proxy`, `no_proxy` | — | egress proxy, also applied to DuckDB |

Loading a `.env` file is the job of the entry point (`load_dotenv()` before the
first S3 call); the former `utils.py` did it on import.

## Migrating from the old copies

| Before (`probe_control_center/…`) | Now |
|---|---|
| `from utils import get_s3_client, configure_s3_for_duckdb, S3_CONFIG, …` | `from probe_core.s3 import …` |
| `utils.NOISY_LOGGERS`, `show_cluster_status`, `cleanup_ray_tmp_sessions`, `extract_extent_from_ascii_dem` | not here: pipeline-only, stay in the pipeline's `utils.py` |
| `from data_lake.data_interface import …` | `from probe_core.data_lake.data_interface import …` (sizing helpers still re-exported) |
| `from data_lake.data_lake_schema import …` | `from probe_core.data_lake.data_lake_schema import …` |
| `from derivate.<module> import …` | `from probe_core.derivate.<module> import …` |
| `probe_config.load_config()` / `app.yaml` values | `probe_core.campaign` constants and the variables above |
| `python derivate/build_raster_derivate.py …` | CLI stays in ProBE_control_center (reads its `jobs.yaml`), calling `run_pass` from here; needs `probe-core[ray]` |
| `python derivate/build_raster_derivate_mosaic.py …` | `python -m probe_core.derivate.build_raster_derivate_mosaic …` |

A consumer can switch gradually by turning its old modules into thin forwarders,
e.g. the pipeline's `utils.py` keeping its Ray helpers and re-exporting
`from probe_core.s3 import *`.

## Development

```bash
uv sync
uv run pytest
```
