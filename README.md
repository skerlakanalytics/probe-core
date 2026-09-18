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

## Development

```bash
uv sync
uv run pytest
```
