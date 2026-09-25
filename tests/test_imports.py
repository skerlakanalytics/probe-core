"""Every module imports without S3 access, without Ray, and without side effects."""

import subprocess
import sys

MODULES = [
    "probe_core",
    "probe_core.campaign",
    "probe_core.resources",
    "probe_core.s3",
    "probe_core.data_lake.data_lake_schema",
    "probe_core.data_lake.data_interface",
    "probe_core.derivate.maxi_event_export",
    "probe_core.derivate.maxi_ifk_and_raster",
    "probe_core.derivate.derivate_lake",
]


def test_import_has_no_side_effects(tmp_path):
    """Run in a fresh interpreter: module caching in this test process would
    hide import-time effects of modules other tests already imported."""
    state_dir = tmp_path / "state"
    code = (
        "import importlib, sys\n"
        f"for m in {MODULES!r}: importlib.import_module(m)\n"
        "assert 'ray' not in sys.modules, 'ray imported at module level'\n"
        "assert 'dotenv' not in sys.modules, 'dotenv imported at module level'\n"
    )
    env = {"PROBE_LOCAL_STATE_DIR": str(state_dir), "PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not state_dir.exists(), "local state dir must be created on first use, not on import"
