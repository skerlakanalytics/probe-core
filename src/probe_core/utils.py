# orchestration
import os
import shutil

# logging, timing
import time
from pathlib import Path
from typing import Iterable

# s3
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# List of internal AvaFrame loggers that are being chatty (avaframe.com1DFA only for PROD runs, it raises particles left)
NOISY_LOGGERS = [
    "avaframe",
    "avaframe.com1DFA.com1DFATools",
    "avaframe.com1DFA.checkCfg",
    "avaframe.in3Utils.cfgUtils",
    "avaframe.in3Utils.geoTrans",
    "avaframe.com1DFA.deriveParameterSet",
    "avaframe.in1Data.getInput",
    "avaframe.in3Utils.initializeProject",
    "pyogrio._io",
    "rasterio",       # The main rasterio logger
    "rasterio._env",  # This is the one specifically making the 'env.py' noise
    "fiona",          # Often noisy alongside GDAL
    "libpysal",        # Sometimes pops up in spatial tasks
    "boto3",
    "botocore",
    "s3transfer",
    "urllib3"
]

from probe_config import load_config

S3_CONFIG = {
    "endpoint_url": os.getenv("PROBE_S3_ENDPOINT_URL", load_config()["s3"]["endpoint"]),
    "access_key": os.getenv("HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY"),
    "secret_key": os.getenv("HOSTTECH_BERLIN_OBJECT_STORAGE_KEY_SECRET")
}

def _duckdb_proxy_for(host: str):
    """(address, username, password) of the HTTP proxy DuckDB should use to
    reach `host` -- address as DuckDB's 'host:port' -- derived from the
    standard https_proxy / http_proxy / no_proxy environment variables, or
    None when no proxy applies.

    DuckDB's httpfs does NOT read those environment variables. Verified
    2026-09-17 against duckdb 1.4.3: with https_proxy set, requests still went
    out directly; only an explicit `SET http_proxy` routed them through the
    proxy. boto3 does read them. So in an environment with a mandatory egress
    proxy -- Bedag's clusters route through proxy.kb-bedag.ch:8080 -- boto3
    calls would work while every DuckDB S3 read fails with "Could not
    establish connection". no_proxy is evaluated with the stdlib's own rules,
    so DuckDB and boto3 make the same bypass decision for the same host."""
    from urllib.parse import unquote, urlsplit
    from urllib.request import proxy_bypass_environment

    proxy = (os.getenv("https_proxy") or os.getenv("HTTPS_PROXY")
             or os.getenv("http_proxy") or os.getenv("HTTP_PROXY"))
    if not proxy or proxy_bypass_environment(host):
        return None
    parts = urlsplit(proxy if "://" in proxy else f"http://{proxy}")
    if not parts.hostname:
        return None
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return (f"{parts.hostname}:{port}",
            unquote(parts.username) if parts.username else None,
            unquote(parts.password) if parts.password else None)


def configure_s3_for_duckdb(con):
    """Point a DuckDB connection's httpfs S3 client at our bucket (same
    credentials as S3_CONFIG/get_s3_resource, DuckDB's own SET syntax), and
    route it through the environment's HTTP proxy if there is one -- see
    _duckdb_proxy_for for why DuckDB needs this spelled out explicitly."""
    con.execute(f"SET s3_access_key_id='{S3_CONFIG['access_key']}'")
    con.execute(f"SET s3_secret_access_key='{S3_CONFIG['secret_key']}'")
    endpoint = S3_CONFIG["endpoint_url"].removeprefix("https://").removeprefix("http://")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    proxy = _duckdb_proxy_for(endpoint.split("/")[0])
    if proxy:
        address, username, password = proxy
        con.execute(f"SET http_proxy='{address}'")
        if username:
            con.execute(f"SET http_proxy_username='{username}'")
        if password:
            con.execute(f"SET http_proxy_password='{password}'")

# Enable initialization of S3 client singleton
_S3_CLIENT = None
_S3_RESOURCE = None

def extract_extent_from_ascii_dem(dem_path, logger):
    header = {}
    with open(dem_path, 'r') as f:
        for _ in range(6):
            line = f.readline().split()
            if not line:
                break
            header[line[0].lower()] = float(line[1])
    try:
        x_min = header.get('xllcorner')
        y_min = header.get('yllcorner')
        ncols = int(header.get('ncols'))
        nrows = int(header.get('nrows'))
        cellsize = header.get('cellsize')
        x_max = x_min + (ncols * cellsize)
        y_max = y_min + (nrows * cellsize)
        extent = [x_min, x_max, y_min, y_max]
        return extent
    except Exception as e:
        logger.error(f"Error when opening DEM {dem_path}: {e}")

def show_cluster_status(logger):
    """Prints a human-readable summary of the Ray cluster."""
    import ray
    if not ray.is_initialized():
        print("❌ Ray is not initialized.")
        return

    resources = ray.cluster_resources()
    nodes = ray.nodes()
    
    # 1. Hardware Stats
    total_cpus = resources.get("CPU", 0)
    # Memory is in bytes, convert to GB
    total_mem_gb = resources.get("memory", 0) / (1024**3)
    obj_store_gb = resources.get("object_store_memory", 0) / (1024**3)

    # 2. Node Stats
    alive_nodes = [n for n in nodes if n["Alive"]]
    head_node_ip = next((n["NodeManagerAddress"] for n in nodes if n.get("Resources", {}).get("node:__internal_head__")), "Unknown")

    logger.info("="*40)
    logger.info("🌐 RAY CLUSTER STATUS REPORT")
    logger.info("="*40)
    logger.info(f"📍 Head Node IP:    {head_node_ip}")
    logger.info(f"👥 Active Nodes:    {len(alive_nodes)}")
    for node in alive_nodes:
        ip = node["NodeManagerAddress"]
        node_cpus = node["Resources"].get("CPU", 0)
        role = "[HEAD]" if ip == head_node_ip else "[WORKER]"
        logger.info(f"  {role} {ip} ({node_cpus:.0f} cores)")
    logger.info("-"*40)
    logger.info(f"⚡ Total CPUs:      {total_cpus:.0f} cores")
    logger.info(f"🧠 System Memory:   {total_mem_gb:.2f} GB")
    logger.info(f"📦 Object Store:    {obj_store_gb:.2f} GB")
    logger.info("-"*40)

def get_s3_client():
    """Process-safe Singleton-ish pattern for high-concurrency."""
    global _S3_CLIENT
    if _S3_CLIENT is None:
        # This only runs ONCE per worker process
        _S3_CLIENT = boto3.client(
            's3',
            endpoint_url=S3_CONFIG["endpoint_url"],
            aws_access_key_id=S3_CONFIG["access_key"],
            aws_secret_access_key=S3_CONFIG["secret_key"],
            config=Config(
                retries={'max_attempts': 10, 'mode': 'adaptive'},
                max_pool_connections=50 # Better for 32-core machines
            )
        )
    return _S3_CLIENT

def get_s3_resource():
    #TODO: check if possible to unify with s3 client?
    """Process-safe Singleton-ish pattern for high-concurrency S3 Resource."""
    global _S3_RESOURCE
    if _S3_RESOURCE is None:
        _S3_RESOURCE = boto3.resource(
            's3',
            endpoint_url=S3_CONFIG["endpoint_url"],
            aws_access_key_id=S3_CONFIG["access_key"],
            aws_secret_access_key=S3_CONFIG["secret_key"],
            config=Config(
                retries={'max_attempts': 10, 'mode': 'adaptive'},
                max_pool_connections=50
            )
        )
    return _S3_RESOURCE

def s3_exists(bucket_name, folder_prefix, filename):
    s3 = get_s3_client()
    key = f"{folder_prefix.strip('/')}/{filename}"
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise ValueError(f"S3 Check Error: {e}")

def s3_key_exists(bucket_name, key):
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise ValueError(f"S3 Check Error: {e}")

def s3_presigned_url(bucket_name, key, expires=86400):
    """Signed GET URL for direct browser download, bypassing the app process
    entirely -- a pure local HMAC computation (no network round-trip to
    *create* it), so it's cheap to call on every Streamlit rerun without any
    caching. Standard S3 API (SigV4), not AWS/Hosttech-specific -- works
    against any S3-compatible endpoint, same as every other S3 call in this
    file. Not pinned to a specific object version: if the underlying key gets
    overwritten before the URL is used (or expires), it resolves to whatever
    is current at request time, not what was there when the URL was signed
    (probe_explorer's Derivate view, 2026-08-25 -- fine there since that
    pipeline's output is immutable per config_hash except a deliberate
    manual --force rebuild, in which case serving the corrected version is
    the desired behavior anyway)."""
    return get_s3_client().generate_presigned_url(
        'get_object', Params={'Bucket': bucket_name, 'Key': key}, ExpiresIn=expires)

RAY_TMP_MAX_AGE_HOURS = 48  # same policy/window as anriss_manager.py's DEM_CACHE_MAX_AGE_HOURS

def cleanup_ray_tmp_sessions(ray_tmp_dir="/tmp/ray", max_age_hours=RAY_TMP_MAX_AGE_HOURS, logger=None):
    """Delete old `session_*` directories under `ray_tmp_dir` left behind by
    previous ray.init()/ray.shutdown() lifetimes. Ray never cleans these up
    itself -- a worker restarted repeatedly over weeks accumulates one
    session dir per restart, each holding logs/spilled objects, with no
    bound (observed: 20GB on a single MAXI worker, a real contributor to a
    2026-09-04 disk-full incident -- same failure class as the 2026-08-29
    aux-fleet incident where 14 nodes hit 88-100% full disks from stale Ray
    sessions).

    Call once, right after ray.init() -- by then 'session_latest' already
    points at the just-created current session, so resolving it first and
    skipping that path guarantees the live session is never touched, no
    matter how old its own directory's mtime looks at the moment of the
    call. Every other `session_*` dir untouched for more than max_age_hours
    is a session from a process that's long gone -- safe to remove.

    Mirrors cleanup_dem_cache's policy (age-based via mtime, OSError
    swallowed for a dir a concurrent worker already deleted). Returns the
    number of session dirs deleted."""
    ray_tmp = Path(ray_tmp_dir)
    if not ray_tmp.is_dir():
        return 0
    current_session = (ray_tmp / "session_latest").resolve()
    cutoff = time.time() - max_age_hours * 3600
    n_deleted = 0
    for entry in ray_tmp.glob("session_20*"):  # e.g. session_2026-09-04_10-56-14_186267_2009737
        if entry.resolve() == current_session:
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry)
                n_deleted += 1
        except OSError:
            pass  # deleted by a concurrent worker's cleanup, or a race -- fine
    if logger and n_deleted:
        logger.info(f"🧹 Ray tmp cleanup: deleted {n_deleted} old session dir(s) "
                    f"older than {max_age_hours}h from {ray_tmp}")
    return n_deleted
