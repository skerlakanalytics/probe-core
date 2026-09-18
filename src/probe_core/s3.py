"""S3 access shared by the app and the pipeline: boto3 clients, DuckDB httpfs
configuration (including the egress proxy), and small key helpers.

Settings come from the environment, read when they are used rather than at
import time, so an entry point may load a .env file after importing this module:

    PROBE_S3_ENDPOINT_URL                       default https://f712.gos3.io
    HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY   S3 key
    HOSTTECH_BERLIN_OBJECT_STORAGE_KEY_SECRET   S3 secret
    https_proxy / http_proxy / no_proxy         egress proxy, see _duckdb_proxy_for

Until 2026-09-18 this lived in utils.py of pgr-atlas and ProBE_control_center,
next to Ray and DEM helpers that stay in the pipeline, and it called
load_dotenv() on import; loading .env is now the entry point's job.
"""

import os
from collections.abc import Mapping

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

DEFAULT_S3_ENDPOINT = "https://f712.gos3.io"


class _S3Config(Mapping):
    """The S3 settings as a read-only mapping ("endpoint_url", "access_key",
    "secret_key") whose values are looked up in the environment on every
    access. Same keys as the former module-level dict, so existing
    S3_CONFIG["..."] call sites keep working."""

    _ENV = {
        "endpoint_url": ("PROBE_S3_ENDPOINT_URL", DEFAULT_S3_ENDPOINT),
        "access_key": ("HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY", None),
        "secret_key": ("HOSTTECH_BERLIN_OBJECT_STORAGE_KEY_SECRET", None),
    }

    def __getitem__(self, key):
        var, default = self._ENV[key]
        return os.getenv(var, default)

    def __iter__(self):
        return iter(self._ENV)

    def __len__(self):
        return len(self._ENV)


S3_CONFIG = _S3Config()

# Client singletons, created on first use (after the entry point has set up
# its environment), once per process.
_S3_CLIENT = None
_S3_RESOURCE = None


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
