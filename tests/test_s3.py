import pytest

from probe_core import s3

PROXY_VARS = ["https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", "no_proxy", "NO_PROXY"]


@pytest.fixture(autouse=True)
def no_proxy_env(monkeypatch):
    for var in PROXY_VARS:
        monkeypatch.delenv(var, raising=False)


def test_no_proxy_configured():
    assert s3._duckdb_proxy_for("f712.gos3.io") is None


def test_proxy_from_https_proxy(monkeypatch):
    monkeypatch.setenv("https_proxy", "http://proxy.kb-bedag.ch:8080")
    assert s3._duckdb_proxy_for("f712.gos3.io") == ("proxy.kb-bedag.ch:8080", None, None)


def test_proxy_with_credentials_and_default_port(monkeypatch):
    monkeypatch.setenv("http_proxy", "http://us%40er:p%3Ass@proxy.example")
    assert s3._duckdb_proxy_for("f712.gos3.io") == ("proxy.example:80", "us@er", "p:ss")


def test_no_proxy_bypass(monkeypatch):
    """Same bypass decision as boto3 (both use the stdlib rules), e.g. for
    Bedag's own S3: a domain suffix with or without leading dot matches."""
    monkeypatch.setenv("https_proxy", "http://proxy.kb-bedag.ch:8080")
    monkeypatch.setenv("no_proxy", "localhost, .be.ch, kb-bedag.ch")
    assert s3._duckdb_proxy_for("x7ba-s3-kakbfe.infra.be.ch") is None
    assert s3._duckdb_proxy_for("proxy.kb-bedag.ch") is None
    assert s3._duckdb_proxy_for("f712.gos3.io") is not None


def test_no_proxy_wildcard_is_not_honoured(monkeypatch):
    """Pitfall: the stdlib (and so boto3 and this module) ignores "*.domain"
    entries; write ".domain" instead. Found 2026-09-18 in pgr-atlas's Bedag
    stage values, which list *.be.ch."""
    monkeypatch.setenv("https_proxy", "http://proxy.kb-bedag.ch:8080")
    monkeypatch.setenv("no_proxy", "*.be.ch")
    assert s3._duckdb_proxy_for("x7ba-s3-kakbfe.infra.be.ch") is not None


def test_s3_config_reads_environment_on_access(monkeypatch):
    monkeypatch.delenv("PROBE_S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY", raising=False)
    assert s3.S3_CONFIG["endpoint_url"] == s3.DEFAULT_S3_ENDPOINT
    assert s3.S3_CONFIG["access_key"] is None
    monkeypatch.setenv("HOSTTECH_BERLIN_OBJECT_STORAGE_ACCESS_KEY", "set-after-import")
    assert s3.S3_CONFIG["access_key"] == "set-after-import"
    assert sorted(dict(s3.S3_CONFIG)) == ["access_key", "endpoint_url", "secret_key"]


class FakeCon:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)


def test_configure_s3_for_duckdb_sets_proxy(monkeypatch):
    monkeypatch.setenv("PROBE_S3_ENDPOINT_URL", "https://f712.gos3.io")
    monkeypatch.setenv("https_proxy", "http://proxy.kb-bedag.ch:8080")
    con = FakeCon()
    s3.configure_s3_for_duckdb(con)
    assert "SET s3_endpoint='f712.gos3.io'" in con.statements
    assert "SET http_proxy='proxy.kb-bedag.ch:8080'" in con.statements


def test_configure_s3_for_duckdb_without_proxy(monkeypatch):
    con = FakeCon()
    s3.configure_s3_for_duckdb(con)
    assert not any("http_proxy" in sql for sql in con.statements)
