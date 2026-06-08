"""Tests for the Snowflake -> Airtable file-stats sync."""

import importlib
from unittest.mock import MagicMock


def test_module_imports():
    """The sync module should import without side effects."""
    mod = importlib.import_module("sync_snowflake_to_airtable")
    assert hasattr(mod, "main")


def test_load_config_env_overrides_file(tmp_path, monkeypatch):
    import sync_snowflake_to_airtable as s

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yml").write_text(
        'SNOWFLAKE_TABLE_NAME: "From File"\nAIRTABLE_BASE_ID: "appFILE"\n'
    )
    (tmp_path / "creds.yaml").write_text('AIRTABLE_PAT: "patfile"\n')
    monkeypatch.setenv("AIRTABLE_PAT", "patenv")

    config = s.load_config()

    assert config["snowflake_table_name"] == "From File"
    assert config["airtable_base_id"] == "appFILE"
    assert config["airtable_pat"] == "patenv"  # env overrides creds.yaml


def test_load_config_missing_files_returns_dict(tmp_path, monkeypatch):
    import sync_snowflake_to_airtable as s

    monkeypatch.chdir(tmp_path)
    config = s.load_config()
    assert isinstance(config, dict)


def test_ensure_snowflake_auth_writes_config_when_pat_present(tmp_path, monkeypatch):
    import sync_snowflake_to_airtable as s

    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(s.os.path, "expanduser",
                        lambda p: p.replace("~", str(fake_home)))

    config = {
        "snowflake_account": "acct123",
        "snowflake_user": "svc_user",
        "snowflake_pat": "tok_secret",
        "snowflake_database": "SYNAPSE_DATA_WAREHOUSE",
        "snowflake_schema": "SYNAPSE",
        "snowflake_warehouse": "COMPUTE_XSMALL",
        "snowflake_role": "DATA_ANALYTICS",
    }

    wrote = s.ensure_snowflake_auth(config)

    assert wrote is True
    cfg_text = (fake_home / ".snowflake" / "config.toml").read_text()
    assert 'account = "acct123"' in cfg_text
    assert 'authenticator = "PROGRAMMATIC_ACCESS_TOKEN"' in cfg_text
    assert 'token = "tok_secret"' in cfg_text
    assert 'database = "SYNAPSE_DATA_WAREHOUSE"' in cfg_text


def test_ensure_snowflake_auth_noop_without_pat(tmp_path, monkeypatch):
    import sync_snowflake_to_airtable as s

    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(s.os.path, "expanduser",
                        lambda p: p.replace("~", str(fake_home)))

    wrote = s.ensure_snowflake_auth({"snowflake_account": "acct123"})

    assert wrote is False
    assert not (fake_home / ".snowflake" / "config.toml").exists()


def test_query_project_meta_uses_scope_view_and_initiative():
    import sync_snowflake_to_airtable as s
    q = s.query_project_meta("52677631")
    assert "id = 52677631" in q
    assert "annotations.studyName.value[0]" in q
    assert "annotations.initiative.value[0]" in q
    assert "scope_ids" in q


def test_query_project_sizes_interpolates_ids():
    import sync_snowflake_to_airtable as s
    q = s.query_project_sizes(["111", "222"])
    assert "'111', '222'" in q
    assert "file_count" in q.lower()
    assert "content_size" in q.lower()


def test_query_project_downloads_excludes_staff_and_dates():
    import sync_snowflake_to_airtable as s
    q = s.query_project_downloads(["111"], "2019-01-01", "2026-06-08")
    assert "objectdownload_event" in q.lower()
    assert "2019-01-01" in q and "2026-06-08" in q
    # staff exclusion present
    assert "user_id NOT IN" in q
    assert str(s.STAFF_USERIDS[0]) in q


def test_run_snowflake_query_parses_json(monkeypatch):
    import sync_snowflake_to_airtable as s

    fake = MagicMock(returncode=0, stdout='[{"PROJECT_ID": "111"}]', stderr="")
    monkeypatch.setattr(s.subprocess, "run", lambda *a, **k: fake)

    rows = s.run_snowflake_query("SELECT 1")
    assert rows == [{"PROJECT_ID": "111"}]


def test_run_snowflake_query_returns_empty_on_blank(monkeypatch):
    import sync_snowflake_to_airtable as s

    fake = MagicMock(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(s.subprocess, "run", lambda *a, **k: fake)

    assert s.run_snowflake_query("SELECT 1") == []


def test_run_snowflake_query_retries_then_raises(monkeypatch):
    import sync_snowflake_to_airtable as s

    calls = {"n": 0}

    def boom(*a, **k):
        calls["n"] += 1
        raise s.subprocess.CalledProcessError(1, "snow", stderr="fail")

    monkeypatch.setattr(s.subprocess, "run", boom)
    monkeypatch.setattr(s.time, "sleep", lambda *_: None)

    try:
        s.run_snowflake_query("SELECT 1", max_retries=3)
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass
    assert calls["n"] == 3
