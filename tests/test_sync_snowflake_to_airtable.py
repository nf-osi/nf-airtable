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


def test_bytes_to_readable():
    import sync_snowflake_to_airtable as s
    assert s.bytes_to_readable(0) == "0 B"
    assert s.bytes_to_readable(1024) == "1.00 KB"
    assert s.bytes_to_readable(1536) == "1.50 KB"
    assert s.bytes_to_readable(1024 ** 4) == "1.00 TB"


def test_batched_splits_evenly():
    import sync_snowflake_to_airtable as s
    assert s.batched([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


def test_transform_records_joins_and_defaults_zero():
    import sync_snowflake_to_airtable as s

    meta = [
        {"PROJECT_ID": "111", "PROJECT_NAME": "Study A", "FUNDER": "NTAP",
         "STUDY_LEADS": "Dr X", "STUDY_STATUS": "Active",
         "DATA_STATUS": "Available", "INITIATIVE": "Init1"},
        {"PROJECT_ID": "222", "PROJECT_NAME": "Study B", "FUNDER": "CTF",
         "STUDY_LEADS": "", "STUDY_STATUS": "", "DATA_STATUS": "",
         "INITIATIVE": "Other"},
    ]
    sizes = [{"PROJECT_ID": "111", "FILE_COUNT": 10,
              "UNIQUE_FILE_HANDLES": 9, "TOTAL_BYTES": 1024}]
    downloads = [{"PROJECT_ID": "111", "DOWNLOAD_BYTES": 2048,
                  "DOWNLOAD_UNIQUE_FILES": 3}]

    records = s.transform_records(meta, sizes, downloads, synced_at="2026-06-08T00:00:00Z")
    by_id = {r["project_id"]: r for r in records}

    assert by_id["111"]["file_count"] == 10
    assert by_id["111"]["total_bytes"] == 1024
    assert by_id["111"]["total_size_readable"] == "1.00 KB"
    assert by_id["111"]["download_bytes"] == 2048
    assert by_id["111"]["initiative"] == "Init1"
    assert by_id["111"]["last_synced"] == "2026-06-08T00:00:00Z"
    # Project with no size/download rows still appears, zeroed.
    assert by_id["222"]["file_count"] == 0
    assert by_id["222"]["download_unique_files"] == 0
    assert by_id["222"]["total_bytes"] == 0


def test_create_snowflake_table_posts_schema(monkeypatch):
    import sync_snowflake_to_airtable as s

    captured = {}

    def fake_request(method, url, headers, **kwargs):
        captured["method"] = method
        captured["json"] = kwargs.get("json")
        return MagicMock(status_code=200, text="ok")

    monkeypatch.setattr(s, "_request_with_retry", fake_request)

    ok = s.create_snowflake_table("appX", "Snowflake - File Stats", "pat")
    assert ok is True
    assert captured["method"] == "POST"
    field_names = {f["name"] for f in captured["json"]["fields"]}
    assert {"project_id", "file_count", "total_bytes", "initiative",
            "download_unique_files", "last_synced"} <= field_names


def test_get_airtable_valid_fields_returns_field_set(monkeypatch):
    import sync_snowflake_to_airtable as s

    resp = MagicMock(status_code=200)
    resp.json.return_value = {"tables": [
        {"name": "Snowflake - File Stats",
         "fields": [{"name": "project_id"}, {"name": "file_count"}]}
    ]}
    resp.raise_for_status = lambda: None
    monkeypatch.setattr(s, "_request_with_retry", lambda *a, **k: resp)

    fields = s.get_airtable_valid_fields("appX", "Snowflake - File Stats", "pat")
    assert fields == {"project_id", "file_count"}


def test_sync_to_airtable_creates_updates_skips(monkeypatch):
    import sync_snowflake_to_airtable as s

    # Existing record for project 111 (unchanged), none for 222.
    existing = [{"id": "rec1", "fields": {"project_id": "111", "file_count": 10}}]

    table = MagicMock()
    table.all.return_value = existing
    api = MagicMock()
    api.table.return_value = table
    monkeypatch.setattr(s.time, "sleep", lambda *_: None)

    records = [
        {"project_id": "111", "file_count": 10},   # unchanged -> skip
        {"project_id": "222", "file_count": 5},     # new -> create
    ]
    valid_fields = {"project_id", "file_count"}

    created, updated, skipped, errors = s.sync_to_airtable(
        api, "appX", "Snowflake - File Stats", records, valid_fields
    )

    assert created == 1
    assert skipped == 1
    assert updated == 0
    assert errors == 0
    table.create.assert_called_once()
