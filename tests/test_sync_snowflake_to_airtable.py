"""Tests for the Snowflake -> Airtable file-stats sync."""

import importlib


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
