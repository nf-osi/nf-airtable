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
