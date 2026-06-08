"""Tests for the Snowflake -> Airtable file-stats sync."""

import importlib


def test_module_imports():
    """The sync module should import without side effects."""
    mod = importlib.import_module("sync_snowflake_to_airtable")
    assert hasattr(mod, "main")
