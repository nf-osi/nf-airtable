#!/usr/bin/env python3
"""Sync per-study file and download statistics from Snowflake to Airtable.

One-way sync: derives the NF project list and metadata directly from the
Synapse Data Warehouse (Snowflake) via the `snow` CLI, computes file and
download stats, and upserts them into a dedicated Airtable table.
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import yaml
import requests
from pyairtable import Api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Environment variables that may override file-based config.
ENV_KEYS = [
    "AIRTABLE_PAT",
    "AIRTABLE_BASE_ID",
    "SNOWFLAKE_TABLE_NAME",
    "SNOWFLAKE_PROJECT_VIEW_ID",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DOWNLOAD_START_DATE",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PAT",
]


def load_config() -> Dict[str, Any]:
    """Load configuration from config.yml + creds.yaml, with env-var override.

    Keys are normalized to lowercase. Environment variables take precedence
    over file values.
    """
    config: Dict[str, Any] = {}

    for path in ("config.yml", "creds.yaml"):
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = yaml.safe_load(f)
                    if data and isinstance(data, dict):
                        config.update({k.lower(): v for k, v in data.items()})
            except Exception as e:  # noqa: BLE001 - log and continue
                logger.warning(f"Could not read {path}: {e}")

    for key in ENV_KEYS:
        if key in os.environ:
            config[key.lower()] = os.environ[key]

    return config


def ensure_snowflake_auth(config: Dict[str, Any]) -> bool:
    """Generate ~/.snowflake/config.toml from PAT credentials when present.

    Returns True if a config file was written (CI path), False if no PAT
    credentials were found (local path — the existing connection is used).
    """
    account = config.get("snowflake_account")
    user = config.get("snowflake_user")
    token = config.get("snowflake_pat")

    if not (account and user and token):
        logger.info("No Snowflake PAT in environment; using existing local connection")
        return False

    snow_dir = os.path.expanduser("~/.snowflake")
    os.makedirs(snow_dir, exist_ok=True)
    config_path = os.path.join(snow_dir, "config.toml")

    database = config.get("snowflake_database", "SYNAPSE_DATA_WAREHOUSE")
    schema = config.get("snowflake_schema", "SYNAPSE")
    warehouse = config.get("snowflake_warehouse", "COMPUTE_XSMALL")
    role = config.get("snowflake_role", "DATA_ANALYTICS")

    toml_text = (
        'default_connection_name = "default"\n\n'
        "[connections.default]\n"
        f'account = "{account}"\n'
        f'user = "{user}"\n'
        'authenticator = "PROGRAMMATIC_ACCESS_TOKEN"\n'
        f'token = "{token}"\n'
        f'database = "{database}"\n'
        f'schema = "{schema}"\n'
        f'warehouse = "{warehouse}"\n'
        f'role = "{role}"\n'
    )

    with open(config_path, "w") as f:
        f.write(toml_text)
    os.chmod(config_path, 0o600)
    logger.info("Wrote Snowflake connection config to %s", config_path)
    return True


def main():
    """Entry point. Implemented in later tasks."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
