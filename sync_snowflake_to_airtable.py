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


def main():
    """Entry point. Implemented in later tasks."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
