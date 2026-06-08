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


# Staff/DCC/DPE user ids excluded from external download metrics.
# Ported from snowflake-streamlit/toolkit/queries.py.
STAFF_USERIDS = [
    3421893, 3389310, 3342573, 3434950, 3459953, 3514384, 3510065,
    3324230, 3460442, 3458117, 3434599, 3440247, 3342492, 3481671,
    3489628, 3441756,
]


def query_project_meta(project_view_id: str) -> str:
    """NF project list + metadata from node_latest scope_ids of the portal view."""
    return f"""
    WITH project_scope AS (
        SELECT CAST(scopes.value AS INTEGER) AS scope_id
        FROM synapse_data_warehouse.synapse.node_latest,
             LATERAL FLATTEN(input => node_latest.scope_ids) scopes
        WHERE id = {project_view_id}
    )
    SELECT
        nl.id AS project_id,
        JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.studyName.value[0]') AS project_name,
        JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.fundingAgency.value') AS funder,
        ARRAY_TO_STRING(PARSE_JSON(JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.studyLeads.value')), ', ') AS study_leads,
        JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.studyStatus.value[0]') AS study_status,
        JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.dataStatus.value[0]') AS data_status,
        COALESCE(
            JSON_EXTRACT_PATH_TEXT(nl.ANNOTATIONS, 'annotations.initiative.value[0]'),
            'Other'
        ) AS initiative
    FROM synapse_data_warehouse.synapse.node_latest nl
    JOIN project_scope ps ON nl.id = ps.scope_id
    ORDER BY project_name;
    """


def query_project_sizes(project_ids: List[str]) -> str:
    """Per-project file counts and total content size."""
    project_list = ", ".join(f"'{pid}'" for pid in project_ids)
    return f"""
    WITH project_files AS (
        SELECT nl.id, nl.file_handle_id, nl.project_id
        FROM synapse_data_warehouse.synapse.node_latest nl
        WHERE nl.project_id IN ({project_list})
          AND nl.node_type = 'file'
    ),
    file_sizes AS (
        SELECT pf.project_id, pf.id AS node_id, pf.file_handle_id, fl.content_size
        FROM project_files pf
        LEFT JOIN synapse_data_warehouse.synapse.file_latest fl
          ON fl.id = pf.file_handle_id
    )
    SELECT
        project_id,
        COUNT(DISTINCT node_id) AS file_count,
        COUNT(DISTINCT file_handle_id) AS unique_file_handles,
        SUM(COALESCE(content_size, 0)) AS total_bytes
    FROM file_sizes
    GROUP BY project_id;
    """


def query_project_downloads(project_ids: List[str], start_date: str, end_date: str) -> str:
    """Per-project download bytes and unique downloaded files, staff excluded."""
    project_list = ", ".join(f"'{pid}'" for pid in project_ids)
    excluded = ", ".join(str(uid) for uid in STAFF_USERIDS)
    return f"""
    WITH download_data AS (
        SELECT
            ode.project_id,
            ode.file_handle_id,
            fl.content_size
        FROM synapse_data_warehouse.synapse_event.objectdownload_event ode
        JOIN synapse_data_warehouse.synapse.file_latest fl
          ON fl.id = ode.file_handle_id
        WHERE ode.project_id IN ({project_list})
          AND ode.record_date BETWEEN '{start_date}' AND '{end_date}'
          AND ode.user_id NOT IN ({excluded})
    )
    SELECT
        project_id,
        SUM(COALESCE(content_size, 0)) AS download_bytes,
        COUNT(DISTINCT file_handle_id) AS download_unique_files
    FROM download_data
    GROUP BY project_id;
    """


def run_snowflake_query(query: str, max_retries: int = 3,
                        timeout: int = 300) -> List[Dict[str, Any]]:
    """Execute a query via `snow sql --format JSON` and return parsed rows.

    Retries transient subprocess/timeout failures with exponential backoff.
    Raises RuntimeError if all attempts fail.
    """
    cmd = ["snow", "sql", "--format", "JSON", "-q", query]
    last_error: Optional[str] = None

    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, check=True, timeout=timeout
            )
            if not result.stdout.strip():
                return []
            data = json.loads(result.stdout)
            return data if isinstance(data, list) else []
        except subprocess.CalledProcessError as e:
            last_error = e.stderr or str(e)
            logger.warning("snow query failed (attempt %d/%d): %s",
                           attempt + 1, max_retries, last_error)
        except subprocess.TimeoutExpired:
            last_error = f"timed out after {timeout}s"
            logger.warning("snow query timed out (attempt %d/%d)",
                           attempt + 1, max_retries)
        except json.JSONDecodeError as e:
            last_error = f"could not parse JSON output: {e}"
            logger.warning("snow query JSON parse failed (attempt %d/%d): %s",
                           attempt + 1, max_retries, e)

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    raise RuntimeError(f"Snowflake query failed after {max_retries} attempts: {last_error}")


def main():
    """Entry point. Implemented in later tasks."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
