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


def bytes_to_readable(n: int) -> str:
    """Convert a byte count to a human-readable string (B, KB, MB, GB, TB, PB)."""
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PB"


def batched(items: List[Any], size: int) -> List[List[Any]]:
    """Split a list into chunks of at most `size`."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def transform_records(meta: List[Dict[str, Any]],
                      sizes: List[Dict[str, Any]],
                      downloads: List[Dict[str, Any]],
                      synced_at: str) -> List[Dict[str, Any]]:
    """Join metadata, sizes, and downloads on project_id into Airtable records.

    Every project in `meta` produces a record; missing size/download rows
    default to zero. Returns new dicts (no mutation of inputs).
    """
    sizes_by_id = {str(r.get("PROJECT_ID")): r for r in sizes}
    downloads_by_id = {str(r.get("PROJECT_ID")): r for r in downloads}

    records: List[Dict[str, Any]] = []
    for m in meta:
        pid = str(m.get("PROJECT_ID"))
        size_row = sizes_by_id.get(pid, {})
        dl_row = downloads_by_id.get(pid, {})
        total_bytes = int(size_row.get("TOTAL_BYTES") or 0)

        records.append({
            "project_id": pid,
            "project_name": m.get("PROJECT_NAME") or "",
            "funder": m.get("FUNDER") or "",
            "initiative": m.get("INITIATIVE") or "",
            "study_status": m.get("STUDY_STATUS") or "",
            "data_status": m.get("DATA_STATUS") or "",
            "study_leads": m.get("STUDY_LEADS") or "",
            "file_count": int(size_row.get("FILE_COUNT") or 0),
            "unique_file_handles": int(size_row.get("UNIQUE_FILE_HANDLES") or 0),
            "total_bytes": total_bytes,
            "total_size_readable": bytes_to_readable(total_bytes),
            "download_bytes": int(dl_row.get("DOWNLOAD_BYTES") or 0),
            "download_unique_files": int(dl_row.get("DOWNLOAD_UNIQUE_FILES") or 0),
            "last_synced": synced_at,
        })
    return records


def _request_with_retry(method: str, url: str, headers: Dict,
                        max_retries: int = 3, **kwargs) -> requests.Response:
    """HTTP request with retry on transient (5xx / 406 / 429) responses."""
    response = None
    for attempt in range(max_retries):
        response = requests.request(method, url, headers=headers, **kwargs)
        if response.status_code < 500 and response.status_code not in (406, 429):
            return response
        wait = 2 ** attempt
        logger.warning("Airtable API returned %s, retrying in %ss (attempt %d/%d)",
                       response.status_code, wait, attempt + 1, max_retries)
        time.sleep(wait)
    return response


SNOWFLAKE_TABLE_FIELDS = [
    {"name": "project_id", "type": "singleLineText", "description": "Synapse project id (upsert key)"},
    {"name": "project_name", "type": "singleLineText", "description": "Study name"},
    {"name": "funder", "type": "singleLineText", "description": "Funding agency"},
    {"name": "initiative", "type": "singleLineText", "description": "Initiative"},
    {"name": "study_status", "type": "singleLineText", "description": "Study status"},
    {"name": "data_status", "type": "singleLineText", "description": "Data status"},
    {"name": "study_leads", "type": "singleLineText", "description": "Study leads"},
    {"name": "file_count", "type": "number", "options": {"precision": 0}, "description": "Distinct file nodes"},
    {"name": "unique_file_handles", "type": "number", "options": {"precision": 0}, "description": "Distinct file handles"},
    {"name": "total_bytes", "type": "number", "options": {"precision": 0}, "description": "Total content bytes"},
    {"name": "total_size_readable", "type": "singleLineText", "description": "Human-readable total size"},
    {"name": "download_bytes", "type": "number", "options": {"precision": 0}, "description": "Downloaded bytes (staff excluded)"},
    {"name": "download_unique_files", "type": "number", "options": {"precision": 0}, "description": "Distinct downloaded files"},
    {"name": "last_synced", "type": "dateTime", "description": "Last sync time (UTC)",
     "options": {"timeZone": "utc", "dateFormat": {"name": "iso"}, "timeFormat": {"name": "24hour"}}},
]


def create_snowflake_table(base_id: str, table_name: str, airtable_pat: str) -> bool:
    """Create the Snowflake stats table via the Airtable Metadata API."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {airtable_pat}", "Content-Type": "application/json"}
    schema = {
        "name": table_name,
        "description": "Per-study file and download stats synced from Snowflake",
        "fields": SNOWFLAKE_TABLE_FIELDS,
    }
    response = _request_with_retry("POST", url, headers=headers, json=schema, timeout=30)
    if response.status_code in (200, 201):
        logger.info("Created Airtable table '%s'", table_name)
        return True
    logger.error("Failed to create table: %s - %s", response.status_code, response.text)
    return False


def get_airtable_valid_fields(base_id: str, table_name: str, airtable_pat: str) -> set:
    """Return the set of field names on the table, or empty set if absent."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {airtable_pat}"}
    response = _request_with_retry("GET", url, headers=headers, timeout=30)
    response.raise_for_status()
    for t in response.json().get("tables", []):
        if t.get("name") == table_name:
            return {field["name"] for field in t.get("fields", [])}
    logger.warning("Table '%s' not found in Airtable metadata", table_name)
    return set()


def main():
    """Entry point. Implemented in later tasks."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
