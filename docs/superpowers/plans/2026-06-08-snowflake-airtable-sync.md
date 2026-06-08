# Snowflake → Airtable File-Stats Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a one-way pipeline that computes per-study file and download statistics from the Synapse Data Warehouse (Snowflake) and upserts them into a dedicated Airtable "Snowflake - File Stats" table.

**Architecture:** A single self-contained script `sync_snowflake_to_airtable.py` mirroring the existing `sync_jira_to_airtable.py`: load config → ensure Snowflake auth → query project metadata + sizes + downloads from Snowflake via the `snow` CLI → join in Python → auto-create the Airtable table if missing → upsert by `project_id`. The NF project list and metadata are derived natively from Snowflake (scope_ids of view `52677631`), so the script has no dependency on other syncs.

**Tech Stack:** Python 3.10, `snow` CLI (Snowflake CLI, JSON output via subprocess), `pyairtable`, `requests`, `pyyaml`, `pytest` (tests).

**Spec:** `docs/superpowers/specs/2026-06-08-snowflake-airtable-sync-design.md`

---

## File Structure

- `sync_snowflake_to_airtable.py` (new) — the entire sync pipeline. Single module to match the repo's existing one-file-per-sync convention. Sections: config, Snowflake auth, query builders, query execution, transform, Airtable I/O, `main()`.
- `tests/test_sync_snowflake_to_airtable.py` (new) — pytest unit/integration tests with mocked subprocess + Airtable.
- `tests/__init__.py` (new) — marks the test package.
- `.github/workflows/sync_snowflake_to_airtable.yml` (new) — scheduled + manual workflow.
- `config.yml` (modify) — add Snowflake non-sensitive config.
- `example_creds.yaml` (modify) — add Snowflake credential placeholders.
- `requirements.txt` (modify) — add `pytest`.
- `CLAUDE.md` (modify) — document the Snowflake sync + PAT rotation.
- `README.md` (modify) — document Snowflake sync usage.

Tests run from the repo root with `python -m pytest`.

---

## Task 1: Test scaffolding and dependencies

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_sync_snowflake_to_airtable.py`
- Modify: `requirements.txt`

- [ ] **Step 1: Add pytest to requirements**

Append to `requirements.txt`:

```
pytest>=7.0.0
```

- [ ] **Step 2: Create the test package marker**

Create `tests/__init__.py` (empty file):

```python
```

- [ ] **Step 3: Create the test module with a smoke import test**

Create `tests/test_sync_snowflake_to_airtable.py`:

```python
"""Tests for the Snowflake -> Airtable file-stats sync."""

import importlib


def test_module_imports():
    """The sync module should import without side effects."""
    mod = importlib.import_module("sync_snowflake_to_airtable")
    assert hasattr(mod, "main")
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sync_snowflake_to_airtable'`

- [ ] **Step 5: Create a minimal module stub so the import passes**

Create `sync_snowflake_to_airtable.py`:

```python
#!/usr/bin/env python3
"""Sync per-study file and download statistics from Snowflake to Airtable."""


def main():
    """Entry point. Implemented in later tasks."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add requirements.txt tests/__init__.py tests/test_sync_snowflake_to_airtable.py sync_snowflake_to_airtable.py
git commit -m "test: scaffold Snowflake sync module and tests (#8)"
```

---

## Task 2: Config loading

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k load_config -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'load_config'`

- [ ] **Step 3: Implement load_config and logging setup**

Replace the contents of `sync_snowflake_to_airtable.py` with:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k load_config -v`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add config loading for Snowflake sync (#8)"
```

---

## Task 2: (continued) — note on `main` raising

`main` still raises `NotImplementedError`; the smoke test from Task 1 only checks `hasattr(mod, "main")`, which still passes. Leave `main` until Task 9.

---

## Task 3: Snowflake auth config generation

The script writes `~/.snowflake/config.toml` only when PAT credentials are present in the environment (CI). Locally, the developer's existing connection is used unchanged.

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k ensure_snowflake_auth -v`
Expected: FAIL with `AttributeError: ... has no attribute 'ensure_snowflake_auth'`

- [ ] **Step 3: Implement ensure_snowflake_auth**

Insert this function above `def main():` in `sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k ensure_snowflake_auth -v`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add Snowflake PAT auth config generation (#8)"
```

---

## Task 4: Query builders

Ported from `snowflake-streamlit/toolkit/queries.py` and
`query_released_data_by_initiative.sql`, with `initiative` added to the
metadata query.

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k query_ -v`
Expected: FAIL with `AttributeError` on `query_project_meta`

- [ ] **Step 3: Implement the query builders and STAFF_USERIDS**

Insert above `def main():`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k query_ -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add Snowflake query builders for project stats (#8)"
```

---

## Task 5: Query execution with retry

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
from unittest.mock import MagicMock


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k run_snowflake_query -v`
Expected: FAIL with `AttributeError` on `run_snowflake_query`

- [ ] **Step 3: Implement run_snowflake_query**

Insert above `def main():`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k run_snowflake_query -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add Snowflake query execution with retry (#8)"
```

---

## Task 6: Batching, transform, and human-readable size

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k "bytes_to_readable or batched or transform_records" -v`
Expected: FAIL with `AttributeError` on `bytes_to_readable`

- [ ] **Step 3: Implement bytes_to_readable, batched, transform_records**

Insert above `def main():`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k "bytes_to_readable or batched or transform_records" -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add transform/join and size formatting for Snowflake sync (#8)"
```

---

## Task 7: Airtable table creation and field introspection

Ports `_request_with_retry` and `get_airtable_valid_fields` from
`sync_jira_to_airtable.py:63-134`, plus a `create_snowflake_table` with the
spec's schema.

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k "create_snowflake_table or get_airtable_valid_fields" -v`
Expected: FAIL with `AttributeError` on `create_snowflake_table`

- [ ] **Step 3: Implement Airtable helpers**

Insert above `def main():`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k "create_snowflake_table or get_airtable_valid_fields" -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add Airtable table creation + field introspection (#8)"
```

---

## Task 8: Upsert to Airtable

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k sync_to_airtable -v`
Expected: FAIL with `AttributeError` on `sync_to_airtable`

- [ ] **Step 3: Implement sync_to_airtable**

Insert above `def main():`:

```python
def sync_to_airtable(api: Api, base_id: str, table_name: str,
                    records: List[Dict[str, Any]], valid_fields: set):
    """Upsert records into Airtable keyed on project_id, with change detection.

    Returns (created, updated, skipped, errors).
    """
    table = api.table(base_id, table_name)
    existing = table.all()
    existing_by_key = {rec["fields"].get("project_id"): rec for rec in existing}
    logger.info("Found %d existing records in Airtable", len(existing))

    created = updated = skipped = errors = 0

    for i, record in enumerate(records, 1):
        try:
            pid = record["project_id"]
            data = {k: v for k, v in record.items() if not valid_fields or k in valid_fields}

            if pid in existing_by_key:
                existing_record = existing_by_key[pid]
                existing_fields = existing_record["fields"]
                needs_update = any(existing_fields.get(k) != v for k, v in data.items())
                if needs_update:
                    table.update(existing_record["id"], data, typecast=True)
                    updated += 1
                else:
                    skipped += 1
            else:
                table.create(data, typecast=True)
                created += 1

            if i % 10 == 0:
                time.sleep(0.2)
        except Exception as e:  # noqa: BLE001 - count and continue
            logger.error("Error syncing project %s: %s", record.get("project_id", "?"), e)
            errors += 1

    logger.info("Sync summary: created=%d updated=%d skipped=%d errors=%d",
                created, updated, skipped, errors)
    return created, updated, skipped, errors
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k sync_to_airtable -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: add Airtable upsert with change detection (#8)"
```

---

## Task 9: Orchestration in main()

**Files:**
- Modify: `sync_snowflake_to_airtable.py`
- Test: `tests/test_sync_snowflake_to_airtable.py`

- [ ] **Step 1: Write the failing test (end-to-end with mocks)**

Add to `tests/test_sync_snowflake_to_airtable.py`:

```python
def test_main_runs_full_flow(monkeypatch):
    import sync_snowflake_to_airtable as s

    monkeypatch.setattr(s, "load_config", lambda: {
        "airtable_pat": "pat", "airtable_base_id": "appX",
        "snowflake_table_name": "Snowflake - File Stats",
        "snowflake_project_view_id": "52677631",
        "snowflake_download_start_date": "2019-01-01",
    })
    monkeypatch.setattr(s, "ensure_snowflake_auth", lambda cfg: False)

    def fake_query(q, **k):
        if "project_scope" in q:
            return [{"PROJECT_ID": "111", "PROJECT_NAME": "A", "FUNDER": "NTAP",
                     "STUDY_LEADS": "", "STUDY_STATUS": "Active",
                     "DATA_STATUS": "Available", "INITIATIVE": "Init1"}]
        if "objectdownload_event" in q.lower():
            return [{"PROJECT_ID": "111", "DOWNLOAD_BYTES": 2048, "DOWNLOAD_UNIQUE_FILES": 3}]
        return [{"PROJECT_ID": "111", "FILE_COUNT": 10,
                 "UNIQUE_FILE_HANDLES": 9, "TOTAL_BYTES": 1024}]

    monkeypatch.setattr(s, "run_snowflake_query", fake_query)
    monkeypatch.setattr(s, "get_airtable_valid_fields",
                        lambda *a, **k: {f["name"] for f in s.SNOWFLAKE_TABLE_FIELDS})

    captured = {}
    def fake_sync(api, base, name, records, fields):
        captured["records"] = records
        return (1, 0, 0, 0)
    monkeypatch.setattr(s, "sync_to_airtable", fake_sync)
    monkeypatch.setattr(s, "Api", lambda pat: MagicMock())

    s.main()

    assert len(captured["records"]) == 1
    assert captured["records"][0]["project_id"] == "111"
    assert captured["records"][0]["download_bytes"] == 2048
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -k main_runs_full_flow -v`
Expected: FAIL with `NotImplementedError`

- [ ] **Step 3: Implement main()**

Replace the `def main():` body in `sync_snowflake_to_airtable.py`:

```python
DEFAULT_PROJECT_VIEW_ID = "52677631"
DEFAULT_DOWNLOAD_START_DATE = "2019-01-01"
BATCH_SIZE = 100


def main():
    """Run the Snowflake -> Airtable file-stats sync."""
    config = load_config()

    base_id = config.get("airtable_base_id")
    airtable_pat = config.get("airtable_pat")
    table_name = config.get("snowflake_table_name", "Snowflake - File Stats")
    project_view_id = str(config.get("snowflake_project_view_id", DEFAULT_PROJECT_VIEW_ID))
    download_start = config.get("snowflake_download_start_date", DEFAULT_DOWNLOAD_START_DATE)

    missing = [name for name, val in
               (("AIRTABLE_BASE_ID", base_id), ("AIRTABLE_PAT", airtable_pat))
               if not val]
    if missing:
        for var in missing:
            logger.error("Missing required credential: %s", var)
        sys.exit(1)

    ensure_snowflake_auth(config)

    # 1. Project list + metadata
    logger.info("Querying NF project metadata from Snowflake...")
    meta = run_snowflake_query(query_project_meta(project_view_id))
    if not meta:
        logger.warning("No projects returned from Snowflake; nothing to sync")
        sys.exit(0)
    project_ids = [str(m.get("PROJECT_ID")) for m in meta if m.get("PROJECT_ID") is not None]
    logger.info("Found %d NF projects", len(project_ids))

    # 2. Sizes + downloads, batched
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sizes: List[Dict[str, Any]] = []
    downloads: List[Dict[str, Any]] = []
    batches = batched(project_ids, BATCH_SIZE)
    for n, batch in enumerate(batches, 1):
        logger.info("Querying stats batch %d/%d (%d projects)...", n, len(batches), len(batch))
        sizes.extend(run_snowflake_query(query_project_sizes(batch)))
        downloads.extend(run_snowflake_query(
            query_project_downloads(batch, download_start, today)))

    # 3. Transform
    synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    records = transform_records(meta, sizes, downloads, synced_at=synced_at)

    # 4. Airtable connection + table
    api = Api(airtable_pat)
    valid_fields = get_airtable_valid_fields(base_id, table_name, airtable_pat)
    if not valid_fields:
        logger.info("Table '%s' not found. Creating it...", table_name)
        if not create_snowflake_table(base_id, table_name, airtable_pat):
            logger.error("Failed to create Airtable table. Cannot proceed.")
            sys.exit(1)
        valid_fields = get_airtable_valid_fields(base_id, table_name, airtable_pat)

    # 5. Upsert
    sync_to_airtable(api, base_id, table_name, records, valid_fields)
    logger.info("Sync completed successfully")
```

Place these module-level constants (`DEFAULT_PROJECT_VIEW_ID`, etc.) near the top of the file with the other constants, and keep only the `main()` function body where `main` was defined.

- [ ] **Step 4: Run the full test suite to verify it passes**

Run: `python -m pytest tests/test_sync_snowflake_to_airtable.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add sync_snowflake_to_airtable.py tests/test_sync_snowflake_to_airtable.py
git commit -m "feat: wire up Snowflake sync orchestration in main (#8)"
```

---

## Task 10: Configuration files

**Files:**
- Modify: `config.yml`
- Modify: `example_creds.yaml`

- [ ] **Step 1: Add Snowflake config to config.yml**

Append to `config.yml`:

```yaml

# Snowflake Sync Configuration
SNOWFLAKE_TABLE_NAME: "Snowflake - File Stats"
SNOWFLAKE_PROJECT_VIEW_ID: "52677631"
SNOWFLAKE_DATABASE: "SYNAPSE_DATA_WAREHOUSE"
SNOWFLAKE_SCHEMA: "SYNAPSE"
SNOWFLAKE_WAREHOUSE: "COMPUTE_XSMALL"
SNOWFLAKE_ROLE: "DATA_ANALYTICS"
SNOWFLAKE_DOWNLOAD_START_DATE: "2019-01-01"
```

- [ ] **Step 2: Add Snowflake placeholders to example_creds.yaml**

Append to `example_creds.yaml`:

```yaml
SNOWFLAKE_ACCOUNT: "your-account-identifier"
SNOWFLAKE_USER: "your-snowflake-user"
SNOWFLAKE_PAT: "your-programmatic-access-token"
```

- [ ] **Step 3: Verify the script still loads config without error**

Run: `python -c "import sync_snowflake_to_airtable as s; print(s.load_config().get('snowflake_table_name'))"`
Expected: prints `Snowflake - File Stats`

- [ ] **Step 4: Commit**

```bash
git add config.yml example_creds.yaml
git commit -m "chore: add Snowflake config and credential placeholders (#8)"
```

---

## Task 11: GitHub Actions workflow

**Files:**
- Create: `.github/workflows/sync_snowflake_to_airtable.yml`

- [ ] **Step 1: Create the workflow**

Create `.github/workflows/sync_snowflake_to_airtable.yml`:

```yaml
name: Sync Snowflake to Airtable

on:
  schedule:
    # Run daily at 5 AM UTC (after Synapse 2/3 AM and Jira 4 AM syncs)
    - cron: '0 5 * * *'
  workflow_dispatch:  # Allow manual triggering

jobs:
  sync:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout repository
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        python -m venv venv
        source venv/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt
        pip install snowflake-cli-labs

    - name: Run sync script
      env:
        # Configuration settings are in config.yml.
        # Only credentials are passed as secrets; the script writes
        # ~/.snowflake/config.toml from these on startup.
        AIRTABLE_PAT: ${{ secrets.AIRTABLE_PAT }}
        SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
        SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
        SNOWFLAKE_PAT: ${{ secrets.SNOWFLAKE_PAT }}
      run: |
        source venv/bin/activate
        python sync_snowflake_to_airtable.py
```

- [ ] **Step 2: Validate YAML syntax**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/sync_snowflake_to_airtable.yml'))"`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/sync_snowflake_to_airtable.yml
git commit -m "ci: add scheduled Snowflake to Airtable sync workflow (#8)"
```

---

## Task 12: Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`

- [ ] **Step 1: Add a Snowflake section to CLAUDE.md**

Under the "Running Sync Scripts" section in `CLAUDE.md`, after the Jira example, add:

````markdown
# Sync Snowflake → Airtable (per-study file + download stats)
export AIRTABLE_BASE_ID="your_base_id"
export SNOWFLAKE_ACCOUNT="your_account"   # only needed in CI; local uses ~/.snowflake/config.toml
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PAT="your_token"
python sync_snowflake_to_airtable.py
````

Under the "Jira Sync" architecture notes, add a "Snowflake Sync" subsection:

```markdown
**Snowflake Sync:**
- `sync_snowflake_to_airtable.py`: One-way sync from the Synapse Data Warehouse
  (Snowflake) to Airtable. Derives the NF project list and metadata natively
  from Snowflake (scope_ids of view 52677631), computes per-study file counts,
  total bytes, and download activity (staff excluded), and upserts into the
  "Snowflake - File Stats" table keyed on project_id.
- Requires the `snow` CLI. Locally uses the configured connection; in CI the
  script writes ~/.snowflake/config.toml from SNOWFLAKE_* secrets.
- Query logic is ported from the snowflake-streamlit dashboards.
```

In the "Important Notes" section, add:

```markdown
- **Snowflake PAT Renewal**: Snowflake programmatic access tokens expire and
  must be rotated. Regenerate the token and update the `SNOWFLAKE_PAT` secret in
  GitHub repo settings. A Snowflake authentication error in the sync workflow
  indicates the token has likely expired (analogous to the Jira PAT note).
```

In the "GitHub Actions" section, add `sync_snowflake_to_airtable.yml` to the
workflow list (runs daily at 5 AM UTC; requires secrets `AIRTABLE_PAT`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PAT`).

- [ ] **Step 2: Add a Snowflake section to README.md**

Add a "Snowflake Sync" section to `README.md` mirroring the existing
Synapse/Jira sections: purpose (surface per-study file + download stats in
Airtable), required env vars/secrets, the `snow` CLI prerequisite
(`pip install snowflake-cli-labs`), and the daily schedule.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: document Snowflake to Airtable sync (#8)"
```

---

## Task 13: Final verification

- [ ] **Step 1: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: all tests PASS

- [ ] **Step 2: Verify the script is importable and `main` is callable**

Run: `python -c "import sync_snowflake_to_airtable"`
Expected: no error

- [ ] **Step 3: (Manual, requires real access) Local end-to-end run**

Prerequisites: a working `snow` connection (`snow connection test`) and a test
Airtable base id + PAT in `creds.yaml`.

Run: `python sync_snowflake_to_airtable.py`
Expected: the "Snowflake - File Stats" table is created (if absent) and populated;
log ends with "Sync completed successfully" and a created/updated/skipped summary.

- [ ] **Step 4: Push the branch and open a PR**

```bash
git push -u origin feat/snowflake-airtable-sync
gh pr create --title "feat: add Snowflake data sync to Airtable via CLI tools (#8)" \
  --body "Implements #8. Adds sync_snowflake_to_airtable.py, a scheduled workflow, config, tests, and docs. See docs/superpowers/specs/2026-06-08-snowflake-airtable-sync-design.md."
```

---

## Self-Review Notes

- **Spec coverage:** config (T2/T10), PAT auth (T3/T11), query builders incl.
  initiative + staff exclusion (T4), query execution w/ retry (T5), transform +
  readable size + last_synced (T6), table auto-create w/ full 14-field schema
  (T7), upsert by project_id w/ change detection (T8), orchestration + batching +
  all-time download window (T9), workflow (T11), docs incl. PAT rotation (T12),
  tests throughout. All spec sections map to a task.
- **Type consistency:** Snowflake JSON keys are upper-case (`PROJECT_ID`,
  `TOTAL_BYTES`, …) as `snow --format JSON` returns them; Airtable field names are
  lower_snake_case. `transform_records` is the single boundary that maps upper →
  lower. `sync_to_airtable` and `create_snowflake_table` both use the lower-case
  names; `SNOWFLAKE_TABLE_FIELDS` is the single source of truth for the schema.
- **Placeholders:** none — every code step contains complete code.
- **Risk:** the exact PAT `config.toml` shape (Task 3) should be confirmed against
  the installed `snow` version during the manual E2E run (Task 13, Step 3); the
  unit test only asserts the file contents, not a live connection.
