#!/usr/bin/env python3
"""
Link each Airtable study to the open Jira tickets that reference it.

nf-osi/auto-jira publishes a daily crosswalk to Synapse (CROSSWALK_SYNAPSE_ID in
config.yml) mapping every open NFOSI ticket to the Synapse projects it
references, including tickets that only name a folder inside the project. This
script writes that mapping into the Studies table's STUDIES_AUTO_JIRA_FIELD, a
link column into the Jira Issues table.

The column is owned by this sync: every run makes it equal the crosswalk, so a
ticket that closes or stops referencing the study drops off. The hand-maintained
"Jira Issues" column is never read for writing and never touched.

Run after sync_jira_to_airtable.py, so every linked ticket already has a record.

Usage:
    python sync_crosswalk_to_airtable.py [--dry-run]
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from pyairtable import Api

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# Refuse a crosswalk not verified against Jira within this window. It is
# refreshed daily; syncing from a dead publisher's last file would slowly strip
# links from every study whose tickets moved since.
MAX_AGE_DAYS = 7


class CrosswalkUnusable(RuntimeError):
    """The crosswalk cannot be trusted to overwrite the auto column."""


def load_config() -> Dict[str, str]:
    """config.yml, then creds.yaml, then environment variables (lowercased keys)."""
    config: Dict[str, str] = {}
    for path in ('config.yml', 'creds.yaml'):
        if os.path.exists(path):
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            config.update({k.lower(): v for k, v in data.items()})
    for key in ('AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'SYNAPSE_PAT'):
        if key in os.environ:
            config[key.lower()] = os.environ[key]
    if not config.get('airtable_pat'):
        raise ValueError("AIRTABLE_PAT not found in credentials file or environment")
    return config


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value) if value else None
    except ValueError:
        return None


def check_crosswalk(payload: Dict, verified_at: Optional[str], now: datetime) -> None:
    """Raise CrosswalkUnusable unless the crosswalk is a known schema and fresh.

    Freshness is the later of generated_at and the entity's verified_at
    annotation: auto-jira skips re-uploading unchanged content, so generated_at
    alone goes old on a quiet but healthy backlog.
    """
    if payload.get('schema_version') != SCHEMA_VERSION:
        raise CrosswalkUnusable(
            f"crosswalk schema_version is {payload.get('schema_version')!r}, expected {SCHEMA_VERSION}")
    stamps = [t for t in (_parse_time(payload.get('generated_at')), _parse_time(verified_at)) if t]
    if not stamps:
        raise CrosswalkUnusable("crosswalk carries no parseable timestamp")
    newest = max(stamps)
    if now - newest > timedelta(days=MAX_AGE_DAYS):
        raise CrosswalkUnusable(
            f"crosswalk was last verified against Jira on {newest.date()}, more than {MAX_AGE_DAYS} days ago")


def plan_links(
    studies: List[Dict],
    staging_ids: Dict[str, str],
    jira_records: Dict[str, str],
    crosswalk_projects: Dict[str, Dict],
    *,
    synapse_link_field: str,
    auto_field: str,
) -> Tuple[List[Dict], Dict]:
    """Return (updates, stats) that make each study's auto column match the crosswalk. Pure.

    ``staging_ids`` maps a staging-table record ID to its Synapse ID, and
    ``jira_records`` maps a ticket key to its Jira Issues record ID. Only rows
    whose links actually change produce an update, and an update only ever sets
    ``auto_field``. A study with no Synapse link is skipped rather than cleared,
    since there is no way to know its tickets.
    """
    updates: List[Dict] = []
    missing: set = set()
    stats = {'no_synapse_id': 0, 'unchanged': 0, 'linked': 0}
    for study in studies:
        fields = study['fields']
        staging = fields.get(synapse_link_field) or []
        synapse_id = (staging_ids.get(staging[0]) or '').strip().lower() if staging else ''
        if not synapse_id:
            stats['no_synapse_id'] += 1
            continue
        keys = sorted((crosswalk_projects.get(synapse_id) or {}).get('ticket_keys') or [])
        missing.update(k for k in keys if k not in jira_records)
        wanted = [jira_records[k] for k in keys if k in jira_records]
        if wanted:
            stats['linked'] += 1
        if set(wanted) == set(fields.get(auto_field) or []):
            stats['unchanged'] += 1
            continue
        updates.append({'id': study['id'], 'fields': {auto_field: wanted}})
    stats['missing_tickets'] = sorted(missing)
    return updates, stats


def fetch_crosswalk(config: Dict) -> Tuple[Dict, Optional[str]]:
    """Download the crosswalk and its verified_at annotation from Synapse."""
    import synapseclient

    syn = synapseclient.Synapse(silent=True, skip_checks=True)
    # CI passes SYNAPSE_PAT; locally, fall back to the cached synapseclient login.
    if config.get('synapse_pat'):
        syn.login(authToken=config['synapse_pat'], silent=True)
    else:
        syn.login(silent=True)
    entity_id = config['crosswalk_synapse_id']
    with tempfile.TemporaryDirectory() as tmp:
        entity = syn.get(entity_id, downloadLocation=tmp, ifcollision='overwrite.local')
        payload = json.loads(Path(entity.path).read_text())
    annotations = syn.restGET(f'/entity/{entity_id}/annotations2')['annotations']
    verified = ((annotations.get('verified_at') or {}).get('value') or [None])[0]
    return payload, verified


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Link Airtable studies to their open Jira tickets.")
    parser.add_argument('--dry-run', action='store_true', help="print the planned changes; write nothing")
    args = parser.parse_args(argv)

    config = load_config()
    base_id = config['airtable_base_id']
    auto_field = config['studies_auto_jira_field']
    link_field = config['studies_synapse_link_field']

    try:
        payload, verified_at = fetch_crosswalk(config)
        check_crosswalk(payload, verified_at, now=datetime.now(timezone.utc))
    except Exception as e:
        # No writes at all: a missing or stale crosswalk must never blank the column.
        logger.error(f"Crosswalk unusable, nothing written: {e}")
        return 1

    api = Api(config['airtable_pat'])
    staging = {r['id']: r['fields'].get(config['synapse_key_field'])
               for r in api.table(base_id, config['synapse_table_name']).all(fields=[config['synapse_key_field']])}
    jira = {r['fields'].get('key'): r['id']
            for r in api.table(base_id, config['jira_table_name']).all(fields=['key'])}
    studies_table = api.table(base_id, config['studies_table_name'])
    studies = studies_table.all(fields=[link_field, auto_field])

    updates, stats = plan_links(studies, staging, jira, payload.get('projects') or {},
                                synapse_link_field=link_field, auto_field=auto_field)
    logger.info(
        f"{len(studies)} studies: {stats['linked']} with open tickets, {len(updates)} to update, "
        f"{stats['unchanged']} unchanged, {stats['no_synapse_id']} without a Synapse ID")
    if stats['missing_tickets']:
        logger.warning(f"{len(stats['missing_tickets'])} crosswalk tickets not yet in the Jira table, "
                       f"skipped until the next run: {stats['missing_tickets'][:10]}")

    if args.dry_run:
        for u in updates[:10]:
            logger.info(f"[dry run] {u['id']}: {len(u['fields'][auto_field])} link(s)")
        logger.info("[dry run] nothing written")
        return 0

    if updates:
        studies_table.batch_update(updates)
    logger.info(f"Updated {len(updates)} studies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
