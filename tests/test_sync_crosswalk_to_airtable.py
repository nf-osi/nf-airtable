"""Tests for linking Studies to their open Jira tickets from the auto-jira crosswalk.

The sync writes into the Studies table, which people curate by hand, so these
pin what it may and may not change. No network: plain dicts stand in for
Airtable records and the crosswalk.
"""

from datetime import datetime, timedelta, timezone

import pytest

from sync_crosswalk_to_airtable import CrosswalkUnusable, check_crosswalk, plan_links

AUTO = "Jira Issues (auto)"
STAGING_LINK = "Portal - Studies View (Staging)"
NOW = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


def _study(rec_id, staging=("recS1",), auto=(), manual=()):
    fields = {STAGING_LINK: list(staging), AUTO: list(auto), "Jira Issues": list(manual)}
    return {"id": rec_id, "fields": fields}


STAGING_IDS = {"recS1": "syn100", "recS2": "syn200"}
JIRA_RECORDS = {"NFOSI-1": "recJ1", "NFOSI-2": "recJ2", "NFOSI-3": "recJ3"}


def _plan(studies, projects):
    return plan_links(studies, STAGING_IDS, JIRA_RECORDS, projects,
                      synapse_link_field=STAGING_LINK, auto_field=AUTO)


def test_a_study_gets_the_open_tickets_that_reference_its_project():
    updates, _ = _plan([_study("rec1")], {"syn100": {"ticket_keys": ["NFOSI-2", "NFOSI-1"]}})
    assert updates == [{"id": "rec1", "fields": {AUTO: ["recJ1", "recJ2"]}}]


def test_links_that_are_already_correct_are_not_rewritten():
    # Unchanged rows generate no write, so a daily run touches only what moved.
    studies = [_study("rec1", auto=("recJ2", "recJ1"))]
    updates, stats = _plan(studies, {"syn100": {"ticket_keys": ["NFOSI-1", "NFOSI-2"]}})
    assert updates == []
    assert stats["unchanged"] == 1


def test_a_ticket_that_closed_is_removed_from_the_auto_column():
    # The auto column is owned by the sync, so it always equals the crosswalk.
    studies = [_study("rec1", auto=("recJ1", "recJ3"))]
    updates, _ = _plan(studies, {"syn100": {"ticket_keys": ["NFOSI-1"]}})
    assert updates == [{"id": "rec1", "fields": {AUTO: ["recJ1"]}}]


def test_a_study_no_longer_in_the_crosswalk_is_cleared():
    updates, _ = _plan([_study("rec1", auto=("recJ1",))], {})
    assert updates == [{"id": "rec1", "fields": {AUTO: []}}]


def test_the_hand_entered_column_is_never_written():
    studies = [_study("rec1", manual=("recJ3",))]
    updates, _ = _plan(studies, {"syn100": {"ticket_keys": ["NFOSI-1"]}})
    assert all(set(u["fields"]) == {AUTO} for u in updates)


def test_a_study_without_a_synapse_link_is_left_alone():
    # No Synapse ID means no way to know its tickets; clearing it would be a guess.
    studies = [_study("rec1", staging=(), auto=("recJ1",))]
    updates, stats = _plan(studies, {"syn100": {"ticket_keys": ["NFOSI-2"]}})
    assert updates == []
    assert stats["no_synapse_id"] == 1


def test_synapse_ids_match_case_insensitively():
    staging = {"recS1": "SYN100"}
    updates, _ = plan_links([_study("rec1")], staging, JIRA_RECORDS,
                            {"syn100": {"ticket_keys": ["NFOSI-1"]}},
                            synapse_link_field=STAGING_LINK, auto_field=AUTO)
    assert updates == [{"id": "rec1", "fields": {AUTO: ["recJ1"]}}]


def test_a_ticket_missing_from_the_jira_table_is_skipped_and_counted():
    # The Jira sync runs just before this; a gap means a ticket it has not synced
    # yet, which a later run will pick up rather than a reason to fail.
    updates, stats = _plan([_study("rec1")], {"syn100": {"ticket_keys": ["NFOSI-1", "NFOSI-99"]}})
    assert updates == [{"id": "rec1", "fields": {AUTO: ["recJ1"]}}]
    assert stats["missing_tickets"] == ["NFOSI-99"]


# -- check_crosswalk: refuse to write from data that cannot be trusted ----------


def _crosswalk(generated_at, schema_version=1):
    return {"schema_version": schema_version, "generated_at": generated_at.isoformat(), "projects": {}}


def test_a_fresh_crosswalk_is_usable():
    check_crosswalk(_crosswalk(NOW - timedelta(days=1)), verified_at=None, now=NOW)


def test_a_stale_crosswalk_is_refused_so_the_column_is_not_blanked():
    # A dead publisher leaves the last file in place; syncing from it would slowly
    # strip links from every study whose tickets changed since.
    with pytest.raises(CrosswalkUnusable, match="verified"):
        check_crosswalk(_crosswalk(NOW - timedelta(days=30)), verified_at=None, now=NOW)


def test_verified_at_keeps_a_stable_crosswalk_usable():
    # auto-jira skips re-uploading unchanged content; its verified_at stamp is
    # the freshness signal, not generated_at.
    check_crosswalk(_crosswalk(NOW - timedelta(days=30)),
                    verified_at=(NOW - timedelta(hours=3)).isoformat(), now=NOW)


def test_an_unknown_schema_version_is_refused():
    with pytest.raises(CrosswalkUnusable, match="schema_version"):
        check_crosswalk(_crosswalk(NOW, schema_version=2), verified_at=None, now=NOW)
