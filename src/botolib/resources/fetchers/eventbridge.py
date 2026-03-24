"""
Fetch all EventBridge rules and their targets for a given AWS account.

Aggregates raw boto3 responses: rules with nested targets.

Usage:
    from botolib.resources.fetchers.eventbridge import fetch_rules
    client = boto3_session.client("events", region_name="eu-west-1")
    rules = list(fetch_rules(client))
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

CONCURRENCY = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _paginate(client, action: str, params: dict, items_key: str) -> list:
    """Paginate an EventBridge call using NextToken."""
    all_items: list = []
    token: str | None = None
    while True:
        req = {**params}
        if token:
            req["NextToken"] = token
        resp = getattr(client, action)(**req)
        all_items.extend(resp.get(items_key, []))
        token = resp.get("NextToken")
        if not token:
            break
    return all_items


# ---------------------------------------------------------------------------
# Per-rule fetcher
# ---------------------------------------------------------------------------

def _fetch_tags(client, rule_arn: str) -> list[dict]:
    """Return tags for a single EventBridge rule."""
    try:
        resp = client.list_tags_for_resource(ResourceARN=rule_arn)
        return resp.get("Tags", [])
    except Exception as e:
        logger.warning("Failed to fetch tags for %s: %s", rule_arn, e)
        return []


def _fetch_targets(client, rule_name: str, event_bus_name: str | None) -> list[dict]:
    """Return all targets for a single rule."""
    params: dict[str, str] = {"Rule": rule_name}
    if event_bus_name:
        params["EventBusName"] = event_bus_name
    try:
        return _paginate(client, "list_targets_by_rule", params, "Targets")
    except Exception as e:
        logger.warning("Failed to fetch targets for rule %s: %s", rule_name, e)
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_rules(
    client: Any,
    event_bus_name: str | None = None,
    on_rule: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield EventBridge rules one at a time.

    Each yielded dict is the raw list_rules item enriched with:
      - "targets": raw list_targets_by_rule items

    Args:
        client: An EventBridge (events) boto3 client.
        event_bus_name: Optional event bus name. Defaults to the default bus.
        on_rule: Optional callback invoked with each rule dict as it is yielded.

    Yields:
        dict for each rule found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        params: dict[str, str] = {}
        if event_bus_name:
            params["EventBusName"] = event_bus_name

        rules = _paginate(client, "list_rules", params, "Rules")
        logger.info("Found %d EventBridge rules", len(rules))

        futures = [
            (
                rule,
                executor.submit(
                    _fetch_targets, client,
                    rule["Name"], rule.get("EventBusName") or event_bus_name,
                ),
                executor.submit(_fetch_tags, client, rule["Arn"]),
            )
            for rule in rules
        ]

        for rule, targets_fut, tags_fut in futures:
            rule["targets"] = targets_fut.result()
            rule["tags"] = tags_fut.result()

            if on_rule:
                on_rule(rule)
            yield rule
    finally:
        executor.shutdown(wait=False)
