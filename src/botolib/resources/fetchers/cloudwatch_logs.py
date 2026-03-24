"""
Fetch all CloudWatch Logs log groups with their tags.

Aggregates raw boto3 responses: log groups enriched with tags.

Usage:
    from botolib.resources.fetchers.cloudwatch_logs import fetch_log_groups
    client = boto3_session.client("logs", region_name="eu-west-1")
    log_groups = list(fetch_log_groups(client))
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

def _paginate_log_groups(client, prefix: str | None) -> list[dict]:
    """Paginate describe_log_groups using nextToken."""
    all_groups: list[dict] = []
    token: str | None = None
    while True:
        params: dict[str, str] = {}
        if prefix:
            params["logGroupNamePrefix"] = prefix
        if token:
            params["nextToken"] = token
        resp = client.describe_log_groups(**params)
        all_groups.extend(resp.get("logGroups", []))
        token = resp.get("nextToken")
        if not token:
            break
    return all_groups


# ---------------------------------------------------------------------------
# Per-group fetchers
# ---------------------------------------------------------------------------

def _fetch_tags(client, log_group_arn: str) -> dict[str, str]:
    """Return tags for a single log group."""
    try:
        resp = client.list_tags_for_resource(resourceArn=log_group_arn)
        return resp.get("tags", {})
    except Exception as e:
        logger.warning("list_tags_for_resource failed for %s: %s", log_group_arn, e)
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_log_groups(
    client: Any,
    prefix: str | None = None,
    on_log_group: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield CloudWatch Logs log groups one at a time.

    Each yielded dict is the raw describe_log_groups item enriched with:
      - "tags": dict from list_tags_for_resource

    Args:
        client: A CloudWatch Logs boto3 client (``logs``).
        prefix: Optional log group name prefix filter.
        on_log_group: Optional callback invoked with each log group dict.

    Yields:
        dict for each log group found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        log_groups = _paginate_log_groups(client, prefix)
        logger.info("Found %d log groups", len(log_groups))

        futures = {
            group["logGroupName"]: executor.submit(
                _fetch_tags, client, group.get("arn", ""),
            )
            for group in log_groups
        }

        for group in log_groups:
            name = group["logGroupName"]
            group["tags"] = futures[name].result()

            if on_log_group:
                on_log_group(group)
            yield group
    finally:
        executor.shutdown(wait=False)
