"""
Fetch all DynamoDB tables and their descriptions for a given AWS account.

Aggregates raw boto3 responses: table names with nested describe_table output.

Usage:
    from botolib.resources.fetchers.dynamodb import fetch_tables
    client = boto3_session.client("dynamodb", region_name="eu-west-1")
    tables = list(fetch_tables(client))
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

def _paginate_table_names(client) -> list[str]:
    """Paginate list_tables using ExclusiveStartTableName."""
    all_names: list[str] = []
    start_table: str | None = None
    while True:
        params: dict[str, str] = {}
        if start_table:
            params["ExclusiveStartTableName"] = start_table
        resp = client.list_tables(**params)
        all_names.extend(resp.get("TableNames", []))
        start_table = resp.get("LastEvaluatedTableName")
        if not start_table:
            break
    return all_names


# ---------------------------------------------------------------------------
# Per-table fetcher
# ---------------------------------------------------------------------------

def _fetch_tags(client, table_arn: str) -> list[dict]:
    """Return tags for a single DynamoDB table. Paginated via NextToken."""
    all_tags: list[dict] = []
    token: str | None = None
    while True:
        params: dict[str, str] = {"ResourceArn": table_arn}
        if token:
            params["NextToken"] = token
        try:
            resp = client.list_tags_of_resource(**params)
        except Exception as e:
            logger.warning("list_tags_of_resource failed for %s: %s", table_arn, e)
            return all_tags
        all_tags.extend(resp.get("Tags", []))
        token = resp.get("NextToken")
        if not token:
            break
    return all_tags


def _describe_table(client, table_name: str) -> dict | None:
    """Call describe_table for a single table. Returns the Table dict or None."""
    try:
        resp = client.describe_table(TableName=table_name)
        return resp.get("Table")
    except Exception as e:
        logger.warning("describe_table failed for %s: %s", table_name, e)
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_tables(
    client: Any,
    on_table: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield DynamoDB tables one at a time.

    Each yielded dict is the raw describe_table "Table" response.

    Args:
        client: A DynamoDB boto3 client.
        on_table: Optional callback invoked with each table dict as it is yielded.

    Yields:
        dict for each DynamoDB table found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        table_names = _paginate_table_names(client)
        logger.info("Found %d DynamoDB tables", len(table_names))

        futures = [
            (name, executor.submit(_describe_table, client, name))
            for name in table_names
        ]

        for name, fut in futures:
            table = fut.result()
            if table is None:
                continue

            table_arn = table.get("TableArn", "")
            if table_arn:
                table["tags"] = _fetch_tags(client, table_arn)
            else:
                table["tags"] = []

            if on_table:
                on_table(table)
            yield table
    finally:
        executor.shutdown(wait=False)
