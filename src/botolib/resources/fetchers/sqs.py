"""
Fetch all SQS queues and their attributes/tags for a given AWS account.

Aggregates raw boto3 responses: queues with nested attributes and tags.

Usage:
    from botolib.resources.fetchers.sqs import fetch_queues
    client = boto3_session.client("sqs", region_name="eu-west-1")
    queues = list(fetch_queues(client))
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

def _paginate_queue_urls(client) -> list[str]:
    """Paginate list_queues using NextToken."""
    all_urls: list[str] = []
    token: str | None = None
    while True:
        params: dict[str, str] = {}
        if token:
            params["NextToken"] = token
        resp = client.list_queues(**params)
        all_urls.extend(resp.get("QueueUrls", []))
        token = resp.get("NextToken")
        if not token:
            break
    return all_urls


# ---------------------------------------------------------------------------
# Per-queue fetchers
# ---------------------------------------------------------------------------

def _get_queue_attributes(client, queue_url: str) -> dict[str, str]:
    """Return all attributes for a single queue."""
    try:
        resp = client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["All"],
        )
        return resp.get("Attributes", {})
    except Exception as e:
        logger.warning("get_queue_attributes failed for %s: %s", queue_url, e)
        return {}


def _get_queue_tags(client, queue_url: str) -> dict[str, str]:
    """Return tags for a single queue."""
    try:
        resp = client.list_queue_tags(QueueUrl=queue_url)
        return resp.get("Tags", {})
    except Exception as e:
        logger.warning("list_queue_tags failed for %s: %s", queue_url, e)
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_queues(
    client: Any,
    on_queue: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield SQS queues one at a time.

    Each yielded dict contains:
      - "QueueUrl": the queue URL
      - "attributes": raw get_queue_attributes response (all attributes)
      - "tags": raw list_queue_tags response (dict of key/value pairs)

    Args:
        client: An SQS boto3 client.
        on_queue: Optional callback invoked with each queue dict as it is yielded.

    Yields:
        dict for each SQS queue found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        queue_urls = _paginate_queue_urls(client)
        logger.info("Found %d SQS queues", len(queue_urls))

        for url in queue_urls:
            logger.info("Fetching details for queue: %s", url)

            attrs_fut = executor.submit(_get_queue_attributes, client, url)
            tags_fut = executor.submit(_get_queue_tags, client, url)

            queue: dict[str, Any] = {"QueueUrl": url}
            queue["attributes"] = attrs_fut.result()
            queue["tags"] = tags_fut.result()

            if on_queue:
                on_queue(queue)
            yield queue
    finally:
        executor.shutdown(wait=False)
