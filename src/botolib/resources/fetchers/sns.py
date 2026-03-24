"""
Fetch all SNS topics with their attributes, subscriptions, and tags.

Aggregates raw boto3 responses: topics with nested attributes,
subscriptions, and tags.

Usage:
    from botolib.resources.fetchers.sns import fetch_topics
    client = boto3_session.client("sns", region_name="eu-west-1")
    topics = list(fetch_topics(client))
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
    """Paginate an SNS call using NextToken."""
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
# Per-topic fetchers
# ---------------------------------------------------------------------------

def _get_topic_attributes(client, topic_arn: str) -> dict[str, str]:
    """Return attributes for a single topic."""
    try:
        resp = client.get_topic_attributes(TopicArn=topic_arn)
        return resp.get("Attributes", {})
    except Exception as e:
        logger.warning("get_topic_attributes failed for %s: %s", topic_arn, e)
        return {}


def _fetch_subscriptions(client, topic_arn: str) -> list[dict]:
    """Return all subscriptions for a single topic."""
    try:
        return _paginate(
            client, "list_subscriptions_by_topic",
            {"TopicArn": topic_arn}, "Subscriptions",
        )
    except Exception as e:
        logger.warning("list_subscriptions_by_topic failed for %s: %s", topic_arn, e)
        return []


def _fetch_tags(client, topic_arn: str) -> list[dict]:
    """Return tags for a single topic."""
    try:
        resp = client.list_tags_for_resource(ResourceArn=topic_arn)
        return resp.get("Tags", [])
    except Exception as e:
        logger.warning("list_tags_for_resource failed for %s: %s", topic_arn, e)
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_topics(
    client: Any,
    on_topic: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield SNS topics one at a time.

    Each yielded dict is the raw list_topics item enriched with:
      - "attributes": raw get_topic_attributes response
      - "subscriptions": raw list_subscriptions_by_topic items
      - "tags": raw list_tags_for_resource items

    Args:
        client: An SNS boto3 client.
        on_topic: Optional callback invoked with each topic dict as it is yielded.

    Yields:
        dict for each SNS topic found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        topics = _paginate(client, "list_topics", {}, "Topics")
        logger.info("Found %d SNS topics", len(topics))

        for topic in topics:
            topic_arn = topic.get("TopicArn", "")
            logger.info("Fetching details for topic: %s", topic_arn)

            attrs_fut = executor.submit(_get_topic_attributes, client, topic_arn)
            subs_fut = executor.submit(_fetch_subscriptions, client, topic_arn)
            tags_fut = executor.submit(_fetch_tags, client, topic_arn)

            topic["attributes"] = attrs_fut.result()
            topic["subscriptions"] = subs_fut.result()
            topic["tags"] = tags_fut.result()

            if on_topic:
                on_topic(topic)
            yield topic
    finally:
        executor.shutdown(wait=False)
