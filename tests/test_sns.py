"""Tests for the SNS topic fetcher."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from botolib.resources.fetchers.sns import fetch_topics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(
    topics: list | None = None,
    topic_pages: list | None = None,
    attributes: dict | None = None,
    subscriptions: list | None = None,
    subscription_pages: list | None = None,
    tags: list | None = None,
) -> MagicMock:
    """Build a fake SNS client."""
    client = MagicMock()

    # list_topics (pages)
    if topic_pages is not None:
        client.list_topics.side_effect = topic_pages
    else:
        client.list_topics.return_value = {
            "Topics": topics or [],
        }

    # get_topic_attributes
    if attributes is not None:
        client.get_topic_attributes.return_value = {"Attributes": attributes}
    else:
        client.get_topic_attributes.return_value = {"Attributes": {}}

    # list_subscriptions_by_topic (pages)
    if subscription_pages is not None:
        client.list_subscriptions_by_topic.side_effect = subscription_pages
    else:
        client.list_subscriptions_by_topic.return_value = {
            "Subscriptions": subscriptions or [],
        }

    # list_tags_for_resource
    client.list_tags_for_resource.return_value = {"Tags": tags or []}

    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_topic_enriched_with_attributes_and_subscriptions():
    client = _make_client(
        topics=[{"TopicArn": "arn:aws:sns:us-east-1:123:my-topic"}],
        attributes={"TopicArn": "arn:aws:sns:us-east-1:123:my-topic", "DisplayName": "My Topic"},
        subscriptions=[
            {"SubscriptionArn": "arn:aws:sns:us-east-1:123:my-topic:sub1", "Protocol": "email"},
        ],
        tags=[{"Key": "env", "Value": "prod"}],
    )

    results = list(fetch_topics(client))
    assert len(results) == 1
    topic = results[0]
    assert topic["TopicArn"] == "arn:aws:sns:us-east-1:123:my-topic"
    assert topic["attributes"]["DisplayName"] == "My Topic"
    assert len(topic["subscriptions"]) == 1
    assert topic["subscriptions"][0]["Protocol"] == "email"
    assert topic["tags"] == [{"Key": "env", "Value": "prod"}]


def test_multiple_topics():
    client = _make_client(
        topics=[
            {"TopicArn": "arn:aws:sns:us-east-1:123:t1"},
            {"TopicArn": "arn:aws:sns:us-east-1:123:t2"},
        ],
    )
    results = list(fetch_topics(client))
    assert len(results) == 2


def test_callback_invoked():
    client = _make_client(
        topics=[{"TopicArn": "arn:aws:sns:us-east-1:123:t1"}],
    )
    seen = []
    list(fetch_topics(client, on_topic=seen.append))
    assert len(seen) == 1
    assert seen[0]["TopicArn"] == "arn:aws:sns:us-east-1:123:t1"


def test_empty_topics():
    client = _make_client(topics=[])
    results = list(fetch_topics(client))
    assert results == []


def test_topic_pagination():
    client = _make_client(
        topic_pages=[
            {"Topics": [{"TopicArn": "arn:aws:sns:us-east-1:123:t1"}], "NextToken": "tok1"},
            {"Topics": [{"TopicArn": "arn:aws:sns:us-east-1:123:t2"}]},
        ],
    )
    results = list(fetch_topics(client))
    assert len(results) == 2
    assert client.list_topics.call_count == 2


def test_subscription_pagination():
    client = _make_client(
        topics=[{"TopicArn": "arn:aws:sns:us-east-1:123:t1"}],
        subscription_pages=[
            {
                "Subscriptions": [{"SubscriptionArn": "sub1", "Protocol": "sqs"}],
                "NextToken": "tok1",
            },
            {
                "Subscriptions": [{"SubscriptionArn": "sub2", "Protocol": "lambda"}],
            },
        ],
    )
    results = list(fetch_topics(client))
    assert len(results[0]["subscriptions"]) == 2


def test_get_attributes_error_returns_empty():
    client = _make_client(
        topics=[{"TopicArn": "arn:aws:sns:us-east-1:123:t1"}],
    )
    client.get_topic_attributes.side_effect = Exception("boom")
    results = list(fetch_topics(client))
    assert results[0]["attributes"] == {}


def test_list_tags_error_returns_empty():
    client = _make_client(
        topics=[{"TopicArn": "arn:aws:sns:us-east-1:123:t1"}],
    )
    client.list_tags_for_resource.side_effect = Exception("boom")
    results = list(fetch_topics(client))
    assert results[0]["tags"] == []
