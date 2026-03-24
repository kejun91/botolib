"""Tests for the CloudWatch Logs log group fetcher."""

from __future__ import annotations

from unittest.mock import MagicMock

from botolib.resources.fetchers.cloudwatch_logs import fetch_log_groups


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(
    log_groups: list | None = None,
    log_group_pages: list | None = None,
    tags: dict | None = None,
) -> MagicMock:
    """Build a fake CloudWatch Logs client."""
    client = MagicMock()

    # describe_log_groups (pages)
    if log_group_pages is not None:
        client.describe_log_groups.side_effect = log_group_pages
    else:
        client.describe_log_groups.return_value = {
            "logGroups": log_groups or [],
        }

    # list_tags_for_resource
    client.list_tags_for_resource.return_value = {"tags": tags or {}}

    return client


def _group(name: str = "/aws/lambda/my-func", arn: str = "arn:aws:logs:us-east-1:123:log-group:/aws/lambda/my-func:*") -> dict:
    return {
        "logGroupName": name,
        "arn": arn,
        "storedBytes": 1024,
        "retentionInDays": 30,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_log_group_enriched_with_tags():
    client = _make_client(
        log_groups=[_group()],
        tags={"env": "prod", "team": "platform"},
    )
    results = list(fetch_log_groups(client))
    assert len(results) == 1
    assert results[0]["logGroupName"] == "/aws/lambda/my-func"
    assert results[0]["tags"] == {"env": "prod", "team": "platform"}
    client.list_tags_for_resource.assert_called_once_with(
        resourceArn="arn:aws:logs:us-east-1:123:log-group:/aws/lambda/my-func:*",
    )


def test_multiple_log_groups():
    client = _make_client(
        log_groups=[
            _group("/aws/lambda/a", "arn:aws:logs:us-east-1:123:log-group:/aws/lambda/a:*"),
            _group("/aws/lambda/b", "arn:aws:logs:us-east-1:123:log-group:/aws/lambda/b:*"),
        ],
    )
    results = list(fetch_log_groups(client))
    assert len(results) == 2
    assert client.list_tags_for_resource.call_count == 2


def test_callback_invoked():
    client = _make_client(log_groups=[_group()])
    seen = []
    list(fetch_log_groups(client, on_log_group=seen.append))
    assert len(seen) == 1
    assert seen[0]["logGroupName"] == "/aws/lambda/my-func"


def test_empty_log_groups():
    client = _make_client(log_groups=[])
    results = list(fetch_log_groups(client))
    assert results == []


def test_pagination():
    client = _make_client(
        log_group_pages=[
            {
                "logGroups": [_group("/aws/lambda/a", "arn:a")],
                "nextToken": "tok1",
            },
            {
                "logGroups": [_group("/aws/lambda/b", "arn:b")],
            },
        ],
    )
    results = list(fetch_log_groups(client))
    assert len(results) == 2
    assert client.describe_log_groups.call_count == 2


def test_prefix_filter():
    client = _make_client(log_groups=[_group()])
    list(fetch_log_groups(client, prefix="/aws/lambda/"))
    client.describe_log_groups.assert_called_once_with(
        logGroupNamePrefix="/aws/lambda/",
    )


def test_tags_error_returns_empty():
    client = _make_client(log_groups=[_group()])
    client.list_tags_for_resource.side_effect = Exception("boom")
    results = list(fetch_log_groups(client))
    assert results[0]["tags"] == {}
