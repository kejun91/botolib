"""Tests for botolib.resources.fetchers.sqs (SQS queue fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.sqs import fetch_queues


def _make_client(queue_urls=None, attributes=None, tags=None):
    """Build a mock SQS client with canned responses."""
    client = MagicMock()

    client.list_queues.return_value = {
        "QueueUrls": queue_urls or [],
    }

    if attributes is not None:
        # Map queue URL -> attributes dict
        client.get_queue_attributes.side_effect = lambda **kw: {
            "Attributes": attributes.get(kw["QueueUrl"], {}),
        }
    else:
        client.get_queue_attributes.return_value = {"Attributes": {}}

    if tags is not None:
        # Map queue URL -> tags dict
        client.list_queue_tags.side_effect = lambda **kw: {
            "Tags": tags.get(kw["QueueUrl"], {}),
        }
    else:
        client.list_queue_tags.return_value = {"Tags": {}}

    return client


class TestFetchQueues:

    def test_yields_queue_with_attributes_and_tags(self):
        url = "https://sqs.us-east-1.amazonaws.com/123/my-queue"
        client = _make_client(
            queue_urls=[url],
            attributes={
                url: {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:my-queue",
                    "ApproximateNumberOfMessages": "5",
                    "VisibilityTimeout": "30",
                },
            },
            tags={url: {"env": "prod"}},
        )
        results = list(fetch_queues(client))
        assert len(results) == 1
        q = results[0]
        assert q["QueueUrl"] == url
        assert q["attributes"]["ApproximateNumberOfMessages"] == "5"
        assert q["tags"] == {"env": "prod"}

    def test_multiple_queues(self):
        urls = [
            "https://sqs.us-east-1.amazonaws.com/123/q1",
            "https://sqs.us-east-1.amazonaws.com/123/q2",
        ]
        client = _make_client(queue_urls=urls)
        results = list(fetch_queues(client))
        assert len(results) == 2
        assert results[0]["QueueUrl"] == urls[0]
        assert results[1]["QueueUrl"] == urls[1]

    def test_on_queue_callback_invoked(self):
        url = "https://sqs.us-east-1.amazonaws.com/123/my-queue"
        client = _make_client(queue_urls=[url])
        captured = []
        list(fetch_queues(client, on_queue=captured.append))
        assert len(captured) == 1
        assert captured[0]["QueueUrl"] == url

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_queues(client))
        assert results == []

    def test_pagination(self):
        client = MagicMock()
        client.list_queues.side_effect = [
            {
                "QueueUrls": ["https://sqs.example.com/q1"],
                "NextToken": "token1",
            },
            {
                "QueueUrls": ["https://sqs.example.com/q2"],
            },
        ]
        client.get_queue_attributes.return_value = {"Attributes": {}}
        client.list_queue_tags.return_value = {"Tags": {}}

        results = list(fetch_queues(client))
        assert len(results) == 2

    def test_attributes_error_yields_empty_dict(self):
        url = "https://sqs.us-east-1.amazonaws.com/123/my-queue"
        client = _make_client(queue_urls=[url])
        client.get_queue_attributes.side_effect = Exception("AccessDenied")
        results = list(fetch_queues(client))
        assert results[0]["attributes"] == {}

    def test_tags_error_yields_empty_dict(self):
        url = "https://sqs.us-east-1.amazonaws.com/123/my-queue"
        client = _make_client(queue_urls=[url])
        client.list_queue_tags.side_effect = Exception("AccessDenied")
        results = list(fetch_queues(client))
        assert results[0]["tags"] == {}
