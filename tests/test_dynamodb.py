"""Tests for botolib.resources.fetchers.dynamodb (DynamoDB table fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.dynamodb import fetch_tables


def _make_client(table_names=None, tables=None, tags=None):
    """Build a mock DynamoDB client with canned responses."""
    client = MagicMock()

    client.list_tables.return_value = {
        "TableNames": table_names or [],
    }

    if tables is not None:
        # Map table name -> Table dict
        client.describe_table.side_effect = lambda **kw: {
            "Table": tables[kw["TableName"]],
        }
    else:
        client.describe_table.return_value = {
            "Table": {"TableName": "default", "TableStatus": "ACTIVE"},
        }

    if tags is not None:
        # tags: dict mapping table ARN -> list of tag dicts
        client.list_tags_of_resource.side_effect = lambda **kw: {
            "Tags": tags.get(kw["ResourceArn"], []),
        }
    else:
        client.list_tags_of_resource.return_value = {"Tags": []}

    return client


class TestFetchTables:

    def test_yields_table_description(self):
        client = _make_client(
            table_names=["my-table"],
            tables={
                "my-table": {
                    "TableName": "my-table",
                    "TableStatus": "ACTIVE",
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                },
            },
        )
        results = list(fetch_tables(client))
        assert len(results) == 1
        assert results[0]["TableName"] == "my-table"
        assert results[0]["TableStatus"] == "ACTIVE"
        assert results[0]["KeySchema"][0]["AttributeName"] == "id"

    def test_multiple_tables(self):
        client = _make_client(
            table_names=["t1", "t2"],
            tables={
                "t1": {"TableName": "t1", "TableStatus": "ACTIVE"},
                "t2": {"TableName": "t2", "TableStatus": "ACTIVE"},
            },
        )
        results = list(fetch_tables(client))
        assert len(results) == 2
        assert results[0]["TableName"] == "t1"
        assert results[1]["TableName"] == "t2"

    def test_on_table_callback_invoked(self):
        client = _make_client(
            table_names=["my-table"],
            tables={"my-table": {"TableName": "my-table"}},
        )
        captured = []
        list(fetch_tables(client, on_table=captured.append))
        assert len(captured) == 1
        assert captured[0]["TableName"] == "my-table"

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_tables(client))
        assert results == []

    def test_pagination(self):
        client = MagicMock()
        client.list_tables.side_effect = [
            {
                "TableNames": ["t1"],
                "LastEvaluatedTableName": "t1",
            },
            {
                "TableNames": ["t2"],
            },
        ]
        client.describe_table.side_effect = lambda **kw: {
            "Table": {"TableName": kw["TableName"], "TableStatus": "ACTIVE"},
        }

        results = list(fetch_tables(client))
        assert len(results) == 2
        assert results[0]["TableName"] == "t1"
        assert results[1]["TableName"] == "t2"

    def test_describe_table_error_skips_table(self):
        client = _make_client(table_names=["good", "bad"])
        client.describe_table.side_effect = [
            {"Table": {"TableName": "good", "TableStatus": "ACTIVE", "TableArn": "arn:good"}},
            Exception("AccessDenied"),
        ]
        results = list(fetch_tables(client))
        assert len(results) == 1
        assert results[0]["TableName"] == "good"

    def test_table_has_tags(self):
        client = _make_client(
            table_names=["my-table"],
            tables={
                "my-table": {
                    "TableName": "my-table",
                    "TableArn": "arn:table1",
                    "TableStatus": "ACTIVE",
                },
            },
            tags={"arn:table1": [{"Key": "env", "Value": "prod"}]},
        )
        results = list(fetch_tables(client))
        assert results[0]["tags"] == [{"Key": "env", "Value": "prod"}]

    def test_tags_error_yields_empty_tags(self):
        client = _make_client(
            table_names=["my-table"],
            tables={
                "my-table": {
                    "TableName": "my-table",
                    "TableArn": "arn:table1",
                    "TableStatus": "ACTIVE",
                },
            },
        )
        client.list_tags_of_resource.side_effect = Exception("AccessDenied")
        results = list(fetch_tables(client))
        assert results[0]["tags"] == []
