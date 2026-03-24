"""Tests for botolib.resources.fetchers.eventbridge (EventBridge rule fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.eventbridge import fetch_rules


def _make_client(rules=None, targets=None, tags=None):
    """Build a mock EventBridge client with canned responses."""
    client = MagicMock()

    client.list_rules.return_value = {"Rules": rules or []}

    if targets is not None:
        if isinstance(targets, dict):
            # Map rule name -> targets list
            client.list_targets_by_rule.side_effect = lambda **kw: {
                "Targets": targets.get(kw["Rule"], []),
            }
        else:
            client.list_targets_by_rule.return_value = {"Targets": targets}
    else:
        client.list_targets_by_rule.return_value = {"Targets": []}

    if tags is not None:
        # tags: dict mapping rule ARN -> list of tag dicts
        client.list_tags_for_resource.side_effect = lambda **kw: {
            "Tags": tags.get(kw["ResourceARN"], []),
        }
    else:
        client.list_tags_for_resource.return_value = {"Tags": []}

    return client


class TestFetchRules:

    def test_yields_rule_with_targets(self):
        client = _make_client(
            rules=[
                {"Name": "my-rule", "Arn": "arn:rule1", "State": "ENABLED"},
            ],
            targets={
                "my-rule": [
                    {"Id": "target1", "Arn": "arn:aws:lambda:us-east-1:123:function:my-fn"},
                ],
            },
        )
        results = list(fetch_rules(client))
        assert len(results) == 1
        rule = results[0]
        assert rule["Name"] == "my-rule"
        assert len(rule["targets"]) == 1
        assert rule["targets"][0]["Id"] == "target1"

    def test_multiple_rules(self):
        client = _make_client(
            rules=[
                {"Name": "rule1", "Arn": "arn:rule1"},
                {"Name": "rule2", "Arn": "arn:rule2"},
            ],
            targets={
                "rule1": [{"Id": "t1", "Arn": "arn:t1"}],
                "rule2": [
                    {"Id": "t2", "Arn": "arn:t2"},
                    {"Id": "t3", "Arn": "arn:t3"},
                ],
            },
        )
        results = list(fetch_rules(client))
        assert len(results) == 2
        assert len(results[0]["targets"]) == 1
        assert len(results[1]["targets"]) == 2

    def test_on_rule_callback_invoked(self):
        client = _make_client(
            rules=[{"Name": "my-rule", "Arn": "arn:rule1"}],
        )
        captured = []
        list(fetch_rules(client, on_rule=captured.append))
        assert len(captured) == 1
        assert captured[0]["Name"] == "my-rule"

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_rules(client))
        assert results == []

    def test_pagination(self):
        client = MagicMock()
        client.list_rules.side_effect = [
            {
                "Rules": [{"Name": "rule1", "Arn": "arn:rule1"}],
                "NextToken": "token1",
            },
            {
                "Rules": [{"Name": "rule2", "Arn": "arn:rule2"}],
            },
        ]
        client.list_targets_by_rule.return_value = {"Targets": []}

        results = list(fetch_rules(client))
        assert len(results) == 2
        assert results[0]["Name"] == "rule1"
        assert results[1]["Name"] == "rule2"

    def test_custom_event_bus(self):
        client = _make_client(
            rules=[{"Name": "my-rule", "Arn": "arn:rule1", "EventBusName": "custom-bus"}],
            targets={"my-rule": [{"Id": "t1", "Arn": "arn:t1"}]},
        )
        results = list(fetch_rules(client, event_bus_name="custom-bus"))
        assert len(results) == 1
        # Verify EventBusName was passed to list_rules
        client.list_rules.assert_called_once()
        call_kwargs = client.list_rules.call_args[1] if client.list_rules.call_args[1] else client.list_rules.call_args.kwargs
        assert call_kwargs.get("EventBusName") == "custom-bus"

    def test_targets_error_yields_empty_list(self):
        client = _make_client(
            rules=[{"Name": "my-rule", "Arn": "arn:rule1"}],
        )
        client.list_targets_by_rule.side_effect = Exception("AccessDenied")
        results = list(fetch_rules(client))
        assert len(results) == 1
        assert results[0]["targets"] == []

    def test_rule_has_tags(self):
        client = _make_client(
            rules=[{"Name": "my-rule", "Arn": "arn:rule1"}],
            tags={"arn:rule1": [{"Key": "env", "Value": "prod"}]},
        )
        results = list(fetch_rules(client))
        assert results[0]["tags"] == [{"Key": "env", "Value": "prod"}]

    def test_tags_error_yields_empty_tags(self):
        client = _make_client(
            rules=[{"Name": "my-rule", "Arn": "arn:rule1"}],
        )
        client.list_tags_for_resource.side_effect = Exception("AccessDenied")
        results = list(fetch_rules(client))
        assert results[0]["tags"] == []
