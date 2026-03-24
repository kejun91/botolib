"""Tests for botolib.resources.fetchers.elbv2 (ELBv2 load balancer fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.elbv2 import fetch_load_balancers


def _make_client(
    load_balancers=None,
    target_groups=None,
    listeners=None,
    rules=None,
    target_health=None,
    tags=None,
):
    """Build a mock ELBv2 client with canned responses."""
    client = MagicMock()

    client.describe_load_balancers.return_value = {
        "LoadBalancers": load_balancers or [],
    }
    client.describe_target_groups.return_value = {
        "TargetGroups": target_groups or [],
    }

    if listeners is not None:
        client.describe_listeners.return_value = {
            "Listeners": listeners,
        }
    else:
        client.describe_listeners.return_value = {"Listeners": []}

    if rules is not None:
        client.describe_rules.return_value = {"Rules": rules}
    else:
        client.describe_rules.return_value = {"Rules": []}

    if target_health is not None:
        if isinstance(target_health, dict):
            # Map TG ARN -> health list
            client.describe_target_health.side_effect = lambda **kw: {
                "TargetHealthDescriptions": target_health.get(
                    kw["TargetGroupArn"], []
                ),
            }
        else:
            client.describe_target_health.return_value = {
                "TargetHealthDescriptions": target_health,
            }
    else:
        client.describe_target_health.return_value = {
            "TargetHealthDescriptions": [],
        }

    if tags is not None:
        # tags: dict mapping ARN -> list of tag dicts
        client.describe_tags.side_effect = lambda **kw: {
            "TagDescriptions": [
                {"ResourceArn": arn, "Tags": tags.get(arn, [])}
                for arn in kw["ResourceArns"]
            ],
        }
    else:
        client.describe_tags.return_value = {"TagDescriptions": []}

    return client


# ---------------------------------------------------------------------------
# fetch_load_balancers
# ---------------------------------------------------------------------------

class TestFetchLoadBalancers:

    def test_yields_lb_with_listeners(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 443, "Protocol": "HTTPS"},
            ],
        )
        results = list(fetch_load_balancers(client))
        assert len(results) == 1
        lb = results[0]
        assert lb["LoadBalancerArn"] == "arn:lb1"
        assert len(lb["listeners"]) == 1
        assert lb["listeners"][0]["ListenerArn"] == "arn:listener1"

    def test_listeners_have_rules(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Priority": "1",
                    "Actions": [
                        {"Type": "forward", "TargetGroupArn": "arn:tg1"},
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
            ],
        )
        results = list(fetch_load_balancers(client))
        listener = results[0]["listeners"][0]
        assert len(listener["rules"]) == 1
        assert listener["rules"][0]["RuleArn"] == "arn:rule1"

    def test_rules_have_target_groups(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {"Type": "forward", "TargetGroupArn": "arn:tg1"},
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
            ],
        )
        results = list(fetch_load_balancers(client))
        rule = results[0]["listeners"][0]["rules"][0]
        assert len(rule["targetGroups"]) == 1
        assert rule["targetGroups"][0]["TargetGroupArn"] == "arn:tg1"

    def test_target_groups_have_health(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {"Type": "forward", "TargetGroupArn": "arn:tg1"},
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
            ],
            target_health={
                "arn:tg1": [
                    {
                        "Target": {"Id": "i-123", "Port": 80},
                        "TargetHealth": {"State": "healthy"},
                    },
                ],
            },
        )
        results = list(fetch_load_balancers(client))
        tg = results[0]["listeners"][0]["rules"][0]["targetGroups"][0]
        assert len(tg["targetHealth"]) == 1
        assert tg["targetHealth"][0]["TargetHealth"]["State"] == "healthy"

    def test_weighted_forward_config(self):
        """Rules with ForwardConfig.TargetGroups should also resolve."""
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {
                            "Type": "forward",
                            "ForwardConfig": {
                                "TargetGroups": [
                                    {"TargetGroupArn": "arn:tg1", "Weight": 80},
                                    {"TargetGroupArn": "arn:tg2", "Weight": 20},
                                ],
                            },
                        },
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
                {"TargetGroupArn": "arn:tg2", "TargetGroupName": "tg-2"},
            ],
        )
        results = list(fetch_load_balancers(client))
        rule = results[0]["listeners"][0]["rules"][0]
        tg_arns = {tg["TargetGroupArn"] for tg in rule["targetGroups"]}
        assert tg_arns == {"arn:tg1", "arn:tg2"}

    def test_on_lb_callback_invoked(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
        )
        captured = []
        list(fetch_load_balancers(client, on_lb=captured.append))
        assert len(captured) == 1
        assert captured[0]["LoadBalancerArn"] == "arn:lb1"

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_load_balancers(client))
        assert results == []

    def test_pagination(self):
        client = MagicMock()
        # First page returns one LB + marker, second returns another
        client.describe_load_balancers.side_effect = [
            {
                "LoadBalancers": [
                    {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "first"},
                ],
                "NextMarker": "token1",
            },
            {
                "LoadBalancers": [
                    {"LoadBalancerArn": "arn:lb2", "LoadBalancerName": "second"},
                ],
            },
        ]
        client.describe_target_groups.return_value = {"TargetGroups": []}
        client.describe_listeners.return_value = {"Listeners": []}

        results = list(fetch_load_balancers(client))
        assert len(results) == 2
        assert results[0]["LoadBalancerArn"] == "arn:lb1"
        assert results[1]["LoadBalancerArn"] == "arn:lb2"

    def test_listener_error_yields_empty_listeners(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
        )
        client.describe_listeners.side_effect = Exception("AccessDenied")
        results = list(fetch_load_balancers(client))
        assert len(results) == 1
        assert results[0]["listeners"] == []

    def test_rule_error_yields_empty_rules(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
        )
        client.describe_rules.side_effect = Exception("AccessDenied")
        results = list(fetch_load_balancers(client))
        listener = results[0]["listeners"][0]
        assert listener["rules"] == []

    def test_target_health_error_yields_empty_list(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {"Type": "forward", "TargetGroupArn": "arn:tg1"},
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
            ],
        )
        client.describe_target_health.side_effect = Exception("AccessDenied")
        results = list(fetch_load_balancers(client))
        tg = results[0]["listeners"][0]["rules"][0]["targetGroups"][0]
        assert tg["targetHealth"] == []

    def test_rule_without_target_group_action(self):
        """Rules with non-forward actions (e.g. redirect) get empty targetGroups."""
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {
                            "Type": "redirect",
                            "RedirectConfig": {
                                "Protocol": "HTTPS",
                                "StatusCode": "HTTP_301",
                            },
                        },
                    ],
                },
            ],
        )
        results = list(fetch_load_balancers(client))
        rule = results[0]["listeners"][0]["rules"][0]
        assert rule["targetGroups"] == []

    def test_lb_has_tags(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            tags={"arn:lb1": [{"Key": "env", "Value": "prod"}]},
        )
        results = list(fetch_load_balancers(client))
        assert results[0]["tags"] == [{"Key": "env", "Value": "prod"}]

    def test_target_group_has_tags(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
            listeners=[
                {"ListenerArn": "arn:listener1", "Port": 80},
            ],
            rules=[
                {
                    "RuleArn": "arn:rule1",
                    "Actions": [
                        {"Type": "forward", "TargetGroupArn": "arn:tg1"},
                    ],
                },
            ],
            target_groups=[
                {"TargetGroupArn": "arn:tg1", "TargetGroupName": "tg-1"},
            ],
            tags={"arn:tg1": [{"Key": "team", "Value": "backend"}]},
        )
        results = list(fetch_load_balancers(client))
        tg = results[0]["listeners"][0]["rules"][0]["targetGroups"][0]
        assert tg["tags"] == [{"Key": "team", "Value": "backend"}]

    def test_tags_error_yields_empty_tags(self):
        client = _make_client(
            load_balancers=[
                {"LoadBalancerArn": "arn:lb1", "LoadBalancerName": "my-alb"},
            ],
        )
        client.describe_tags.side_effect = Exception("AccessDenied")
        results = list(fetch_load_balancers(client))
        assert results[0]["tags"] == []
