"""Tests for botolib.resources.fetchers.route53 (Route 53 hosted zone fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.route53 import fetch_hosted_zones


def _make_client(hosted_zones=None, record_sets=None, tags=None):
    """Build a mock Route 53 client with canned responses."""
    client = MagicMock()

    client.list_hosted_zones.return_value = {
        "HostedZones": hosted_zones or [],
        "IsTruncated": False,
    }

    if record_sets is not None:
        if isinstance(record_sets, dict):
            # Map zone ID -> records list
            client.list_resource_record_sets.side_effect = lambda **kw: {
                "ResourceRecordSets": record_sets.get(
                    kw["HostedZoneId"], []
                ),
                "IsTruncated": False,
            }
        else:
            client.list_resource_record_sets.return_value = {
                "ResourceRecordSets": record_sets,
                "IsTruncated": False,
            }
    else:
        client.list_resource_record_sets.return_value = {
            "ResourceRecordSets": [],
            "IsTruncated": False,
        }

    if tags is not None:
        # tags: dict mapping zone resource ID -> list of tag dicts
        client.list_tags_for_resource.side_effect = lambda **kw: {
            "ResourceTagSet": {
                "Tags": tags.get(kw["ResourceId"], []),
            },
        }
    else:
        client.list_tags_for_resource.return_value = {
            "ResourceTagSet": {"Tags": []},
        }

    return client


class TestFetchHostedZones:

    def test_yields_zone_with_record_sets(self):
        records = [
            {"Name": "example.com.", "Type": "A", "TTL": 300,
             "ResourceRecords": [{"Value": "1.2.3.4"}]},
            {"Name": "example.com.", "Type": "NS", "TTL": 172800,
             "ResourceRecords": [{"Value": "ns1.example.com."}]},
        ]
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com.",
                 "CallerReference": "ref1"},
            ],
            record_sets={"/hostedzone/Z1": records},
        )
        results = list(fetch_hosted_zones(client))
        assert len(results) == 1
        zone = results[0]
        assert zone["Id"] == "/hostedzone/Z1"
        assert zone["Name"] == "example.com."
        assert len(zone["resourceRecordSets"]) == 2
        assert zone["resourceRecordSets"][0]["Type"] == "A"

    def test_multiple_zones(self):
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
                {"Id": "/hostedzone/Z2", "Name": "other.com."},
            ],
            record_sets={
                "/hostedzone/Z1": [
                    {"Name": "example.com.", "Type": "A",
                     "ResourceRecords": [{"Value": "1.2.3.4"}]},
                ],
                "/hostedzone/Z2": [
                    {"Name": "other.com.", "Type": "CNAME",
                     "ResourceRecords": [{"Value": "alias.other.com."}]},
                ],
            },
        )
        results = list(fetch_hosted_zones(client))
        assert len(results) == 2
        assert results[0]["Name"] == "example.com."
        assert results[1]["Name"] == "other.com."
        assert len(results[0]["resourceRecordSets"]) == 1
        assert len(results[1]["resourceRecordSets"]) == 1

    def test_on_zone_callback_invoked(self):
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
            ],
        )
        captured = []
        list(fetch_hosted_zones(client, on_zone=captured.append))
        assert len(captured) == 1
        assert captured[0]["Id"] == "/hostedzone/Z1"

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_hosted_zones(client))
        assert results == []

    def test_hosted_zone_pagination(self):
        client = MagicMock()
        client.list_hosted_zones.side_effect = [
            {
                "HostedZones": [
                    {"Id": "/hostedzone/Z1", "Name": "first.com."},
                ],
                "IsTruncated": True,
                "NextMarker": "marker1",
            },
            {
                "HostedZones": [
                    {"Id": "/hostedzone/Z2", "Name": "second.com."},
                ],
                "IsTruncated": False,
            },
        ]
        client.list_resource_record_sets.return_value = {
            "ResourceRecordSets": [],
            "IsTruncated": False,
        }

        results = list(fetch_hosted_zones(client))
        assert len(results) == 2
        assert results[0]["Name"] == "first.com."
        assert results[1]["Name"] == "second.com."
        # Verify Marker was passed on second call
        calls = client.list_hosted_zones.call_args_list
        assert calls[1].kwargs.get("Marker") == "marker1" or \
            calls[1][1].get("Marker") == "marker1"

    def test_record_set_pagination(self):
        client = MagicMock()
        client.list_hosted_zones.return_value = {
            "HostedZones": [
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
            ],
            "IsTruncated": False,
        }
        client.list_resource_record_sets.side_effect = [
            {
                "ResourceRecordSets": [
                    {"Name": "a.example.com.", "Type": "A",
                     "ResourceRecords": [{"Value": "1.1.1.1"}]},
                ],
                "IsTruncated": True,
                "NextRecordName": "b.example.com.",
                "NextRecordType": "A",
            },
            {
                "ResourceRecordSets": [
                    {"Name": "b.example.com.", "Type": "A",
                     "ResourceRecords": [{"Value": "2.2.2.2"}]},
                ],
                "IsTruncated": False,
            },
        ]

        results = list(fetch_hosted_zones(client))
        assert len(results[0]["resourceRecordSets"]) == 2

    def test_record_set_error_yields_empty_list(self):
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
            ],
        )
        client.list_resource_record_sets.side_effect = Exception("AccessDenied")
        results = list(fetch_hosted_zones(client))
        assert len(results) == 1
        assert results[0]["resourceRecordSets"] == []

    def test_zone_has_tags(self):
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
            ],
            tags={"Z1": [{"Key": "env", "Value": "prod"}]},
        )
        results = list(fetch_hosted_zones(client))
        assert results[0]["tags"] == [{"Key": "env", "Value": "prod"}]

    def test_tags_error_yields_empty_tags(self):
        client = _make_client(
            hosted_zones=[
                {"Id": "/hostedzone/Z1", "Name": "example.com."},
            ],
        )
        client.list_tags_for_resource.side_effect = Exception("AccessDenied")
        results = list(fetch_hosted_zones(client))
        assert results[0]["tags"] == []
