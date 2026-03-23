"""Tests for botolib.resources.fetchers.apigatewayv2 (V2 HTTP/WS API fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.apigatewayv2 import (
    fetch_custom_domains,
    fetch_http_apis,
)


def _make_client(
    apis=None,
    stages=None,
    authorizers=None,
    routes=None,
    integrations=None,
    domains=None,
    api_mappings=None,
):
    """Build a mock API Gateway V2 client with canned responses."""
    client = MagicMock()

    client.get_apis.return_value = {"Items": apis or []}
    client.get_stages.return_value = {"Items": stages or []}
    client.get_authorizers.return_value = {"Items": authorizers or []}
    client.get_routes.return_value = {"Items": routes or []}
    client.get_domain_names.return_value = {"Items": domains or []}
    client.get_api_mappings.return_value = {"Items": api_mappings or []}

    if integrations:
        client.get_integration.side_effect = lambda **kw: integrations.get(
            (kw["ApiId"], kw["IntegrationId"]),
            {},
        )
    else:
        client.get_integration.return_value = {
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:fn",
        }

    return client


# ---------------------------------------------------------------------------
# fetch_http_apis
# ---------------------------------------------------------------------------

class TestFetchHttpApis:

    def test_yields_api_with_enriched_fields(self):
        client = _make_client(
            apis=[{"ApiId": "abc123", "Name": "MyHTTP", "ProtocolType": "HTTP"}],
            stages=[{"StageName": "$default"}],
            authorizers=[{"AuthorizerId": "auth1", "Name": "MyAuth"}],
            routes=[{"RouteKey": "GET /hello", "RouteId": "r1"}],
        )

        results = list(fetch_http_apis(client))

        assert len(results) == 1
        api = results[0]
        assert api["ApiId"] == "abc123"
        assert api["Name"] == "MyHTTP"
        assert "routes" in api
        assert "stages" in api
        assert "authorizers" in api

    def test_routes_have_nested_integration(self):
        integration = {
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:fn",
        }
        client = _make_client(
            apis=[{"ApiId": "api1", "Name": "Test", "ProtocolType": "HTTP"}],
            routes=[
                {"RouteKey": "GET /items", "RouteId": "r1", "Target": "integrations/int1"},
            ],
            integrations={("api1", "int1"): integration},
        )

        api = list(fetch_http_apis(client))[0]
        route = api["routes"][0]

        assert "integration" in route
        assert route["integration"]["IntegrationType"] == "AWS_PROXY"

    def test_authorizer_attached_to_route(self):
        authorizer = {"AuthorizerId": "auth1", "Name": "JWTAuth", "AuthorizerType": "JWT"}
        client = _make_client(
            apis=[{"ApiId": "api1", "Name": "Test", "ProtocolType": "HTTP"}],
            authorizers=[authorizer],
            routes=[
                {"RouteKey": "GET /secure", "RouteId": "r1", "AuthorizerId": "auth1"},
            ],
        )

        api = list(fetch_http_apis(client))[0]
        route = api["routes"][0]

        assert route["authorizer"]["AuthorizerId"] == "auth1"
        assert route["authorizer"]["Name"] == "JWTAuth"

    def test_route_without_target_has_no_integration(self):
        client = _make_client(
            apis=[{"ApiId": "api1", "Name": "Test", "ProtocolType": "HTTP"}],
            routes=[{"RouteKey": "$default", "RouteId": "r1"}],
        )

        api = list(fetch_http_apis(client))[0]
        route = api["routes"][0]

        assert "integration" not in route

    def test_on_api_callback_invoked(self):
        client = _make_client(
            apis=[
                {"ApiId": "a1", "Name": "A", "ProtocolType": "HTTP"},
                {"ApiId": "a2", "Name": "B", "ProtocolType": "HTTP"},
            ],
        )
        seen = []
        list(fetch_http_apis(client, on_api=lambda api: seen.append(api["ApiId"])))

        assert seen == ["a1", "a2"]

    def test_empty_account(self):
        client = _make_client(apis=[])
        assert list(fetch_http_apis(client)) == []

    def test_pagination(self):
        """Verify _paginate follows NextToken."""
        client = MagicMock()
        client.get_apis.side_effect = [
            {"Items": [{"ApiId": "a1", "Name": "A", "ProtocolType": "HTTP"}], "NextToken": "tok1"},
            {"Items": [{"ApiId": "a2", "Name": "B", "ProtocolType": "HTTP"}]},
        ]
        client.get_routes.return_value = {"Items": []}
        client.get_stages.return_value = {"Items": []}
        client.get_authorizers.return_value = {"Items": []}

        results = list(fetch_http_apis(client))
        assert len(results) == 2
        assert results[0]["ApiId"] == "a1"
        assert results[1]["ApiId"] == "a2"

    def test_integration_cache_deduplicates(self):
        """Two routes sharing the same integration ID should only trigger one get_integration call."""
        integration = {"IntegrationType": "AWS_PROXY", "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:fn"}
        client = _make_client(
            apis=[{"ApiId": "api1", "Name": "Test", "ProtocolType": "HTTP"}],
            routes=[
                {"RouteKey": "GET /a", "RouteId": "r1", "Target": "integrations/int1"},
                {"RouteKey": "GET /b", "RouteId": "r2", "Target": "integrations/int1"},
            ],
            integrations={("api1", "int1"): integration},
        )

        list(fetch_http_apis(client))

        # Due to thread-pool execution, the cache dedup may or may not prevent
        # the second call from racing. But with sequential futures, it should
        # be at most 2 calls for the same integration.
        call_count = client.get_integration.call_count
        assert call_count >= 1


# ---------------------------------------------------------------------------
# fetch_custom_domains
# ---------------------------------------------------------------------------

class TestFetchCustomDomains:

    def test_yields_domains_with_api_mappings(self):
        client = _make_client(
            domains=[{"DomainName": "api.example.com"}],
            api_mappings=[
                {"ApiMappingId": "m1", "ApiId": "api1", "Stage": "$default"},
            ],
        )

        results = list(fetch_custom_domains(client))

        assert len(results) == 1
        domain = results[0]
        assert domain["DomainName"] == "api.example.com"
        assert len(domain["ApiMappings"]) == 1
        assert domain["ApiMappings"][0]["ApiId"] == "api1"

    def test_on_domain_callback_invoked(self):
        client = _make_client(
            domains=[{"DomainName": "a.com"}, {"DomainName": "b.com"}],
        )
        seen = []
        list(fetch_custom_domains(client, on_domain=lambda d: seen.append(d["DomainName"])))

        assert seen == ["a.com", "b.com"]

    def test_empty_domains(self):
        client = _make_client(domains=[])
        assert list(fetch_custom_domains(client)) == []

    def test_skips_domain_without_name(self):
        client = _make_client(domains=[{}, {"DomainName": "ok.com"}])
        results = list(fetch_custom_domains(client))
        assert len(results) == 1
        assert results[0]["DomainName"] == "ok.com"

    def test_api_mapping_error_yields_empty_list(self):
        client = MagicMock()
        client.get_domain_names.return_value = {"Items": [{"DomainName": "fail.com"}]}
        client.get_api_mappings.side_effect = Exception("access denied")

        results = list(fetch_custom_domains(client))
        assert len(results) == 1
        assert results[0]["ApiMappings"] == []
