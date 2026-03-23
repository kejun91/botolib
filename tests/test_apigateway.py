"""Tests for botolib.resources.fetchers.apigateway (V1 REST API fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.apigateway import (
    fetch_custom_domains,
    fetch_rest_apis,
)


def _make_client(
    rest_apis=None,
    stages=None,
    authorizers=None,
    resources=None,
    doc_parts=None,
    methods=None,
    integrations=None,
    domains=None,
    base_path_mappings=None,
):
    """Build a mock API Gateway V1 client with canned responses."""
    client = MagicMock()

    client.get_rest_apis.return_value = {"items": rest_apis or []}
    client.get_stages.return_value = {"item": stages or []}
    client.get_authorizers.return_value = {"items": authorizers or []}
    client.get_resources.return_value = {"items": resources or []}
    client.get_documentation_parts.return_value = {"items": doc_parts or []}
    client.get_domain_names.return_value = {"items": domains or []}
    client.get_base_path_mappings.return_value = {"items": base_path_mappings or []}

    if methods:
        client.get_method.side_effect = lambda **kw: methods.get(
            (kw["restApiId"], kw["resourceId"], kw["httpMethod"]),
            {},
        )
    else:
        client.get_method.return_value = {"httpMethod": "GET"}

    if integrations:
        client.get_integration.side_effect = lambda **kw: integrations.get(
            (kw["restApiId"], kw["resourceId"], kw["httpMethod"]),
            {},
        )
    else:
        client.get_integration.return_value = {"type": "AWS_PROXY", "uri": "arn:aws:lambda:us-east-1:123:function:my-fn"}

    return client


# ---------------------------------------------------------------------------
# fetch_rest_apis
# ---------------------------------------------------------------------------

class TestFetchRestApis:

    def test_yields_api_with_enriched_fields(self):
        client = _make_client(
            rest_apis=[{"id": "api1", "name": "MyAPI", "description": "desc"}],
            stages=[{"stageName": "$default"}],
            authorizers=[{"id": "auth1", "name": "MyAuth", "type": "TOKEN"}],
            resources=[
                {
                    "id": "res1",
                    "path": "/hello",
                    "resourceMethods": {"GET": {}},
                },
            ],
            doc_parts=[],
        )

        results = list(fetch_rest_apis(client))

        assert len(results) == 1
        api = results[0]
        assert api["id"] == "api1"
        assert api["name"] == "MyAPI"
        assert "stages" in api
        assert "authorizers" in api
        assert "resources" in api
        assert "documentationParts" in api

    def test_resources_have_nested_methods(self):
        method_resp = {"httpMethod": "GET", "authorizationType": "NONE"}
        integration_resp = {"type": "AWS_PROXY", "uri": "arn:aws:lambda:us-east-1:123:function:fn"}

        client = _make_client(
            rest_apis=[{"id": "api1", "name": "Test"}],
            resources=[
                {
                    "id": "res1",
                    "path": "/items",
                    "resourceMethods": {"GET": {}, "POST": {}},
                },
            ],
            methods={
                ("api1", "res1", "GET"): {**method_resp, "httpMethod": "GET"},
                ("api1", "res1", "POST"): {**method_resp, "httpMethod": "POST"},
            },
            integrations={
                ("api1", "res1", "GET"): integration_resp,
                ("api1", "res1", "POST"): integration_resp,
            },
        )

        api = list(fetch_rest_apis(client))[0]
        resource = api["resources"][0]

        assert "methods" in resource
        assert "GET" in resource["methods"]
        assert "POST" in resource["methods"]
        assert resource["methods"]["GET"]["integration"]["type"] == "AWS_PROXY"

    def test_authorizer_attached_to_method(self):
        authorizer = {"id": "auth1", "name": "MyJWT", "type": "TOKEN"}
        method_resp = {"httpMethod": "GET", "authorizerId": "auth1"}

        client = _make_client(
            rest_apis=[{"id": "api1", "name": "Test"}],
            authorizers=[authorizer],
            resources=[
                {"id": "res1", "path": "/secure", "resourceMethods": {"GET": {}}},
            ],
            methods={("api1", "res1", "GET"): method_resp},
        )

        api = list(fetch_rest_apis(client))[0]
        method = api["resources"][0]["methods"]["GET"]

        assert method["authorizer"]["id"] == "auth1"
        assert method["authorizer"]["name"] == "MyJWT"

    def test_on_api_callback_invoked(self):
        client = _make_client(
            rest_apis=[{"id": "api1", "name": "A"}, {"id": "api2", "name": "B"}],
        )
        seen = []
        list(fetch_rest_apis(client, on_api=lambda api: seen.append(api["id"])))

        assert seen == ["api1", "api2"]

    def test_empty_account(self):
        client = _make_client(rest_apis=[])
        assert list(fetch_rest_apis(client)) == []

    def test_pagination(self):
        """Verify _paginate follows position tokens."""
        client = MagicMock()
        # First page returns one item + position, second page returns one item + no position
        client.get_rest_apis.side_effect = [
            {"items": [{"id": "api1", "name": "A"}], "position": "tok1"},
            {"items": [{"id": "api2", "name": "B"}]},
        ]
        # Stubs for the per-api calls
        client.get_stages.return_value = {"item": []}
        client.get_authorizers.return_value = {"items": []}
        client.get_resources.return_value = {"items": []}
        client.get_documentation_parts.return_value = {"items": []}

        results = list(fetch_rest_apis(client))
        assert len(results) == 2
        assert results[0]["id"] == "api1"
        assert results[1]["id"] == "api2"


# ---------------------------------------------------------------------------
# fetch_custom_domains
# ---------------------------------------------------------------------------

class TestFetchCustomDomains:

    def test_yields_domains_with_base_path_mappings(self):
        client = _make_client(
            domains=[{"domainName": "api.example.com"}],
            base_path_mappings=[
                {"basePath": "/", "restApiId": "api1", "stage": "prod"},
            ],
        )

        results = list(fetch_custom_domains(client))

        assert len(results) == 1
        domain = results[0]
        assert domain["domainName"] == "api.example.com"
        assert len(domain["basePathMappings"]) == 1
        assert domain["basePathMappings"][0]["restApiId"] == "api1"

    def test_on_domain_callback_invoked(self):
        client = _make_client(
            domains=[{"domainName": "a.com"}, {"domainName": "b.com"}],
        )
        seen = []
        list(fetch_custom_domains(client, on_domain=lambda d: seen.append(d["domainName"])))

        assert seen == ["a.com", "b.com"]

    def test_empty_domains(self):
        client = _make_client(domains=[])
        assert list(fetch_custom_domains(client)) == []

    def test_skips_domain_without_name(self):
        client = _make_client(domains=[{}, {"domainName": "ok.com"}])
        results = list(fetch_custom_domains(client))
        assert len(results) == 1
        assert results[0]["domainName"] == "ok.com"

    def test_base_path_mapping_error_yields_empty_list(self):
        client = MagicMock()
        client.get_domain_names.return_value = {"items": [{"domainName": "fail.com"}]}
        client.get_base_path_mappings.side_effect = Exception("access denied")

        results = list(fetch_custom_domains(client))
        assert len(results) == 1
        assert results[0]["basePathMappings"] == []
