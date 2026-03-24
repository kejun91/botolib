"""Tests for botolib.resources.fetchers.lambda_aws (Lambda function fetcher)."""

from unittest.mock import MagicMock

from botolib.resources.fetchers.lambda_aws import fetch_functions


def _make_client(functions=None, get_function_resp=None, tags=None):
    """Build a mock Lambda client with canned responses."""
    client = MagicMock()

    client.list_functions.return_value = {
        "Functions": functions or [],
    }

    if get_function_resp is not None:
        if isinstance(get_function_resp, dict):
            # Map function name -> get_function response
            client.get_function.side_effect = lambda **kw: get_function_resp[
                kw["FunctionName"]
            ]
        else:
            client.get_function.return_value = get_function_resp
    else:
        client.get_function.return_value = {
            "Configuration": {"FunctionName": "fn", "Runtime": "python3.12"},
            "Code": {"Location": "https://awslambda.example.com/code"},
        }

    if tags is not None:
        if isinstance(tags, dict):
            # Map ARN -> tags dict
            client.list_tags.side_effect = lambda **kw: {
                "Tags": tags.get(kw["Resource"], {}),
            }
        else:
            client.list_tags.return_value = {"Tags": tags}
    else:
        client.list_tags.return_value = {"Tags": {}}

    return client


class TestFetchFunctions:

    def test_yields_function_with_detail_and_tags(self):
        detail = {
            "Configuration": {"FunctionName": "my-fn", "Runtime": "python3.12"},
            "Code": {"Location": "https://example.com/code"},
        }
        client = _make_client(
            functions=[
                {"FunctionName": "my-fn", "FunctionArn": "arn:aws:lambda:us-east-1:123:function:my-fn"},
            ],
            get_function_resp={"my-fn": detail},
            tags={"arn:aws:lambda:us-east-1:123:function:my-fn": {"env": "prod"}},
        )
        results = list(fetch_functions(client))
        assert len(results) == 1
        fn = results[0]
        assert fn["FunctionName"] == "my-fn"
        assert fn["detail"] == detail
        assert fn["tags"] == {"env": "prod"}

    def test_multiple_functions(self):
        client = _make_client(
            functions=[
                {"FunctionName": "fn1", "FunctionArn": "arn:fn1"},
                {"FunctionName": "fn2", "FunctionArn": "arn:fn2"},
            ],
        )
        results = list(fetch_functions(client))
        assert len(results) == 2
        assert results[0]["FunctionName"] == "fn1"
        assert results[1]["FunctionName"] == "fn2"

    def test_on_function_callback_invoked(self):
        client = _make_client(
            functions=[
                {"FunctionName": "my-fn", "FunctionArn": "arn:fn"},
            ],
        )
        captured = []
        list(fetch_functions(client, on_function=captured.append))
        assert len(captured) == 1
        assert captured[0]["FunctionName"] == "my-fn"

    def test_empty_account(self):
        client = _make_client()
        results = list(fetch_functions(client))
        assert results == []

    def test_pagination(self):
        client = MagicMock()
        client.list_functions.side_effect = [
            {
                "Functions": [
                    {"FunctionName": "fn1", "FunctionArn": "arn:fn1"},
                ],
                "NextMarker": "token1",
            },
            {
                "Functions": [
                    {"FunctionName": "fn2", "FunctionArn": "arn:fn2"},
                ],
            },
        ]
        client.get_function.return_value = {
            "Configuration": {"FunctionName": "fn"},
            "Code": {},
        }
        client.list_tags.return_value = {"Tags": {}}

        results = list(fetch_functions(client))
        assert len(results) == 2
        assert results[0]["FunctionName"] == "fn1"
        assert results[1]["FunctionName"] == "fn2"

    def test_get_function_error_skips_detail(self):
        client = _make_client(
            functions=[
                {"FunctionName": "my-fn", "FunctionArn": "arn:fn"},
            ],
        )
        client.get_function.side_effect = Exception("AccessDenied")
        results = list(fetch_functions(client))
        assert len(results) == 1
        assert "detail" not in results[0]

    def test_list_tags_error_yields_empty_tags(self):
        client = _make_client(
            functions=[
                {"FunctionName": "my-fn", "FunctionArn": "arn:fn"},
            ],
        )
        client.list_tags.side_effect = Exception("AccessDenied")
        results = list(fetch_functions(client))
        assert len(results) == 1
        assert results[0]["tags"] == {}
