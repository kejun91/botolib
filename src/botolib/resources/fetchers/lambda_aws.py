"""
Fetch all AWS Lambda functions for a given AWS account.

Aggregates raw boto3 responses: functions with nested configuration,
code location, and tags.

Usage:
    from botolib.resources.fetchers.lambda_aws import fetch_functions
    client = boto3_session.client("lambda", region_name="eu-west-1")
    functions = list(fetch_functions(client))
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

CONCURRENCY = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _paginate(client, action: str, params: dict, items_key: str) -> list:
    """Paginate a Lambda call using Marker / NextMarker."""
    all_items: list = []
    marker: str | None = None
    while True:
        req = {**params}
        if marker:
            req["Marker"] = marker
        resp = getattr(client, action)(**req)
        all_items.extend(resp.get(items_key, []))
        marker = resp.get("NextMarker")
        if not marker:
            break
    return all_items


# ---------------------------------------------------------------------------
# Per-function fetchers
# ---------------------------------------------------------------------------

def _fetch_function_detail(client, function_name: str) -> dict | None:
    """Call get_function for a single Lambda function.

    Returns the raw get_function response (Configuration + Code + Tags etc.)
    or None on error.
    """
    try:
        return client.get_function(FunctionName=function_name)
    except Exception as e:
        logger.warning("get_function failed for %s: %s", function_name, e)
        return None


def _fetch_tags(client, function_arn: str) -> dict[str, str]:
    """Return tags for a single Lambda function."""
    try:
        resp = client.list_tags(Resource=function_arn)
        return resp.get("Tags", {})
    except Exception as e:
        logger.warning("list_tags failed for %s: %s", function_arn, e)
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_functions(
    client: Any,
    on_function: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield Lambda functions one at a time.

    Each yielded dict is the raw list_functions item enriched with:
      - "detail": raw get_function response (Configuration, Code, Tags, etc.)
      - "tags": raw list_tags response (dict of tag key/value pairs)

    Args:
        client: A Lambda boto3 client.
        on_function: Optional callback invoked with each function dict as it is yielded.

    Yields:
        dict for each Lambda function found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        functions = _paginate(client, "list_functions", {}, "Functions")
        logger.info("Found %d Lambda functions", len(functions))

        for fn in functions:
            fn_name = fn.get("FunctionName", "")
            fn_arn = fn.get("FunctionArn", "")
            logger.info("Fetching details for function: %s", fn_name)

            detail_fut = executor.submit(_fetch_function_detail, client, fn_name)
            tags_fut = executor.submit(_fetch_tags, client, fn_arn)

            detail = detail_fut.result()
            if detail is not None:
                fn["detail"] = detail

            fn["tags"] = tags_fut.result()

            if on_function:
                on_function(fn)
            yield fn
    finally:
        executor.shutdown(wait=False)
