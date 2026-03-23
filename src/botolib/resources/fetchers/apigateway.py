"""
Fetch all API Gateway V1 (REST API) data for a given AWS account.

Aggregates raw boto3 responses: REST APIs with nested resources,
integrations, methods, stages, authorizers, and documentation parts.
Custom domains with base path mappings are available separately.

Usage:
    from botolib.resources.fetchers.apigateway import fetch_rest_apis, fetch_custom_domains
    client = boto3_session.client("apigateway", region_name="eu-west-1")
    apis = list(fetch_rest_apis(client))
    domains = list(fetch_custom_domains(client))
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

def _remove_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _paginate(client, action: str, params: dict, items_key: str) -> list:
    """Paginate a V1 API Gateway call using position-based tokens."""
    all_items: list = []
    position = None
    while True:
        req = {**params}
        if position:
            req["position"] = position
        resp = getattr(client, action)(**_remove_none(req))
        all_items.extend(resp.get(items_key) or resp.get("items") or [])
        position = resp.get("position")
        if not position:
            break
    return all_items


# ---------------------------------------------------------------------------
# Per-resource fetchers
# ---------------------------------------------------------------------------

def _fetch_method(
    client,
    rest_api_id: str,
    resource: dict,
    http_method: str,
    authorizer_map: dict[str, dict],
) -> dict | None:
    """Fetch method + integration for one resource x method pair.

    Returns the raw get_method response enriched with:
      - "integration": raw get_integration response
      - "authorizer": raw authorizer dict (if any)
    """
    try:
        method = client.get_method(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod=http_method,
        )
    except Exception as e:
        logger.debug("get_method failed %s %s %s: %s", rest_api_id, resource.get("path"), http_method, e)
        return None

    try:
        method["integration"] = client.get_integration(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod=http_method,
        )
    except Exception as e:
        logger.debug("get_integration failed %s %s %s: %s", rest_api_id, resource.get("path"), http_method, e)

    auth_id = method.get("authorizerId")
    if auth_id and auth_id in authorizer_map:
        method["authorizer"] = authorizer_map[auth_id]

    return method


def _enrich_resources(
    client,
    rest_api_id: str,
    resources: list[dict],
    authorizer_map: dict[str, dict],
    executor: ThreadPoolExecutor,
) -> None:
    """Enrich each resource dict in-place with a "methods" map."""
    futures: list[tuple] = []

    for resource in resources:
        resource_methods = resource.get("resourceMethods") or {}
        for http_method in resource_methods:
            f = executor.submit(
                _fetch_method,
                client, rest_api_id, resource, http_method, authorizer_map,
            )
            futures.append((resource, http_method, f))

    for resource, http_method, f in futures:
        result = f.result()
        if result:
            resource.setdefault("methods", {})[http_method] = result


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def fetch_rest_apis(
    client: Any,
    on_api: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield V1 REST API data one API at a time.

    Each yielded dict is the raw get_rest_apis item enriched with:
      - "stages": raw get_stages response items
      - "authorizers": raw get_authorizers response items
      - "resources": raw get_resources items, each with nested "methods"
      - "documentationParts": raw get_documentation_parts items

    Args:
        client: An API Gateway boto3 client.
        on_api: Optional callback invoked with each API dict as it is yielded.

    Yields:
        dict for each REST API found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        rest_apis = _paginate(client, "get_rest_apis", {}, "items")
        logger.info("Found %d REST APIs", len(rest_apis))

        for api in rest_apis:
            api_id = api["id"]
            logger.info("Fetching details for REST API: %s (%s)", api.get("name", ""), api_id)

            # Fetch child resources in parallel
            stages_fut = executor.submit(
                lambda aid: (client.get_stages(restApiId=aid).get("item") or []),
                api_id,
            )
            auth_fut = executor.submit(
                _paginate, client, "get_authorizers", {"restApiId": api_id}, "items",
            )
            resources_fut = executor.submit(
                _paginate, client, "get_resources", {"restApiId": api_id}, "items",
            )
            docs_fut = executor.submit(
                _paginate, client, "get_documentation_parts", {"restApiId": api_id}, "items",
            )

            stages = stages_fut.result()
            authorizers = auth_fut.result()
            resources = resources_fut.result()
            docs = docs_fut.result()

            authorizer_map = {a["id"]: a for a in authorizers}

            # Enrich resources with method + integration details
            _enrich_resources(client, api_id, resources, authorizer_map, executor)

            api["stages"] = stages
            api["authorizers"] = authorizers
            api["resources"] = resources
            api["documentationParts"] = docs

            if on_api:
                on_api(api)
            yield api
    finally:
        executor.shutdown(wait=False)


def fetch_custom_domains(
    client: Any,
    on_domain: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield V1 custom domains with their base path mappings.

    Each yielded dict is the raw get_domain_names item enriched with
    a "basePathMappings" key containing the raw get_base_path_mappings items.

    Args:
        client: An API Gateway boto3 client.
        on_domain: Optional callback invoked with each domain dict as it is yielded.

    Yields:
        dict for each custom domain found.
    """
    try:
        domains = _paginate(client, "get_domain_names", {}, "items")
        logger.info("Found %d V1 custom domains", len(domains))

        for domain in domains:
            domain_name = domain.get("domainName")
            if not domain_name:
                continue

            try:
                domain["basePathMappings"] = _paginate(
                    client, "get_base_path_mappings", {"domainName": domain_name}, "items",
                )
            except Exception as e:
                logger.warning("Failed to fetch base path mappings for %s: %s", domain_name, e)
                domain["basePathMappings"] = []

            if on_domain:
                on_domain(domain)
            yield domain
    except Exception as e:
        logger.warning("Failed to fetch V1 custom domains: %s", e)
