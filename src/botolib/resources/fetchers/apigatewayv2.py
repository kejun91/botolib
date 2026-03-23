"""
Fetch all API Gateway V2 (HTTP & WebSocket API) data for a given AWS account.

Aggregates raw boto3 responses: HTTP/WS APIs with nested routes,
integrations, stages, and authorizers.
Custom domains with API mappings are available separately.

Usage:
    from botolib.resources.fetchers.apigatewayv2 import fetch_http_apis, fetch_custom_domains
    client = boto3_session.client("apigatewayv2", region_name="eu-west-1")
    apis = list(fetch_http_apis(client))
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
    """Paginate a V2 API Gateway call using NextToken-based tokens."""
    all_items: list = []
    next_token = None
    while True:
        req = {**params}
        if next_token:
            req["NextToken"] = next_token
        resp = getattr(client, action)(**_remove_none(req))
        all_items.extend(resp.get(items_key) or resp.get("Items") or [])
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return all_items


# ---------------------------------------------------------------------------
# Per-route fetcher
# ---------------------------------------------------------------------------

def _enrich_route(
    client,
    api_id: str,
    route: dict,
    integrations_cache: dict[str, dict],
    authorizer_map: dict[str, dict],
) -> None:
    """Enrich a route dict in-place with integration and authorizer."""
    target = route.get("Target")
    if target:
        integration_id = target.replace("integrations/", "")
        integration = integrations_cache.get(integration_id)
        if not integration:
            try:
                integration = client.get_integration(
                    ApiId=api_id,
                    IntegrationId=integration_id,
                )
                integrations_cache[integration_id] = integration
            except Exception as e:
                logger.debug("get_integration failed for route %s: %s", route.get("RouteKey"), e)
        if integration:
            route["integration"] = integration

    auth_id = route.get("AuthorizerId")
    if auth_id and auth_id in authorizer_map:
        route["authorizer"] = authorizer_map[auth_id]


def _enrich_routes(
    client,
    api_id: str,
    routes: list[dict],
    authorizer_map: dict[str, dict],
    executor: ThreadPoolExecutor,
) -> None:
    """Enrich all route dicts in-place with integration + authorizer."""
    integrations_cache: dict[str, dict] = {}
    futures = []

    for route in routes:
        f = executor.submit(
            _enrich_route, client, api_id, route, integrations_cache, authorizer_map,
        )
        futures.append(f)

    for f in futures:
        f.result()


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def fetch_http_apis(
    client: Any,
    on_api: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield V2 HTTP/WebSocket API data one API at a time.

    Each yielded dict is the raw get_apis item enriched with:
      - "stages": raw get_stages response items
      - "authorizers": raw get_authorizers response items
      - "routes": raw get_routes items, each with nested "integration"
        and "authorizer"

    Args:
        client: An API Gateway V2 boto3 client.
        on_api: Optional callback invoked with each API dict as it is yielded.

    Yields:
        dict for each HTTP/WebSocket API found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        apis = _paginate(client, "get_apis", {}, "Items")
        logger.info("Found %d HTTP/WebSocket APIs", len(apis))

        for api in apis:
            api_id = api["ApiId"]
            api_name = api.get("Name", "")
            protocol = api.get("ProtocolType", "HTTP")
            logger.info("Fetching details for %s API: %s (%s)", protocol, api_name, api_id)

            # Fetch child resources in parallel
            routes_fut = executor.submit(
                _paginate, client, "get_routes", {"ApiId": api_id}, "Items",
            )
            stages_fut = executor.submit(
                _paginate, client, "get_stages", {"ApiId": api_id}, "Items",
            )
            auth_fut = executor.submit(
                _paginate, client, "get_authorizers", {"ApiId": api_id}, "Items",
            )

            routes = routes_fut.result()
            stages = stages_fut.result()
            authorizers = auth_fut.result()

            authorizer_map = {a["AuthorizerId"]: a for a in authorizers}

            # Enrich routes with integration + authorizer details
            _enrich_routes(client, api_id, routes, authorizer_map, executor)

            api["routes"] = routes
            api["stages"] = stages
            api["authorizers"] = authorizers

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
    Yield V2 custom domains with their API mappings.

    Each yielded dict is the raw get_domain_names item enriched with
    an "ApiMappings" key containing the raw get_api_mappings items.

    Args:
        client: An API Gateway V2 boto3 client.
        on_domain: Optional callback invoked with each domain dict as it is yielded.

    Yields:
        dict for each custom domain found.
    """
    try:
        domains = _paginate(client, "get_domain_names", {}, "Items")
        logger.info("Found %d V2 custom domains", len(domains))

        for domain in domains:
            domain_name = domain.get("DomainName")
            if not domain_name:
                continue

            try:
                domain["ApiMappings"] = _paginate(
                    client, "get_api_mappings", {"DomainName": domain_name}, "Items",
                )
            except Exception as e:
                logger.warning("Failed to fetch API mappings for %s: %s", domain_name, e)
                domain["ApiMappings"] = []

            if on_domain:
                on_domain(domain)
            yield domain
    except Exception as e:
        logger.warning("Failed to fetch V2 custom domains: %s", e)
