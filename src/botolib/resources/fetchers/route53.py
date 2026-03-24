"""
Fetch all Route 53 hosted zones and their resource record sets.

Aggregates raw boto3 responses: hosted zones with nested record sets.

Usage:
    from botolib.resources.fetchers.route53 import fetch_hosted_zones
    client = boto3_session.client("route53")
    zones = list(fetch_hosted_zones(client))
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

def _paginate_hosted_zones(client) -> list[dict]:
    """Paginate list_hosted_zones using Marker / NextMarker."""
    all_zones: list[dict] = []
    marker: str | None = None
    while True:
        params: dict[str, str] = {}
        if marker:
            params["Marker"] = marker
        resp = client.list_hosted_zones(**params)
        all_zones.extend(resp.get("HostedZones", []))
        if resp.get("IsTruncated"):
            marker = resp.get("NextMarker")
        else:
            break
    return all_zones


def _paginate_record_sets(client, hosted_zone_id: str) -> list[dict]:
    """Paginate list_resource_record_sets using StartRecordName/Type."""
    all_records: list[dict] = []
    params: dict[str, str] = {"HostedZoneId": hosted_zone_id}
    while True:
        resp = client.list_resource_record_sets(**params)
        all_records.extend(resp.get("ResourceRecordSets", []))
        if resp.get("IsTruncated"):
            params = {
                "HostedZoneId": hosted_zone_id,
                "StartRecordName": resp["NextRecordName"],
                "StartRecordType": resp["NextRecordType"],
            }
            if "NextRecordIdentifier" in resp:
                params["StartRecordIdentifier"] = resp["NextRecordIdentifier"]
        else:
            break
    return all_records


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _fetch_zone_tags(client, zone_id: str) -> list[dict]:
    """Fetch tags for a hosted zone."""
    # zone_id comes as '/hostedzone/Z123', need just 'Z123'
    resource_id = zone_id.split("/")[-1] if "/" in zone_id else zone_id
    try:
        resp = client.list_tags_for_resource(
            ResourceType="hostedzone", ResourceId=resource_id,
        )
        return resp.get("ResourceTagSet", {}).get("Tags", [])
    except Exception as e:
        logger.warning("Failed to fetch tags for zone %s: %s", zone_id, e)
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_hosted_zones(
    client: Any,
    on_zone: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield Route 53 hosted zones one at a time.

    Each yielded dict is the raw list_hosted_zones item enriched with:
      - "resourceRecordSets": raw list_resource_record_sets items

    Args:
        client: A Route 53 boto3 client.
        on_zone: Optional callback invoked with each zone dict as it is yielded.

    Yields:
        dict for each hosted zone found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        zones = _paginate_hosted_zones(client)
        logger.info("Found %d hosted zones", len(zones))

        for zone in zones:
            zone_id = zone.get("Id", "")
            logger.info(
                "Fetching record sets for zone: %s (%s)",
                zone.get("Name", ""), zone_id,
            )

            records_fut = executor.submit(_paginate_record_sets, client, zone_id)
            tags_fut = executor.submit(_fetch_zone_tags, client, zone_id)

            try:
                zone["resourceRecordSets"] = records_fut.result()
            except Exception as e:
                logger.warning(
                    "Failed to fetch record sets for %s: %s", zone_id, e,
                )
                zone["resourceRecordSets"] = []

            zone["tags"] = tags_fut.result()

            if on_zone:
                on_zone(zone)
            yield zone
    finally:
        executor.shutdown(wait=False)
