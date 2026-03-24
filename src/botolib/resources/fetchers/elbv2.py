"""
Fetch all ELBv2 (ALB / NLB / GWLB) data for a given AWS account.

Aggregates raw boto3 responses: load balancers with nested listeners,
rules, target groups and target health.

Usage:
    from botolib.resources.fetchers.elbv2 import fetch_load_balancers
    client = boto3_session.client("elbv2", region_name="eu-west-1")
    lbs = list(fetch_load_balancers(client))
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
    """Paginate an ELBv2 call using Marker / NextToken."""
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
# Tags
# ---------------------------------------------------------------------------

def _fetch_tags_batched(client, arns: list[str]) -> dict[str, list[dict]]:
    """Fetch tags for up to 20 ARNs at a time. Returns ARN -> Tags list."""
    result: dict[str, list[dict]] = {}
    for i in range(0, len(arns), 20):
        batch = arns[i:i + 20]
        try:
            resp = client.describe_tags(ResourceArns=batch)
            for desc in resp.get("TagDescriptions", []):
                result[desc["ResourceArn"]] = desc.get("Tags", [])
        except Exception as e:
            logger.warning("Failed to fetch tags for batch: %s", e)
    return result


# ---------------------------------------------------------------------------
# Per-resource fetchers
# ---------------------------------------------------------------------------

def _fetch_target_health(client, target_group_arn: str) -> list[dict]:
    """Return target health descriptions for a single target group."""
    try:
        resp = client.describe_target_health(TargetGroupArn=target_group_arn)
        return resp.get("TargetHealthDescriptions", [])
    except Exception as e:
        logger.warning("Failed to fetch target health for %s: %s", target_group_arn, e)
        return []


def _fetch_rules(client, listener_arn: str) -> list[dict]:
    """Return all rules for a single listener."""
    try:
        return _paginate(
            client, "describe_rules",
            {"ListenerArn": listener_arn}, "Rules",
        )
    except Exception as e:
        logger.warning("Failed to fetch rules for listener %s: %s", listener_arn, e)
        return []


def _enrich_rules_with_target_groups(
    client,
    rules: list[dict],
    tg_map: dict[str, dict],
    executor: ThreadPoolExecutor,
) -> None:
    """Attach matching target groups (with health) to each rule in-place."""
    for rule in rules:
        rule_tg_arns: set[str] = set()
        for action in rule.get("Actions", []):
            # Simple forward
            tg_arn = action.get("TargetGroupArn")
            if tg_arn:
                rule_tg_arns.add(tg_arn)
            # Weighted forward
            fwd_config = action.get("ForwardConfig", {})
            for tg_entry in fwd_config.get("TargetGroups", []):
                tg_arn = tg_entry.get("TargetGroupArn")
                if tg_arn:
                    rule_tg_arns.add(tg_arn)

        rule_tgs: list[dict] = []
        for arn in rule_tg_arns:
            tg = tg_map.get(arn)
            if tg:
                rule_tgs.append(tg)
        rule["targetGroups"] = rule_tgs

    # Now enrich each referenced TG with health (deduplicated)
    health_futures: dict[str, Any] = {}
    for rule in rules:
        for tg in rule.get("targetGroups", []):
            arn = tg["TargetGroupArn"]
            if arn not in health_futures and "targetHealth" not in tg:
                health_futures[arn] = (tg, executor.submit(_fetch_target_health, client, arn))

    for arn, (tg, fut) in health_futures.items():
        tg["targetHealth"] = fut.result()

    # Copy health to any duplicates sharing the same ARN
    health_cache: dict[str, list] = {
        arn: tg["targetHealth"] for arn, (tg, _) in health_futures.items()
    }
    for rule in rules:
        for tg in rule.get("targetGroups", []):
            if "targetHealth" not in tg:
                tg["targetHealth"] = health_cache.get(tg["TargetGroupArn"], [])


def _fetch_listeners_with_rules(
    client,
    lb_arn: str,
    tg_map: dict[str, dict],
    executor: ThreadPoolExecutor,
) -> list[dict]:
    """Fetch listeners for a LB, then enrich each with rules + target groups."""
    try:
        listeners = _paginate(
            client, "describe_listeners",
            {"LoadBalancerArn": lb_arn}, "Listeners",
        )
    except Exception as e:
        logger.warning("Failed to fetch listeners for %s: %s", lb_arn, e)
        return []

    # Fetch rules for all listeners concurrently
    rule_futures = [
        (listener, executor.submit(_fetch_rules, client, listener["ListenerArn"]))
        for listener in listeners
    ]
    for listener, fut in rule_futures:
        rules = fut.result()
        _enrich_rules_with_target_groups(client, rules, tg_map, executor)
        listener["rules"] = rules

    return listeners


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_load_balancers(
    client: Any,
    on_lb: Callable[[dict], None] | None = None,
) -> Iterator[dict]:
    """
    Yield ELBv2 load balancers one at a time.

    Each yielded dict is the raw describe_load_balancers item enriched with:
      - "listeners": raw describe_listeners items, each with nested:
        - "rules": raw describe_rules items, each with nested:
          - "targetGroups": matching describe_target_groups items, each with:
            - "targetHealth": raw describe_target_health items

    Args:
        client: An ELBv2 boto3 client.
        on_lb: Optional callback invoked with each LB dict as it is yielded.

    Yields:
        dict for each load balancer found.
    """
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY)

    try:
        load_balancers = _paginate(
            client, "describe_load_balancers", {}, "LoadBalancers",
        )
        logger.info("Found %d load balancers", len(load_balancers))

        # Pre-fetch all target groups for the account (one paginated call)
        all_target_groups = _paginate(
            client, "describe_target_groups", {}, "TargetGroups",
        )
        tg_map: dict[str, dict] = {
            tg["TargetGroupArn"]: tg for tg in all_target_groups
        }
        logger.info("Found %d target groups", len(all_target_groups))

        # Pre-fetch tags for all LBs and TGs in batches
        all_arns = (
            [lb["LoadBalancerArn"] for lb in load_balancers]
            + [tg["TargetGroupArn"] for tg in all_target_groups]
        )
        tags_map = _fetch_tags_batched(client, all_arns)
        for tg in all_target_groups:
            tg["tags"] = tags_map.get(tg["TargetGroupArn"], [])

        for lb in load_balancers:
            lb_arn = lb["LoadBalancerArn"]
            logger.info(
                "Fetching details for LB: %s (%s)",
                lb.get("LoadBalancerName", ""), lb_arn,
            )

            lb["listeners"] = _fetch_listeners_with_rules(
                client, lb_arn, tg_map, executor,
            )
            lb["tags"] = tags_map.get(lb_arn, [])

            if on_lb:
                on_lb(lb)
            yield lb
    finally:
        executor.shutdown(wait=False)
