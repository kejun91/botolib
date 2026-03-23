from .resources.fetchers.apigateway import fetch_rest_apis, fetch_custom_domains as fetch_v1_custom_domains
from .resources.fetchers.apigatewayv2 import fetch_http_apis, fetch_custom_domains as fetch_v2_custom_domains

from .resources.fetchers.apigateway import (
    fetch_custom_domains as fetch_v1_custom_domains,
)
from .resources.fetchers.apigateway import (
    fetch_rest_apis as fetch_rest_apis,
)
from .resources.fetchers.apigatewayv2 import (
    fetch_custom_domains as fetch_v2_custom_domains,
)
from .resources.fetchers.apigatewayv2 import (
    fetch_http_apis as fetch_http_apis,
)

__all__ = [
    "fetch_http_apis",
    "fetch_rest_apis",
    "fetch_v1_custom_domains",
    "fetch_v2_custom_domains",
]
