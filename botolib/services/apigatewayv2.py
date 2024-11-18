from . import AWSService
from ..utils.common import remove_none_values


class APIGatewayV2(AWSService):
    __servicename__ = 'apigatewayv2'

    def get_apis(self, next_token = None):
        request_params = remove_none_values({
            'NextToken': next_token
        })
        return self.client.get_apis(**request_params)
    
    def get_apis_with_paginator(self):
        return self.get_result_from_paginator('get_apis', 'Items')