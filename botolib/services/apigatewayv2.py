from . import AWSService, paginateable


class APIGatewayV2(AWSService):
    __servicename__ = 'apigatewayv2'

    @paginateable('get_apis', 'Items', 'NextToken', ['NextToken', "MaxResults"])
    def get_apis(self, NextToken = None, MaxResults = None):
        return self.client.get_apis(**self.get_request_params(locals()))
