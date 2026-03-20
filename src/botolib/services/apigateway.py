from . import AWSService, paginateable

class APIGateway(AWSService):
    __servicename__ = 'apigateway'

    @paginateable('get_rest_apis','items','position', ['position','limit'])
    def get_rest_apis(self, position = None, limit = None):
        return self.client.get_rest_apis(**self.get_request_params(locals()))
    
    @paginateable("get_resources", "items", "position", ["position", "limit"])
    def get_resources(self, restApiId, embed = ["methods"], position = None, limit = None):
        return self.client.get_resources(**self.get_request_params(locals()))
    
    @paginateable("get_domain_names", "items", "position", ["position", "limit"])
    def get_domain_names(self, resourceOwner = None, position = None, limit = None):
        return self.client.get_domain_names(**self.get_request_params(locals()))
    
    @paginateable("get_base_path_mappings", "items", "position", ["position", "limit"])
    def get_base_path_mappings(self, domainName, domainNameId = None, position = None, limit = None):
        return self.client.get_base_path_mappings(**self.get_request_params(locals()))
    
    def get_integration(self, restApiId:str, resourceId:str, httpMethod:str):
        return self.client.get_integration(**self.get_request_params(locals()))
    
    def get_method(self, restApiId:str, resourceId:str, httpMethod:str):
        return self.client.get_method(**self.get_request_params(locals()))