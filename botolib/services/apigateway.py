from . import AWSService
from ..utils.common import remove_none_values

class APIGateway(AWSService):
    __servicename__ = 'apigateway'

    def get_apis(self, position = None):
        request_params = remove_none_values({
            'position':position
        })
        return self.client.get_rest_apis(**request_params)
    
    def get_rest_apis_with_paginator(self):
        return self.get_result_from_paginator('get_rest_apis', 'items')
    
    def get_api_resources(self, rest_api_id, position = None):
        request_params = remove_none_values({
            'position':position,
            'restApiId':rest_api_id,
            'embed':["methods"]
        })
        return self.client.get_resources(**request_params)
    
    def get_resources_with_paginator(self, rest_api_id):
        return self.get_result_from_paginator('get_resources', 'items', restApiId=rest_api_id)

    def get_domain_names(self, position = None, limit = 500):
        request_params = remove_none_values({
            'position':position,
            'limit':limit
        })
        return self.client.get_domain_names(**request_params)
    
    def get_domain_names_with_paginator(self):
        return self.get_result_from_paginator('get_domain_names', 'items')
    
    def get_base_path_mappings_by_domain_name(self, domain_name, position = None, limit = 500):
        request_params = remove_none_values({
            'domainName':domain_name,
            'position':position,
            'limit':limit
        })
        return self.client.get_base_path_mappings(**request_params)
    
    def get_base_path_mappings_with_paginator(self, domain_name):
        return self.get_result_from_paginator('get_base_path_mappings', 'items', domainName=domain_name)

    def get_integration_by_resource_id(self, rest_api_id, resource_id, http_method):
        request_params = remove_none_values({
            'restApiId':rest_api_id,
            'resourceId':resource_id,
            'httpMethod':http_method
        })
        return self.client.get_integration(**request_params)
    
    def get_method_by_resource_id(self, rest_api_id, resource_id, http_method):
        request_params = remove_none_values({
            'restApiId':rest_api_id,
            'resourceId':resource_id,
            'httpMethod':http_method
        })
        return self.client.get_method(**request_params)
