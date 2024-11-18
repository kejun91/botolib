from . import AWSService
from ..utils.common import remove_none_values
# As "lambda" is a reserved word in python, the file name changed to "lambda_aws"

class Lambda(AWSService):
    __servicename__ = 'lambda'
    
    def get_functions(self, marker = None):
        request_params = remove_none_values({
            'Marker':marker
        })

        return self.client.list_functions(**request_params)

    def list_functions_with_paginator(self):
        return self.get_result_from_paginator('list_functions','Functions')
    
    def get_event_source_mappings(self, marker = None):
        request_params = remove_none_values({
            'Marker':marker
        })
        return self.client.list_event_source_mappings(**request_params)
    
    def list_event_source_mappings_with_paginator(self):
        return self.get_result_from_paginator('list_event_source_mappings', 'EventSourceMappings')