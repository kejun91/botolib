from . import AWSService
from ..utils.common import remove_none_values


class IoT(AWSService):
    __servicename__ = 'iot'

    def get_thing_describe(self, thing_name):
        return self.client.describe_thing(thingName = thing_name)

    def get_principals_by_thing(self, thing_name, next_token = None):
        request_params = remove_none_values({
            'thingName':thing_name,
            'nextToken':next_token
        })
        
        return self.client.list_thing_principals(**request_params)
    
    def list_thing_principals_with_paginator(self, thing_name:str):
        return self.get_result_from_paginator('list_thing_principals', 'principals', thingName=thing_name)
    
    def get_certificate_describe(self, certificate_id):
        return self.client.describe_certificate(certificateId=certificate_id)
    
    def get_logging_options(self):
        return self.client.get_logging_options()
    
    def get_v2_logging_options(self):
        return self.client.get_v2_logging_options()