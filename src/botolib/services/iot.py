from . import AWSService, paginateable
from ..utils.common import remove_none_values

class IoT(AWSService):
    __servicename__ = 'iot'

    def describe_thing(self, thingName):
        return self.client.describe_thing(thingName = thingName)
    
    @paginateable("list_thing_principals", "principals", "nextToken", ["nextToken", "maxResults"])
    def list_thing_principals(self, thingName, nextToken = None, maxResults = None):
        return self.client.list_thing_principals(**self.get_request_params(locals()))

    def describe_certificate(self, certificateId):
        return self.client.describe_certificate(certificateId = certificateId)
    
    #TODO: remove
    def get_principals_by_thing(self, thing_name, next_token = None):
        request_params = remove_none_values({
            'thingName':thing_name,
            'nextToken':next_token
        })
        
        return self.client.list_thing_principals(**request_params)
    
    #TODO: remove
    def get_certificate_describe(self, certificate_id):
        return self.client.describe_certificate(certificateId=certificate_id)
