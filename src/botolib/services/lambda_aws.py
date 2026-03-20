from . import AWSService, paginateable
# As "lambda" is a reserved word in python, the file name changed to "lambda_aws"

class Lambda(AWSService):
    __servicename__ = 'lambda'

    @paginateable("list_functions", "Functions", "NextMarker", ["Marker", "MaxItems"])
    def list_functions(self, MasterRegion = None, FunctionVersion = None, Marker = None, MaxItems:int = None):
        return self.client.list_functions(**self.get_request_params(locals()))
    
    @paginateable("list_event_source_mappings", "EventSourceMappings", "NextMarker", ["Marker", "MaxItems"])
    def list_event_source_mappings(self, EventSourceArn:str = None, FunctionName:str = None, Marker = None, MaxItems:int = None):
        return self.client.list_event_source_mappings(**self.get_request_params(locals()))

    def invoke(self, function_name:str, payload):
        return self.client.invoke(FunctionName=function_name, Payload=payload)