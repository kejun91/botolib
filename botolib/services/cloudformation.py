from . import AWSService
from ..utils.common import remove_none_values


class CloudFormation(AWSService):
    __servicename__ = 'cloudformation'

    def get_stacks(self, next_token = None):
        request_params = remove_none_values({
            'NextToken':next_token
        })
        return self.client.list_stacks(**request_params)
    
    def list_stacks_with_paginator(self):
        return self.get_result_from_paginator('list_stacks', 'StackSummaries')
    
    def get_stack_resources_by_stack_name(self, stack_name, next_token = None):
        request_params = remove_none_values({
            'StackName':stack_name,
            'NextToken':next_token
        })

        return self.client.list_stack_resources(**request_params)
    
    def list_stack_resources_with_paginator(self, stack_name):
        return self.get_result_from_paginator('list_stack_resources','StackResourceSummaries', StackName=stack_name)