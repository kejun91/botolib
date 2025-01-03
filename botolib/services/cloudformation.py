from . import AWSService, paginateable


class CloudFormation(AWSService):
    __servicename__ = 'cloudformation'

    @paginateable("list_stacks", "StackSummaries", "NextToken", ["NextToken"])
    def list_stacks(self, NextToken = None):
        return self.client.list_stacks(**self.get_request_params(locals()))
    
    @paginateable("list_stack_resources", "StackResourceSummaries", "NextToken", ["NextToken"])
    def list_stack_resources(self, StackName, NextToken = None):
        return self.client.list_stack_resources(**self.get_request_params(locals()))
