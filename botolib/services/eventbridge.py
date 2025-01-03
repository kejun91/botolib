from . import AWSService, paginateable


class EventBridge(AWSService):
    __servicename__ = 'events'

    @paginateable("list_rules", "Rules", "NextToken", ["NextToken", "Limit"])
    def list_rules(self, NamePrefix = None, EventBusName = None, NextToken = None, Limit = None):
        return self.client.list_rules(**self.get_request_params(locals()))
    
    @paginateable("list_targets_by_rule", "Targets", "NextToken", ["NextToken", "Limit"])
    def list_targets_by_rule(self, Rule, EventBusName = None, NextToken = None, Limit = None):
        return self.client.list_targets_by_rule(**self.get_request_params(locals()))

