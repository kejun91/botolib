from . import AWSService
from ..utils.common import remove_none_values


class EventBridge(AWSService):
    __servicename__ = 'events'

    def get_rules(self, next_token = None):
        request_params = remove_none_values({
            'NextToken':next_token
        })
        return self.client.list_rules(**request_params)

    def list_rules_with_paginator(self):
        return self.get_result_from_paginator('list_rules', 'Rules')

    def get_targets_by_rule(self, rule, next_token = None):
        request_params = remove_none_values({
            'Rule':rule,
            'NextToken':next_token
        })
        return self.client.list_targets_by_rule(**request_params)

    def list_targets_by_rule_with_paginator(self, rule):
        return self.get_result_from_paginator('list_targets_by_rule', 'Targets', Rule=rule)
    
    def get_rule_describe(self, rule):
        pass

    def get_event_buses(self):
        pass

    def get_event_sources(self):
        pass