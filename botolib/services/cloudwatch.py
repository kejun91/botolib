from . import AWSService
from ..utils.common import remove_none_values


class CloudWatch(AWSService):
    __servicename__ = 'cloudwatch'

    def get_metrics(self, next_token = None):
        request_params = remove_none_values({
            'NextToken': next_token
        })
        return self.client.list_metrics(**request_params)
    
    def list_metrics_with_paginator(self):
        return self.get_result_from_paginator('list_metrics', 'Metrics')