from . import AWSService, paginateable


class CloudWatch(AWSService):
    __servicename__ = 'cloudwatch'

    @paginateable("list_metrics", "Metrics", "NextToken", ["NextToken"])
    def list_metrics(self, Namespace:str = None, MetricName:str = None, Dimensions:list = None, RecentlyActive:str = None, IncludeLinkedAccounts:bool = None, OwningAccount:str = None, NextToken = None):
        return self.client.list_metrics(**self.get_request_params(locals()))
