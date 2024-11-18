from . import AWSService
from ..utils.common import remove_none_values


class ResourceGroupsTaggingAPI(AWSService):
    __servicename__ = 'resourcegroupstaggingapi'

    def get_resources(self, pagination_token = None):
        request_params = remove_none_values({
            "PaginationToken":pagination_token
        })
        return self.client.get_resources(**request_params)
    
    def get_resources_with_paginator(self, callback_func = None):
        return self.get_result_from_paginator('get_resources', 'ResourceTagMappingList', callback_func)