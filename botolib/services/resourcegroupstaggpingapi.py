from . import AWSService, paginateable


class ResourceGroupsTaggingAPI(AWSService):
    __servicename__ = 'resourcegroupstaggingapi'

    @paginateable("get_resources", "ResourceTagMappingList", "PaginationToken", ["PaginationToken", "ResourcesPerPage"])
    def get_resources(self, PaginationToken = None, TagFilters:list = None, ResourcesPerPage:int = None, TagsPerPage:int = None, ResourceTypeFilters:list = None, IncludeComplianceDetails:bool = None, ExcludeCompliantResources:bool = None, ResourceARNList:list = None):
        return self.client.get_resources(**self.get_request_params(locals()))
