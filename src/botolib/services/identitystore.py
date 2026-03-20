from . import AWSService, paginateable


class IdentityStore(AWSService):
    __servicename__ = 'identitystore'

    @paginateable('list_users', 'Users',"NextToken",["NextToken", "MaxResults"])
    def list_users(self, IdentityStoreId, Filters:list = None, MaxResults = None, NextToken = None):
        return self.client.list_users(**self.get_request_params(locals()))
