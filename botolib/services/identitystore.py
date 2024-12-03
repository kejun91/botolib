from . import AWSService


class IdentityStore(AWSService):
    __servicename__ = 'identitystore'

    def list_users(self, identity_store_id):
        return self.client.list_users(IdentityStoreId = identity_store_id)
    
    def list_users_with_paginator(self, identity_store_id):
        request_params = {
            "IdentityStoreId": identity_store_id
        }
        return self.get_result_from_paginator('list_users', 'Users', **request_params)