from . import AWSService
from ..utils.common import remove_none_values

class SSO(AWSService):
    __servicename__ = 'sso'

    def list_accounts(self, access_token, next_token = None):
        request_params = remove_none_values({
            'accessToken':access_token,
            'nextToken':next_token
        })

        return self.client.list_accounts(**request_params)
    
    def list_accounts_with_paginator(self, access_token):
        return self.get_result_from_paginator('list_accounts','accountList', accessToken=access_token)

    def list_account_roles(self, access_token, account_id, next_token = None):
        request_params = remove_none_values({
            'accessToken':access_token,
            'accountId':account_id,
            'nextToken':next_token
        })

        return self.client.list_account_roles(**request_params)
    
    def list_account_roles_with_paginator(self, access_token, account_id):
        return self.get_result_from_paginator('list_account_roles', 'roleList', accessToken=access_token, accountId=account_id)