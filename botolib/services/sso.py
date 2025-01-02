from . import AWSService, paginateable
from ..utils.common import remove_none_values

class SSO(AWSService):
    __servicename__ = 'sso'

    @paginateable("list_accounts", "accountList")
    def list_accounts(self, accessToken, nextToken = None):
        request_params = remove_none_values({
            'accessToken':accessToken,
            'nextToken':nextToken
        })

        return self.client.list_accounts(**request_params)

    @paginateable("list_account_roles", "roleList")
    def list_account_roles(self, accessToken, accountId, nextToken = None):
        request_params = remove_none_values({
            'accessToken':accessToken,
            'accountId':accountId,
            'nextToken':nextToken
        })

        return self.client.list_account_roles(**request_params)
    
    def get_role_credentials(self, role_name, account_id, sso_access_token):
        return self.client.get_role_credentials(
            roleName=role_name,
            accountId=account_id,
            accessToken=sso_access_token
        ).get('roleCredentials')