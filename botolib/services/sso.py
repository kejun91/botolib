from . import AWSService, paginateable

class SSO(AWSService):
    __servicename__ = 'sso'

    @paginateable("list_accounts", "accountList","nextToken", ["nextToken"])
    def list_accounts(self, accessToken, nextToken = None):
        return self.client.list_accounts(**self.get_request_params(locals()))

    @paginateable("list_account_roles", "roleList", "nextToken", ["nextToken"])
    def list_account_roles(self, accessToken, accountId, nextToken = None):
        return self.client.list_account_roles(**self.get_request_params(locals()))
    
    def get_role_credentials(self, role_name, account_id, sso_access_token):
        return self.client.get_role_credentials(
            roleName=role_name,
            accountId=account_id,
            accessToken=sso_access_token
        ).get('roleCredentials')