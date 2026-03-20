from . import AWSService


class IAM(AWSService):
    __servicename__ = 'iam'

    def list_account_aliases(self):
        return self.client.list_account_aliases().get('AccountAliases')[0]