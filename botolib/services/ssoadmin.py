from botolib.services import AWSService


class SSOAdmin(AWSService):
    __servicename__ = 'sso-admin'

    def list_instances(self):
        return self.client.list_instances().get('Instances')