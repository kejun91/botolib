from . import AWSService


class SecretsManager(AWSService):
    __servicename__ = 'secretsmanager'

    def get_secret_value(self, secret_id):
        return self.client.get_secret_value(SecretId=secret_id)