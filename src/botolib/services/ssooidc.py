from datetime import datetime
import time
from . import AWSService
from botocore.utils import (
    datetime2timestamp
)

_CLIENT_REGISTRATION_TYPE = 'public'
_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:device_code'

class SSO_OIDC(AWSService):
    __servicename__ = 'sso-oidc'

    def register_client(self, client_name = None, client_type = _CLIENT_REGISTRATION_TYPE):
        return self.client.register_client(
            clientName= client_name or ('botocore-client-%s' % int(datetime2timestamp(datetime.now()))),
            clientType=client_type
        )
    
    def start_device_authorization(self, client_id, client_secret, start_url):
        return self.client.start_device_authorization(
            clientId=client_id,
            clientSecret=client_secret,
            startUrl=start_url
        )
    
    def create_token(self, client_id, client_secret, device_code):
        return self.client.create_token(
            grantType= _GRANT_TYPE,
            clientId= client_id,
            clientSecret= client_secret,
            deviceCode= device_code
        )

    def poll_for_token(self, client_id, client_secret, device_code):
        interval = 1

        while True:
            try:
                return self.create_token(client_id, client_secret, device_code)
            except self.client.exceptions.SlowDownException:
                interval += 5
            except self.client.exceptions.AuthorizationPendingException:
                pass
            except self.client.exceptions.ExpiredTokenException as e:
                raise e
            time.sleep(interval)