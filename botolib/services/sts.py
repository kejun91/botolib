from . import AWSService

class STS(AWSService):
    __servicename__ = 'sts'
    
    def assume_role(self, role_arn, role_session_name):
        return self.client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name
        )