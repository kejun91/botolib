from . import AWSService

class STS(AWSService):
    __servicename__ = 'sts'

    def get_caller_identity(self):
        return self.client.get_caller_identity()
    
    def assume_role(self, role_arn, role_session_name):
        return self.client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name
        )