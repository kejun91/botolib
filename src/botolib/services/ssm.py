from ..utils.common import remove_none_values
from . import AWSService


class SSM(AWSService):
    __servicename__ = 'ssm'

    def get_parameter(self, name, with_decryption = None):
        req_params = remove_none_values({
            "Name":name, 
            "WithDecryption":with_decryption
        })
        
        return self.client.get_parameter(**req_params).get('Parameter').get('Value')
    
    def put_parameter(self, name, value, type, description, overwrite = True):
        req_params = remove_none_values({
            "Name":name,
            "Description":description,
            "Value":value,
            "Type":type,
            "Overwrite":overwrite
        })

        return self.client.put_parameter(**req_params)