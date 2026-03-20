import json
from typing import Dict, Union
from . import AWSService, paginateable
from ..utils.common import remove_none_values


class SNS(AWSService):
    __servicename__ = 'sns'

    @paginateable("list_topics", "Topics", "NextToken", ["NextToken"])
    def list_topics(self, NextToken = None):
        return self.client.list_topics(**self.get_request_params(locals()))

    def get_topic_attributes(self, topic_arn):
        response = self.client.get_topic_attributes(TopicArn=topic_arn)
        return response.get('Attributes')
    
    def publish(self, topic_arn, message: Union[dict, str], subject = None, message_attributes:Dict[str, Union[str, bytes]] = None):
        if isinstance(message, dict):
            message = json.dumps(message)
        elif not isinstance(message, str):
            message = str(message)

        msg_attrs = {}
        if message_attributes is not None:
            for k,v in message_attributes.items():
                if isinstance(v, str):
                    msg_attrs[k] = {"DataType": "String", "StringValue": v}
                elif isinstance(v, str):
                    msg_attrs[k] = {"DataType": "Binary", "BinaryValue": v}
                else:
                    msg_attrs[k] = {"DataType": "String", "StringValue": str(v)}

        req_params = remove_none_values({
            "TopicArn": topic_arn,
            "Message": message,
            "Subject": subject,
            "MessageAttributes": msg_attrs
        })

        return self.client.publish(**req_params)