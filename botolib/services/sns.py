import json
from typing import Union
from . import AWSService
from ..utils.common import remove_none_values


class SNS(AWSService):
    __servicename__ = 'sns'

    def get_topics(self, next_token = None):
        request_params = remove_none_values({
            'NextToken':next_token
        })
        return self.client.list_topics(**request_params)
    
    def list_topics_with_paginator(self):
        return self.get_result_from_paginator('list_topics', 'Topics')
    
    def get_topic_attributes(self, topic_arn):
        response = self.client.get_topic_attributes(TopicArn=topic_arn)
        return response.get('Attributes')
    
    def publish(self, topic_arn, message: Union[dict, str], subject = None, message_attributes = None):
        if isinstance(message, dict):
            message = json.dumps(message)
        elif not isinstance(message, str):
            message = str(message)

        req_params = remove_none_values({
            "TopicArn": topic_arn,
            "Message": message,
            "Subject": subject,
            "MessageAttributes": message_attributes
        })

        return self.client.publish(**req_params)