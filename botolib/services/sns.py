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