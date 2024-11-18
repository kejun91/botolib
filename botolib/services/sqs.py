from . import AWSService
from ..utils.common import remove_none_values


class SQS(AWSService):
    __servicename__ = 'sqs'
    
    def get_queues(self, next_token = None):
        request_params = remove_none_values({
            'NextToken':next_token
        })
        return self.client.list_queues(**request_params)

    def list_queues_with_paginator(self):
        return self.get_result_from_paginator('list_queues', 'QueueUrls')
    
    def get_queue_attributes(self, queue_url, attribute_names = ['All']):
        response = self.client.get_queue_attributes(QueueUrl=queue_url,AttributeNames=attribute_names)
        return response.get('Attributes')
    
    def get_queue_url(self, queue_name):
        response = self.client.get_queue_url(QueueName=queue_name)
        return response.get('QueueUrl')
    
    def receive_messages(self, queue_url, number_of_messages, message_attribute_names = ['All'], *, callback_handler):
        while number_of_messages > 0:
            max_number_of_messages = min(number_of_messages, 10)

            response = self.client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MessageAttributeNames=message_attribute_names,
                MaxNumberOfMessages=max_number_of_messages
            )

            if 'Messages' in response:
                callback_handler(response['Messages'])

            number_of_messages -= max_number_of_messages

    def send_message(self, queue_url, message_body, message_attributes):
        return self.client.send_message(
            QueueUrl = queue_url,
            MessageBody = message_body,
            MessageAttributes = message_attributes
        )