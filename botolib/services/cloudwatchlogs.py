import time
from typing import Union
from . import AWSService
from ..utils.common import remove_none_values
from ..utils.logger import get_logger

logger = get_logger('CloudWatchLogs')

class CloudWatchLogs(AWSService):
    __servicename__ = 'logs'

    def get_log_groups(self, next_token = None):
        request_params = remove_none_values({
            'nextToken':next_token
        })
        return self.client.describe_log_groups(**request_params)
    
    def describe_log_groups_with_paginator(self):
        return self.get_result_from_paginator('describe_log_groups', 'logGroups')
    
    def query(self, log_group_names:Union[str, list], query_string, start_time, end_time):
        results, _ = self.query_with_statistics(log_group_names, query_string, start_time, end_time)
        return results
    
    def query_with_statistics(self, log_group_names:Union[str, list], query_string:str, start_time, end_time):
        if isinstance(log_group_names,str):
            log_group_names = [log_group_names]

        query_id = self.start_query(log_group_names, query_string, start_time, end_time)

        results = None # check 
        while True:
            status, results, statistics = self.get_query_results(query_id)
            if status == 'Complete':
                break
            elif status in ['Failed','Cancelled','Timeout','Unknown']:
                logger.error(f'Query execution aborted with status: {status}') 
                break

            time.sleep(1)
        
        returned_values = []
        if results is not None:
            for r in results:
                converted_result = {}
                for f in r:
                    converted_result[f['field']] = f['value']
                returned_values.append(converted_result)
        return returned_values, statistics
    
    def start_query(self, log_group_names, query_string, start_time, end_time):
        response = self.client.start_query(
            logGroupNames = log_group_names,
            queryString = query_string,
            startTime = int(start_time.timestamp() * 1000),
            endTime = int(end_time.timestamp() * 1000)
        )

        return response['queryId']
    
    def get_query_results(self, query_id):
        query_result = self.client.get_query_results(
            queryId = query_id
        )

        return query_result.get('status'), query_result.get('results'), query_result.get('statistics')
    
    def get_log_record(self, log_record_pointer):
        return self.client.get_log_record(logRecordPointer = log_record_pointer)
    
    def start_live_tail(self, log_group_identifiers, callback_func, log_event_filter_pattern = None):
        request_params = remove_none_values({
            'logGroupIdentifiers':log_group_identifiers,
            'logEventFilterPattern':log_event_filter_pattern
        })
        response = self.client.start_live_tail(request_params)

        event_streams = response['responseStream']
        for stream in event_streams:
            callback_func(stream)

    def get_log_group_fields(self, log_group_name):
        return self.client.get_log_group_fields(logGroupName = log_group_name)