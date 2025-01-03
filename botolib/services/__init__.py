from abc import ABC
from functools import wraps
import inspect
from boto3.session import Session
import boto3
from ..utils.common import remove_none_values

_available_services = Session().get_available_services()

class AWSService(ABC):
    __servicename__ = None

    def __init__(self, session:Session = None):
        sn = self.__servicename__

        if sn not in _available_services:
            raise Exception(f"Service {sn} is not available")
        
        self.client = session.client(sn) if session is not None else boto3.client(sn)

    def __getattr__(self, name):
        if hasattr(self.client, name):
            return getattr(self.client, name)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


    def _get_all_with_callback(self, get_list_function, result_token_name, next_token_name, callback_function, *args, next_token = None):
        results = []
        next_token = next_token
        has_next = True
        
        while has_next:
            response = get_list_function(*args, next_token)
            result = response.get(result_token_name)

            if callback_function:
                callback_function(result)
            else:
                if result is not None:
                    results.extend(result)
            next_token = response.get(next_token_name)
            has_next = next_token_name in response

        if not callback_function:
            return results
    
    def _get_all(self, get_list_function, result_token_name, next_token_name, *args):
        return self._get_all_with_callback(get_list_function, result_token_name, next_token_name, None, *args)
    
    def get_request_params(self, locals):
        request_params = remove_none_values({k: v for k, v in locals.items() if k != 'self'})
        return request_params

class CustomPaginationIterator:
    def __init__(self, initial_result, iterator, result_token):
        self._initial_result = initial_result
        self._iterator = iterator
        self._result_token = result_token
    
    def __iter__(self):
        yield self._initial_result.get(self._result_token, [])
        for i in self._iterator:
            yield i.get(self._result_token, [])

class ResultWithPagination:
    def __init__(self, initial_result, client, operation_name, result_token, next_token_key, kwargs):
        self._initial_result = initial_result
        self._paginator = client.get_paginator(operation_name)
        self._result_token = result_token
        self._kwargs = kwargs
        self._pagination_start_token = None if initial_result is None else initial_result.get(next_token_key)

    def __getattr__(self, name):
        return getattr(self._initial_result, name)
    
    def paginate(self, pagination_config = None):
        if self._pagination_start_token is None:
            result_items = self._initial_result.get(self._result_token)
            return [result_items] if result_items is not None else []
    
        kwargs = self._kwargs or {}
        kwargs["PaginationConfig"] =  pagination_config or {
            # 'PageSize': 50
        }
        kwargs["PaginationConfig"]["StartingToken"] = self._pagination_start_token

        return CustomPaginationIterator(self._initial_result, self._paginator.paginate(**remove_none_values(kwargs)), self._result_token)

def paginateable(operation_name, result_token, next_token_key, ignore_arguments = []):
    def decorator(func):
        arg_names = list(inspect.signature(func).parameters.keys())[1:] # ignore the self
        @wraps(func)
        def wrapper(*args, **kwargs):
            args_dict = {}

            for i, arg in enumerate(args[1:]):
                args_dict[arg_names[i]] = arg
            args_dict.update(kwargs)

            for ia in ignore_arguments:
                args_dict.pop(ia, None)
                
            result = func(*args, **kwargs)
            return ResultWithPagination(result, args[0].client, operation_name, result_token, next_token_key, args_dict)
        return wrapper
    return decorator