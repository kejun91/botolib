from datetime import datetime
from typing import Union, overload
from ..utils.chunk import chunk_dict
from ..utils.retry import ExponentialBackoffRetry
from . import AWSService, paginateable
from ..utils.common import remove_none_values
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder

class DynamoDB(AWSService):
    __servicename__ = 'dynamodb'

    def batch_get_item(self, keys_with_tables):
        res = self.client.batch_get_item(
            RequestItems=keys_with_tables
        )

        responses = res.get('Responses', {})

        if 'UnprocessedKeys' in res:
            r = self.batch_get_item(res['UnprocessedKeys'])
            for k,v in r.items():
                if k in responses:
                    responses[k].extend(v)
                else:
                    responses[k] = v

        if 'Responses' in res:
            for k,v in responses.items():
                responses[k] = dynamodb_type_to_python_type(v)

        return responses
    
    def get_item(self, table_name:str, primary_key:dict):
        return dynamodb_type_to_python_type(self.client.get_item(
            TableName=table_name,
            Key=python_type_to_dynamodb_type(primary_key)
        ).get('Item'))
    
    def paginated_query(self, table_name, index_name, key_condition_expression:ConditionBase, scan_index_forward:bool = None, select = None, filter_expression:ConditionBase = None, selected_attributes = None):
        # TableName:str, 
        # IndexName:str = None, 
        # Select:str = None, 
        # ExclusiveStartKey:dict = None, 
        # ReturnConsumedCapacity:str = None, 
        # ScanIndexForward:bool = None, 
        # KeyConditionExpression:str = None, 
        # ProjectionExpression:str = None, 
        # FilterExpression:str = None, 
        # ExpressionAttributeNames:dict = None, 
        # ExpressionAttributeValues:dict = None, 
        # ConsistentRead:bool = None, 
        # Limit = None
        kwargs = _generate_query_or_scan_kwargs(table_name, index_name, None, select, None, key_condition_expression, scan_index_forward, filter_expression, selected_attributes)
        return ScanAndQueryPaginator(self.client, 'query', **kwargs)
    
    def paginated_scan(self, table_name, index_name = None, filter_expression:ConditionBase = None, select = None, selected_attributes = None):
        # TableName:str, 
        # IndexName:str = None, 
        # Select:str = None, 
        # ExclusiveStartKey:dict = None, 
        # ReturnConsumedCapacity:str = None, 
        # TotalSegments:int = None, 
        # Segment:int = None, 
        # ProjectionExpression:str = None, 
        # FilterExpression:str = None, 
        # ExpressionAttributeNames:dict = None, 
        # ExpressionAttributeValues:dict = None, 
        # ConsistentRead:bool = None, 
        # Limit = None
        kwargs = _generate_query_or_scan_kwargs(table_name, index_name, None, select, None, None, None, filter_expression, selected_attributes)
        return ScanAndQueryPaginator(self.client, 'scan', **kwargs)
    
    def execute_partiql_with_custom_paginator(self, partiql_statement, callback_handler = None, next_token = None):
        return self._get_all_with_callback(self.execute_partiql, 'Items', 'NextToken', callback_handler, partiql_statement, next_token = next_token)
    
    def execute_partiql(self, query_statement, next_token = None):
        request_params = {
            "Statement": query_statement,
            "NextToken": next_token
        }
        result = self.client.execute_statement(**remove_none_values(request_params))
        if 'Items' in result:
            result['Items'] = dynamodb_type_to_python_type(result['Items'])
        return result
    
    def describe_table(self, table_name):
        res = self.client.describe_table(TableName=table_name)
        return res.get('Table')
    
    @paginateable('list_tables', 'TableNames', 'LastEvaluatedTableName', ["ExclusiveStartTableName", "Limit"])
    def list_tables(self, ExclusiveStartTableName = None, Limit = None):
        return self.client.list_tables(**self.get_request_params(locals()))
    
    def put_item(self, table_name, item):
        return self.client.put_item(
            TableName = table_name,
            Item = python_type_to_dynamodb_type(item)
        )
    
    def delete_item(self, table_name, key:dict):
        self.client.delete_item(
            TableName = table_name,
            Key = python_type_to_dynamodb_type(key)
        )

    def batch_write_item(self, put_items:dict = None, delete_keys:dict = None):
        requested_items = {}
        if put_items is not None:
            for tn, items in put_items.items():
                if tn not in requested_items:
                    requested_items[tn] = []
                
                for i in items:
                    requested_items[tn].append({
                        "PutRequest": {
                            "Item": python_type_to_dynamodb_type(i)
                        }
                    })
        
        if delete_keys is not None:
            for tn, keys in delete_keys.items():
                if tn not in requested_items:
                    requested_items[tn] = []

                for k in keys:
                    requested_items[tn].append({
                        "DeleteRequest": {
                            "Key": python_type_to_dynamodb_type(k)
                        }
                    })

        if requested_items != {}:
            #TODO: check bytes of total items for chunk
            for chunk in chunk_dict(requested_items, 25):
                self._batch_write_item_with_retry(chunk)

    def _batch_write_item_with_retry(self, request_items):
        condition_check = lambda req_items: req_items is not None
        execution_func = lambda req_items: self.client.batch_write_item(RequestItems=req_items).get('UnprocessedItems')
        ExponentialBackoffRetry().execute(condition_check, execution_func, request_items)

    def batch_delete_items(self, table_name, keys):
        self.batch_write_item(None, {table_name:keys})

    def batch_put_items(self, table_name, items):
        self.batch_write_item({table_name:items}, None)

    def update_item_with_condition(self, table_name, key, condition_expression:ConditionBase, set_attribute_values:dict, remove_attributes:list = None):

        '''
        Parameters not exposed:
        ReturnConsumedCapacity
        ReturnItemCollectionMetrics
        ReturnValuesOnConditionCheckFailure
        '''

        update_expression, names, values = get_update_expression_attributes(python_type_to_dynamodb_type(set_attribute_values), remove_attributes)

        if condition_expression is not None:
            ce_builder = ConditionExpressionBuilder()
            result = ce_builder.build_expression(condition_expression, False)
            condition_expression = result.condition_expression
            names.update(result.attribute_name_placeholders)
            values.update(python_type_to_dynamodb_type(result.attribute_value_placeholders))
        
        request_params = remove_none_values({
            "TableName": table_name,
            "Key": python_type_to_dynamodb_type(key),
            "UpdateExpression": update_expression,
            "ConditionExpression": condition_expression,
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
            "ReturnValues":"UPDATED_NEW"
        })

        return self.client.update_item(**request_params).get("Attributes")
    
    def update_item(self, table_name, key, set_attribute_values:dict, remove_attributes:list = None):
        return self.update_item_with_condition(table_name, key, None, set_attribute_values, remove_attributes)

def get_update_expression_attributes(update_attribute_values, remove_attributes, set_expression_callback = lambda name, value: f"{name} = {value}"):
    update_expression = ''
    i = 0
    expression_attribute_names = {}
    expression_attribute_values = {}
    expressions = []
    if update_attribute_values is not None:
        for n, v in update_attribute_values.items():
            n_alias = f'#name{i}'
            v_alias = f':value{i}'
            i = i + 1
            expressions.append(set_expression_callback(n_alias,v_alias))
            expression_attribute_names.update({n_alias:n})
            expression_attribute_values.update({v_alias:v})

        if len(expressions) > 0:
            update_expression += "SET " + ", ".join(expressions)

    remove_aliases = []
    if remove_attributes is not None:
        for r in remove_attributes:
            n_alias = f'#name{i}'
            remove_aliases.append(n_alias)
            i = i + 1
            expression_attribute_names.update({n_alias:r})

        if len(remove_aliases) > 0:
            update_expression += "REMOVE " + ", ".join(remove_aliases)
    
    return update_expression, expression_attribute_names, expression_attribute_values
    
def _generate_query_or_scan_kwargs(table_name, index_name, exclusive_start_key, select, limit, key_condition_expression, scan_index_forward, filter_expression, selected_attributes):
    kwargs = {
        "TableName":table_name,
        "IndexName":index_name,
        "ExclusiveStartKey":exclusive_start_key,
        "Select": select,
        "Limit":limit
    }

    if scan_index_forward is not None:
        kwargs["ScanIndexForward"] = scan_index_forward

    ce_builder = ConditionExpressionBuilder()
    expr_attr_names = {}
    expr_attr_values = {}

    if key_condition_expression is not None:
        key_expr_result = ce_builder.build_expression(key_condition_expression, True)
        kwargs['KeyConditionExpression'] = key_expr_result.condition_expression
        expr_attr_names.update(key_expr_result.attribute_name_placeholders)
        expr_attr_values.update(python_type_to_dynamodb_type(key_expr_result.attribute_value_placeholders))

    if filter_expression is not None:
        expr_result = ce_builder.build_expression(filter_expression, False)
        kwargs['FilterExpression'] = expr_result.condition_expression
        expr_attr_names.update(expr_result.attribute_name_placeholders)
        expr_attr_values.update(python_type_to_dynamodb_type(expr_result.attribute_value_placeholders))

    if selected_attributes is not None:
        pe = []
        for i, value in enumerate(selected_attributes):
            a = f'#PE{i}'
            expr_attr_names[a] = value
            pe.append(a)
        kwargs['ProjectionExpression'] = ','.join(pe)
    
    if len(expr_attr_names) > 0:
        kwargs['ExpressionAttributeNames'] = expr_attr_names
    
    if len(expr_attr_values) > 0:
        kwargs['ExpressionAttributeValues'] = expr_attr_values

    return remove_none_values(kwargs)

@overload
def python_type_to_dynamodb_type(item:dict) -> dict: ...

@overload
def python_type_to_dynamodb_type(items:list) -> list: ...

def python_type_to_dynamodb_type(arg:Union[dict, list]) -> Union[dict, list]:
    if arg is None:
        return arg
    elif isinstance(arg, dict):
        dynamodb_item = {}
        serializer = TypeSerializer()
        for k,v in arg.items():
            if isinstance(v, datetime):
                v = v.timestamp()
            if isinstance(v, float):
                v = Decimal(str(v))
            dynamodb_item[k] = serializer.serialize(v)
        return dynamodb_item
    elif isinstance(arg, list):
        return [python_type_to_dynamodb_type(item) for item in arg]
    else:
        raise Exception('arg must be dict or list')
    
@overload
def dynamodb_type_to_python_type(item:dict) -> dict: ...

@overload
def dynamodb_type_to_python_type(items:list) -> list: ...

def dynamodb_type_to_python_type(arg:Union[dict,list]) -> Union[dict,list]:
    if arg is None:
        return arg
    elif isinstance(arg, dict):
        deserializer = TypeDeserializer()
        python_item = {}
        deserializer = TypeDeserializer()
        for k,v in arg.items():
            python_item[k] = deserializer.deserialize(v)
        return python_item
    elif isinstance(arg, list):
        return [dynamodb_type_to_python_type(item) for item in arg]
    else:
        raise Exception('arg must be dict or list')
    
class ScanAndQueryPaginator:
    def __init__(self, client, operation_name, **kwargs):
        self._iterable = client.get_paginator(operation_name).paginate(**kwargs)
    
    def __iter__(self):
        for i in self._iterable:
            yield dynamodb_type_to_python_type(i.get("Items", []))