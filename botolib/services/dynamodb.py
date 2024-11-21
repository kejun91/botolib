from typing import Union, overload
from . import AWSService
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
    
    def query(self, table_name, index_name, key_condition_expression:ConditionBase, select = None, limit = None, exclusive_start_key = None, filter_expression:ConditionBase = None, selected_attributes = None):
        kwargs = _generate_query_or_scan_kwargs(table_name, index_name, exclusive_start_key, select, limit, key_condition_expression, filter_expression, selected_attributes)
        result = self.client.query(**kwargs)

        if 'Items' in result:
            result['Items'] = dynamodb_type_to_python_type(result['Items'])
        return result
    
    def scan(self, table_name, index_name = None, filter_expression:ConditionBase = None, exclusive_start_key = None, limit:int = None, select = None, selected_attributes = None):
        kwargs = _generate_query_or_scan_kwargs(table_name, index_name, exclusive_start_key, select, limit, None, filter_expression, selected_attributes)
        result = self.client.scan(**kwargs)

        if 'Items' in result:
            result['Items'] = dynamodb_type_to_python_type(result['Items'])
        return result
    
    def scan_with_paginator(self, table_name, index_name = None, filter_expression:ConditionBase = None, select = None, selected_attributes = None, callback_handler = None):
        kwargs = _generate_query_or_scan_kwargs(table_name, index_name, None, select, None, None, filter_expression, selected_attributes)

        if callback_handler is not None:
            def convert_and_callback(items):
                callback_handler(dynamodb_type_to_python_type(items))

            return self.get_result_from_paginator('scan', 'Items', convert_and_callback, **kwargs)
        else:
            return dynamodb_type_to_python_type(self.get_result_from_paginator('scan', 'Items', None, **kwargs))
    
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
    
    def list_tables(self, last_evaluated_table = None):
        request_params = remove_none_values({
            'ExclusiveStartTableName':last_evaluated_table
        })
        return self.client.list_tables(**request_params)
    
    def describe_table(self, table_name):
        res = self.client.describe_table(TableName=table_name)
        return res.get('Table')
    
    def list_tables_with_paginator(self):
        '''
        TableNames, LastEvaluatedTableName, ExclusiveStartTableName
        '''
        return self.get_result_from_paginator('list_tables', 'TableNames')
    
    def put_item(self, table_name, item):
        return self.client.put_item(
            TableName = table_name,
            Item = python_type_to_dynamodb_type(item)
        )
    
    def update_item(self, table_name, key, update_attribute_values):
        update_expressions, names, values = get_update_expression_attributes(python_type_to_dynamodb_type(update_attribute_values))

        return self.client.update_item(
            TableName=table_name,
            Key=python_type_to_dynamodb_type(key),
            UpdateExpression = "SET " + ", ".join(update_expressions),
            ExpressionAttributeNames = names,
            ExpressionAttributeValues = values,
            ReturnValues="UPDATED_NEW"
        ).get("Attributes")

def get_update_expression_attributes(attribute_values, expression_callback = lambda name, value: f"{name} = {value}"):
    i = 0
    expression_attribute_names = {}
    expression_attribute_values = {}
    expressions = []
    for n, v in attribute_values.items():
        n_alias = f'#name{i}'
        v_alias = f':value{i}'
        i = i + 1
        expressions.append(expression_callback(n_alias,v_alias))
        expression_attribute_names.update({n_alias:n})
        expression_attribute_values.update({v_alias:v})
    
    return expressions, expression_attribute_names, expression_attribute_values
    
def _generate_query_or_scan_kwargs(table_name, index_name, exclusive_start_key, select, limit, key_condition_expression, filter_expression, selected_attributes):
    kwargs = {
        "TableName":table_name,
        "IndexName":index_name,
        "ExclusiveStartKey":exclusive_start_key,
        "Select": select,
        "Limit":limit
    }

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