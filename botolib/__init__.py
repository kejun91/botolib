from .services.apigateway import APIGateway
from .services.apigatewayv2 import APIGatewayV2
from .services.cloudformation import CloudFormation
from .services.cloudwatch import CloudWatch
from .services.cloudwatchlogs import CloudWatchLogs
from .services.dynamodb import DynamoDB, dynamodb_type_to_python_type, python_type_to_dynamodb_type
from .services.eventbridge import EventBridge
from .services.iot import IoT
from .services.iam import IAM
from .services.identitystore import IdentityStore
from .services.lambda_aws import Lambda
from .services.resourcegroupstaggpingapi import ResourceGroupsTaggingAPI
from .services.s3 import S3
from .services.secretsmanager import SecretsManager
from .services.sns import SNS
from .services.ssm import SSM
from .services.sqs import SQS
from .services.sso import SSO
from .services.ssoadmin import SSOAdmin
from .services.ssooidc import SSO_OIDC
from .services.sts import STS