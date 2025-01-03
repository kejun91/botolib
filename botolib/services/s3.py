from typing import Union
from . import AWSService, paginateable


class S3(AWSService):
    __servicename__ = 's3'

    @paginateable("list_objects_v2", "Contents", "ContinuationToken", ["ContinuationToken", "MaxKeys"])
    def list_objects_v2(self, Bucket, Delimiter:str = None, EncodingType:str = None, MaxKeys:int = None, Prefix:str = None, ContinuationToken:str = None, FetchOwner:bool = None, StartAfter:str = None, RequestPayer:str = None, ExpectedBucketOwner:str = None, OptionalObjectAttributes:list = None):
        return self.client.list_objects_v2(**self.get_request_params(locals()))

    def get_object(self, s3_path:str) -> bytes:
        bucket_name, key_name = get_bucket_and_key(s3_path)
        response = self.client.get_object(Bucket=bucket_name, Key=key_name)
        return response["Body"].read()
    
    def put_object(self, s3_path:str, body:Union[bytes,str]):
        bucket_name, key_name = get_bucket_and_key(s3_path)
        if isinstance(body,str):
            body = body.encode('utf-8')
        self.client.put_object(Bucket=bucket_name, Key=key_name, Body=body)

def get_bucket_and_key(s3_path:str):
    parts = s3_path.removeprefix('/').split('/',1)
    bucket_name = parts[0]
    key_name = parts[1] if len(parts) > 1 else ""

    return bucket_name, key_name