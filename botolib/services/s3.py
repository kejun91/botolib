from . import AWSService
from ..utils.common import remove_none_values


class S3(AWSService):
    __servicename__ = 's3'
    
    def get_objects_by_bucket_name(self, bucket_name, continuation_token = None):
        request_params = remove_none_values({
            "Bucket":bucket_name,
            'ContinuationToken': continuation_token
        })
        return self.client.list_objects_v2(**request_params)
    
    def list_objects_v2_with_paginator(self, bucket:str):
        return self.get_result_from_paginator('list_objects_v2', 'Contents', Bucket = bucket)

    def download_file(self, bucket_name, key, file_obj):
        self.client.download_fileobj(bucket_name,key,file_obj)

    def get_object(self, s3_path:str) -> bytes:
        parts = s3_path.removeprefix('/').split('/',1)
        bucket_name = parts[0]
        key_name = parts[1] if len(parts) > 1 else ""

        response = self.client.get_object(Bucket=bucket_name, Key=key_name)
        return response["Body"].read()