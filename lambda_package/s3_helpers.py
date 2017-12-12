import boto3
import json


def _is_single_object(json_object):
    return json_object[0] == u'{'


def _connect_s3(*args, **kwargs):
    client = boto3.resource('s3')
    return client


def _json_from_s3(s3_bucket, s3_path):
    client = _connect_s3()

    content_object = client.Object(s3_bucket, s3_path)

    file_content = content_object.get()
    decoded_body = file_content['Body'].read().decode('utf-8')

    return decoded_body


def read_json(s3_bucket, s3_path):
    lines_from_s3 = _json_from_s3(s3_bucket, s3_path)
    return [json.loads(l) for l in lines_from_s3.splitlines()]

