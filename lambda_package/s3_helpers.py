import boto3
import json


def _is_single_object(json_object):
    return json_object[0] == u'{'


def _connect_s3(*args, **kwargs):
    session = boto3.Session(*args, **kwargs)
    resource = session.resource('s3')
    return resource


def _json_from_s3(s3_bucket, s3_path, aws_options):
    client = _connect_s3(**aws_options)

    content_object = client.Object(s3_bucket, s3_path)

    file_content = content_object.get()
    decoded_body = file_content['Body'].read().decode('utf-8')

    return decoded_body


def read_json(s3_bucket, s3_path, aws_options):
    lines_from_s3 = _json_from_s3(s3_bucket, s3_path, aws_options)
    return [json.loads(l) for l in lines_from_s3.splitlines()]

