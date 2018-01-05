from __future__ import absolute_import

import json
import boto3

from .invocation_types import InvocationType


class CalculatorClient(object):

    def __init__(self, aws_info=None):
        session = boto3.Session(**aws_info)
        self._resource = session.client('lambda')

    def _get_one_breakdown(self, uid, pids=[], month='01'):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'detailed',
                'uid': uid,
                'month': month,
            }
        }

        if pids:
            request['queryStringParameters']['pids'] = pids

        encoded_payload = bytes(json.dumps(request)).encode('utf-8')
        response = self._resource.invoke(
            FunctionName='ma_calculator',
            InvocationType=InvocationType.RequestResponse,
            LogType='None',
            Payload=encoded_payload,
        )

        # TODO: improve the error message when AWS specifies StatusCode != 200
        if response['StatusCode'] != 200:
            raise Exception('Lambda failed for some unknown reason')

        else:
            calculator_response = json.loads(response['Payload'].read())
            if calculator_response['StatusCode'] != 200:
                raise Exception(calculator_response['Message'])

            else:
                return calculator_response['Message']
