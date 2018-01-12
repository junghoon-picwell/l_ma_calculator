from __future__ import absolute_import

import json
import boto3
import logging

# from multiprocessing.pool import ThreadPool
from .shared_utils import ThreadPool

_MAX_THREADS = 100  # prevent opening too many files

from .invocation_types import InvocationType


logger = logging.getLogger()


class CalculatorClient(object):

    def __init__(self, aws_info=None):
        self._aws_info = {} if aws_info is None else aws_info

    def __getstate__(self):
        raise Exception('CalculatorClient object cannot be pickled')

    # TODO: improve error handling with threading.Thread since some threads can fail.
    def get_breakdown(self, uids, pids, month='01',
                      max_uids_to_calculate=None, max_lambda_calls=None,
                      verbose=False):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'breakdown',
                'uids': uids,
                'pids': pids,
                'month': month,
            }
        }
        if max_uids_to_calculate is not None:
            request['queryStringParameters']['max_uids_to_calculate'] = max_uids_to_calculate
        if max_lambda_calls is not None:
            request['queryStringParameters']['max_lambda_calls'] = max_lambda_calls

        # boto3 is not thread safe:
        # http://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
        session = boto3.Session(**self._aws_info)
        client = session.client('lambda')

        encoded_payload = bytes(json.dumps(request)).encode('utf-8')
        response = client.invoke(
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
                if verbose:
                    print calculator_response['Message']
                return calculator_response['Return']
