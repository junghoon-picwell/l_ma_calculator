from __future__ import absolute_import

import base64
import boto3
import json
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

    def _issue_interactive_request(self, service, uids, pids, month,
                                   use_s3_for_claims,
                                   max_calculated_uids, max_lambda_calls, verbose):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': service,
                'uids': uids,
                'pids': pids,
                'month': month,
            }
        }
        if use_s3_for_claims is not None:
            request['queryStringParameters']['use_s3_for_claims'] = use_s3_for_claims
        if max_calculated_uids is not None:
            request['queryStringParameters']['max_calculated_uids'] = max_calculated_uids
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
            LogType='Tail' if verbose else 'None',
            Payload=encoded_payload,
        )

        if response['StatusCode'] != 200:
            # TODO: it is not clear when this is supposed to happen. Improve the error message when we do.
            raise Exception('Lambda failed for some unknown reason')

        else:
            payload = json.loads(response['Payload'].read())
            if verbose:
                print base64.b64decode(response['LogResult'])

            if 'FunctionError' in response:
                raise Exception(payload['errorMessage'])

            else:
                return payload

    def get_breakdown(self, uids, pids, month='01',
                      use_s3_for_claims=None,
                      max_calculated_uids=None, max_lambda_calls=None,
                      verbose=False):
        return self._issue_interactive_request('breakdown', uids, pids, month,
                                               use_s3_for_claims,
                                               max_calculated_uids, max_lambda_calls, verbose)

    def get_oop(self, uids, pids, month='01',
                use_s3_for_claims=None,
                max_calculated_uids=None, max_lambda_calls=None,
                verbose=False):
        return self._issue_interactive_request('oop', uids, pids, month,
                                               use_s3_for_claims,
                                               max_calculated_uids, max_lambda_calls, verbose)