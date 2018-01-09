from __future__ import absolute_import

import json
import boto3
import logging
from multiprocessing.pool import ThreadPool

from .invocation_types import InvocationType

# This limits opening too many files:
_MAX_THREADS = None  # use value from cpu_count()

logger = logging.getLogger()


class CalculatorClient(object):

    def __init__(self, aws_info=None):
        self._aws_info = {} if aws_info is None else aws_info

    def __getstate__(self):
        raise Exception('CalculatorClient object cannot be pickled')

    def _get_one_breakdown(self, uid, pids, month):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'breakdown',
                'uid': uid,
                'pids': pids,
                'month': month,
            }
        }

        encoded_payload = bytes(json.dumps(request)).encode('utf-8')

        # boto3 is not thread safe:
        # http://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
        session = boto3.Session(**self._aws_info)
        client = session.client('lambda')

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
                return calculator_response['Message']

    # TODO: improve error handling with threading.Thread since some threads can fail.
    def get_breakdown(self, uids, pids, month='01'):
        # Issue a thread for each person:
        #
        # Processes cannot be used because the object cannot be pickled for security reasons.
        pool = ThreadPool(processes=_MAX_THREADS)

        # Each call can return costs for multiple plans:
        costs = pool.map(lambda uid: self._get_one_breakdown(uid, pids, month), uids)

        # Combine all costs and return:
        return sum(costs, [])
