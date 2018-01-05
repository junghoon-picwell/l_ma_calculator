from __future__ import absolute_import

import json
import boto3
import Queue
import threading
import time

from .invocation_types import InvocationType

_REQUEST_DELAY = 0.03  # at most 33 requests per second


class CalculatorClient(object):

    def __init__(self, aws_info=None):
        session = boto3.Session(**aws_info)
        self._resource = session.client('lambda')

    def _get_one_breakdown(self, uid, pids, month):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'detailed',
                'uid': uid,
                'month': month,
            }
        }

        if pids is not None:
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

    # TODO: improve error handling with threading.Thread since some threads can fail.
    def get_breakdown(self, uids, pids=None, month='01'):
        # Issue a thread for each state given:
        queue = Queue.Queue()
        threads = []
        for uid in uids:
            t = threading.Thread(target=lambda q, u, p, m: q.put(self._get_one_breakdown(u, p, m)),
                                 args=(queue, uid, pids, month))
            threads.append(t)
            t.start()
            time.sleep(_REQUEST_DELAY)

        # Wait for all threads to finish:
        for t in threads:
            t.join()

        # Combine all the results:
        costs = []
        while not queue.empty():
            costs += queue.get()

        return costs
