from __future__ import absolute_import

import datetime
import json
import boto3
import logging
import Queue
import threading
import time

from .invocation_types import InvocationType

_MAX_ACTIVE_THREADS = 100
_DELAY_TO_FINISH = 0.01

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
        # Issue a thread for each state given:
        start = datetime.datetime.now()

        queue = Queue.Queue()
        threads = []
        for uid in uids:
            # Limit the number of active threads to manage the number of open files:
            while threading.active_count() >= _MAX_ACTIVE_THREADS:
                time.sleep(_DELAY_TO_FINISH)

            t = threading.Thread(target=lambda q, u, p, m: q.put(self._get_one_breakdown(u, p, m)),
                                 args=(queue, uid, pids, month))
            threads.append(t)
            t.start()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to start all threads for get_breakdown().'.format(time_elapsed))

        # Wait for all threads to finish:
        start = datetime.datetime.now()

        for t in threads:
            t.join()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to join all threads for get_breakdown().'.format(time_elapsed))

        # Combine all the results:
        start = datetime.datetime.now()

        costs = []
        while not queue.empty():
            costs += queue.get()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to combine results for get_breakdown().'.format(time_elapsed))

        return costs
