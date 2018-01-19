from __future__ import absolute_import

import base64
import boto3
import datetime
import json
import logging
import Queue
import threading
import time

# from multiprocessing.pool import ThreadPool
from .shared_utils import ThreadPool

_MAX_THREADS = 100  # prevent opening too many files

logger = logging.getLogger()


class CalculatorClient(object):

    def __init__(self, aws_info=None):
        self._aws_info = {} if aws_info is None else aws_info

    def __getstate__(self):
        raise Exception('CalculatorClient object cannot be pickled')

    def _issue_request(self, request, use_s3_for_claims,
                       max_calculated_uids, max_lambda_calls, verbose):
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
            InvocationType='RequestResponse',
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
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'breakdown',
                'uids': uids,
                'pids': pids,
                'month': month,
            }
        }

        return self._issue_request(request, use_s3_for_claims,
                                   max_calculated_uids, max_lambda_calls, verbose)

    def get_oop(self, uids, pids, month='01',
                use_s3_for_claims=None,
                max_calculated_uids=None, max_lambda_calls=None,
                verbose=False):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'oop',
                'uids': uids,
                'pids': pids,
                'month': month,
            }
        }

        return self._issue_request(request, use_s3_for_claims,
                                   max_calculated_uids, max_lambda_calls, verbose)

    def run_batch(self, uids, states=None, months=None,
                  use_s3_for_claims=None,
                  max_calculated_uids=None, max_lambda_calls=None,
                  verbose=False):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'batch',
                'uids': uids,
            }
        }
        if states is not None:
            request['queryStringParameters']['states'] = states
        if months is not None:
            request['queryStringParameters']['months'] = months

        return self._issue_request(request, use_s3_for_claims,
                                   max_calculated_uids, max_lambda_calls, verbose)


def run_batch_on_schedule(fun, uids, num_writes_per_uid, mean_runtime,
                          min_writes, max_writes, fraction_increment=0.2, increment_interval=90,
                          verbose=False):
    max_writes = float(max_writes)  # make curr_writes float

    queue = Queue.Queue()
    threads = []
    remaining_uids = uids

    start_time = datetime.datetime.now()
    while remaining_uids:
        # Evaluate current capacity:
        seconds_since_start = (datetime.datetime.now() - start_time).total_seconds()
        intervals_since_start = int(float(seconds_since_start)/increment_interval)

        curr_writes = min(min_writes*(1.0 + fraction_increment)**intervals_since_start,
                          max_writes)
        assert curr_writes >= min_writes  # possible overflow???

        # Number of UIDs that can be evaluated per second at steady-state under current capacity:
        num_uids = max(float(curr_writes)/(num_writes_per_uid*mean_runtime), 1.0)
        num_uids = min(num_uids, 900.0/mean_runtime)  # lambda throttling
        
        rounded_uids = int(round(num_uids))
        time_delta = rounded_uids/num_uids

        t = threading.Thread(target=lambda q, u: q.put(fun(u)),
                             args=(queue, remaining_uids[:rounded_uids]))
        threads.append(t)
        t.start()

        remaining_uids = remaining_uids[rounded_uids:]
        if verbose:
            print ('\r{}: issuing {} UIDs for every {} seconds'.format(seconds_since_start,
                                                                       rounded_uids,
                                                                       time_delta) +
                   ' ({} intervals, {} remaining)'.format(intervals_since_start,
                                                          len(remaining_uids)))

        time.sleep(time_delta)

    for t in threads:
        t.join()

    responses = []
    while not queue.empty():
        responses += queue.get()

    return responses
