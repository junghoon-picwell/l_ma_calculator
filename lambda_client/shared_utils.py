# Absolute imports are not used because the code is shared with lambda_package:
#
# from __future__ import  absolute_import

import boto3
import datetime
import logging
import json
import multiprocessing
import os
import Queue
import random
import threading
import time

from config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)

# This limit is set by AWS:
#
# See BatchGetItem in https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
_BATCH_READ_SIZE = 100

# This limits opening too many files:
MAX_THREADS = 100

_MAX_DYNAMODB_TRIES = 7

logger = logging.getLogger()
# logger = None


class TimeLogger(object):
    __slots__ = (
        '_logger',
        '_start_message',
        '_end_message',
        '_start_time',
    )

    def __init__(self, logger, end_message='', start_message=''):
        """
        Use {time} and {elapsed} to capture the current time and time elapsed. {elapsed} is
        only meaningful for the end_message.
        """
        self._logger = logger
        self._start_message = start_message
        self._end_message = end_message

    @property
    def start_time(self):
        return self._start_time

    @property
    def elapsed(self):
        return (datetime.datetime.now() - self._start_time).total_seconds()

    def __enter__(self):
        self._start_time = datetime.datetime.now()

        if self._start_message and self._logger is not None:
            self._logger.info(self._start_message.format(time=self._start_time))

        return self

    # TODO: introduce better error handling?
    def __exit__(self, exception_type, exception_value, traceback):
        time = datetime.datetime.now()

        if self._end_message and self._logger is not None:
            self._logger.info(self._end_message.format(time=time, elapsed=self.elapsed))


class ThreadPool(object):
    """
    Alternative implementation of multiprocessing.pool.ThreadPool for AWS Lambda. We
    found that the implementation in multiprocessing uses too much memory and also
    has some instability issues on AWS Lambda, possibly due multiprocessing.Queue
    described here:

    https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/

    The current implementation does not really recycle threads; it just limits the
    total number of active threads. See

    https://github.com/python/cpython/blob/master/Lib/multiprocessing/pool.py

    for a better implementation.
    """
    _DELAY = 0.01

    def __init__(self, processes=None):
        self._processes = multiprocessing.cpu_count() if processes is None else processes

    def imap(self, fun, sequence):
        with TimeLogger(logger,
                        end_message='Thread initialization took {elapsed} seconds.'):
            queue = Queue.Queue()
            threads = []
            for index, value in enumerate(sequence):
                # Limit the number of active threads to manage the number of open files:
                while threading.active_count() >= self._processes:
                    time.sleep(ThreadPool._DELAY)

                t = threading.Thread(target=lambda q, i, v: q.put([index, fun(v)]),
                                     args=(queue, index, value))
                threads.append(t)
                t.start()

        # Wait for all threads to finish:
        with TimeLogger(logger,
                        end_message='Joining all threads took {elapsed} seconds.'):
            for t in threads:
                t.join()

        # Combine all the results:
        with TimeLogger(logger,
                        end_message='Combining all results in order took {elapsed} seconds.'):
            pairs = []  # (index, return value) pairs
            while not queue.empty():
                pairs.append(queue.get())
            sorted_pairs = sorted(pairs, key=lambda pair: pair[0])

        for index, value in sorted_pairs:
            yield value

    def map(self, fun, sequence):
        return list(self.imap(fun, sequence))


def _json_from_s3(s3_bucket, s3_path, resource):
    content_object = resource.Object(s3_bucket, s3_path)

    file_content = content_object.get()
    decoded_body = file_content['Body'].read().decode('utf-8')

    return decoded_body


def _read_json(s3_bucket, s3_path, resource):
    lines_from_s3 = _json_from_s3(s3_bucket, s3_path, resource)
    return [json.loads(l) for l in lines_from_s3.splitlines()]


# The following two functions are very similar to what is in misscleo:
def _read_batch_from_dynamodb(keys, table_name, aws_info):
    """
    Make one batch read request to a DynamoDB table.

    The keys need to be unique. The order of returned items and unprocessed keys can
    be different from the order of the keys given.

    :param keys: a sequence of dictionaries, where each dictionary is a unique key.
    :param table_name: name of the DynamoDB table.
    :param aws_info: information to create a session. {} can be used.
    :return: {
                 'items': list of items returned by DynamoDB. None is returned if
                          an error occurs.
                 'unprocessed_keys': list of keys not processed.
             }
    """
    session = boto3.Session(**aws_info)
    resource = session.resource('dynamodb')

    # Guard against error due to throttling:
    try:
        # Only use keys within the maximum batch size. The rest is returned as unprocessed.
        response = resource.batch_get_item(
            RequestItems={
                table_name: {
                    'Keys': keys[:_BATCH_READ_SIZE],
                    'ConsistentRead': False,  # eventually consistent reads
                },
            },
            ReturnConsumedCapacity='TOTAL',
        )

        # Keys that do not exist in the table will be ignored by DynamoDB:
        items = response['Responses'].get(table_name, [])
        unprocessed_keys = (response['UnprocessedKeys'].get(table_name, {})
                                                       .get('Keys', []))

    except Exception as e:
        logging.info(e.message)
        items = None  # indicate that an error occurred
        unprocessed_keys = keys[:_BATCH_READ_SIZE]

    return {
        'items': items,
        'unprocessed_keys': keys[_BATCH_READ_SIZE:] + unprocessed_keys,
    }


def _read_from_dynamodb(keys, table_name, aws_info):
    """
    Read from a DynamoDB table.

    The keys need to be unique. The order of returned items and unprocessed keys can
    be different from the order of the keys given. Finally, the number of items
    returned can be fewer than the number of keys if some keys do not exist in the
    table.

    :param keys: a sequence of dictionaries, where each dictionary is a unique key.
    :param table_name: name of the DynamoDB table.
    :param aws_info: information to create a session. {} can be used.
    :return: {
                 'items': list of items returned by DynamoDB.
                 'unprocessed_keys': list of keys not processed.
             }
    """
    # Maximum number of keys that can be processed within a single iteration:
    max_keys = _BATCH_READ_SIZE * MAX_THREADS

    tries = 0
    items = []
    unprocessed_keys = keys

    # Create a random number generator with the seed set randomly:
    rng = random.Random()

    while unprocessed_keys:
        # Break the keys into batches:
        num_keys_to_process = min(len(unprocessed_keys), max_keys)
        key_batches = (unprocessed_keys[start:(start + _BATCH_READ_SIZE)]
                       for start in xrange(0, num_keys_to_process, _BATCH_READ_SIZE))

        # TODO: I don't know why imap() does not work. Furthermore, should use imap_unordered()?
        pool = ThreadPool(MAX_THREADS)
        responses = pool.map(
            lambda ks: _read_batch_from_dynamodb(ks, table_name, aws_info),
            key_batches,
        )

        is_error = False
        unprocessed_keys = unprocessed_keys[num_keys_to_process:]
        for response in responses:
            if response['items'] is None:
                is_error = True
            else:
                items += response['items']
            unprocessed_keys += response['unprocessed_keys']

        if is_error:
            tries += 1
            if tries > _MAX_DYNAMODB_TRIES:
                raise Exception('Giving up after {} tries'.format(_MAX_DYNAMODB_TRIES))

            # Exponential delay algorithm:
            max_sleep_time = 2.0 ** tries / 100.0  # start with 20 ms delay
            sleep_time = rng.uniform(0, max_sleep_time)

            time.sleep(sleep_time)

            logger.info('Retrying after {} seconds.'.format(sleep_time))

        else:
            tries = 0

    return items


class ClaimsClient(object):
    __slots__ = ('_aws_info', '_s3_bucket', '_s3_path', '_table_name')

    @staticmethod
    def format_claim(claim):
        return {
            'admitted': str(claim['admitted']),
            'discharged': str(claim['discharged']),
            'benefit_category': int(claim['benefit_category']),
            'length_of_stay': int(claim['length_of_stay']),
            'cost': float(claim['cost']),
        }

    @staticmethod
    def format_person(person):
        return {
            'uid': str(person['uid']),
            'medical_claims': [ClaimsClient.format_claim(claim)
                               for claim in person['medical_claims']],
        }

    def __init__(self, aws_info=None,
                 s3_bucket=None, s3_path=None,
                 table_name=None):
        # 3 options are provided: use config file, S3, or DynamoDB.
        use_config = (s3_bucket is None and s3_path is None and table_name is None)
        assert (use_config or
                (s3_bucket is not None and s3_path is not None) or
                table_name is not None)

        if use_config:
            configs = ConfigInfo(CONFIG_FILE_NAME)

            if configs.use_s3_for_claims:
                self._s3_bucket = configs.claims_bucket
                self._s3_path = configs.claims_path
                self._table_name = None

            else:
                self._s3_bucket = None
                self._s3_path = None
                self._table_name = configs.claims_table

        else:
            self._s3_bucket = s3_bucket
            self._s3_path = s3_path
            self._table_name = table_name

        self._aws_info = {} if aws_info is None else aws_info

    @property
    def use_s3(self):
        return self._s3_bucket is not None

    def __getstate__(self):
        # AWS credentials should not be stored.
        raise Exception('ClaimsClient object cannot be pickled.')

    def _get_one_from_s3(self, uid):
        session = boto3.Session(**self._aws_info)
        resource = session.resource('s3')

        file_name = os.path.join(self._s3_path, '{}.json'.format(uid))
        claims_list = _read_json(self._s3_bucket, file_name, resource)

        if not claims_list:
            message = 'No user data located at s3://{}'.format(file_name)
            raise Exception(message)

        return claims_list[0]

    def _get_from_s3(self, uids):
        pool = ThreadPool(processes=MAX_THREADS)
        return pool.map(lambda uid: self._get_one_from_s3(uid), uids)

    def _get_from_dynamodb(self, uids):
        # Only read unique UIDs
        keys = [{'uid': uid} for uid in set(uids)]
        people = _read_from_dynamodb(keys, self._table_name, self._aws_info)

        # For faster lookup:
        people_dict = {person['uid']: person for person in people}
        return [people_dict[uid] for uid in uids]

    def get(self, uids):
        # Issue a thread for each state because this is IO bound:
        #
        # Processes cannot be used because the object cannot be pickled for security reasons.
        if self.use_s3:
            people = self._get_from_s3(uids)
        else:
            people = self._get_from_dynamodb(uids)

        return [ClaimsClient.format_person(person) for person in people]


class BenefitsClient(object):
    __slots__ = ('_aws_info', '_s3_bucket', '_s3_path', '_all_states')

    def __init__(self, aws_info=None, s3_bucket=None, s3_path=None):
        use_config = (s3_bucket is None and s3_path is None)
        assert (use_config or
                (s3_bucket is not None and s3_path is not None))

        configs = ConfigInfo(CONFIG_FILE_NAME)
        if use_config:
            self._s3_bucket = configs.benefits_bucket
            self._s3_path = configs.benefits_path

        else:
            self._s3_bucket = s3_bucket
            self._s3_path = s3_path

        self._aws_info = {} if aws_info is None else aws_info
        self._all_states = configs.all_states

    @property
    def all_states(self):
        return self._all_states

    def __getstate__(self):
        # AWS credentials should not be stored.
        raise Exception('BenefitsClient object cannot be pickled.')

    def _get_one_state(self, state):
        # boto3 is not thread safe:
        # http://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
        session = boto3.Session(**self._aws_info)
        resource = session.resource('s3')

        file_name = os.path.join(self._s3_path, '{}.json'.format(state))
        return _read_json(self._s3_bucket, file_name, resource)

    def get_by_state(self, states):
        assert all(state in self.all_states for state in states)

        # Issue a thread for each state because this is IO bound:
        #
        # Processes cannot be used because the object cannot be pickled for security reasons.
        pool = ThreadPool(processes=MAX_THREADS)

        # Each call can return plans:
        plans = pool.imap(lambda state: self._get_one_state(state), states)

        # Combine and return:
        return sum(plans, [])

    def get_by_pid(self, pids):
        # Ensure that PIDs are treated as strings:
        pids = set(str(pid) for pid in pids)

        # Read plans in only those necessasry states:
        states = {pid[-2:] for pid in pids}
        plans = self.get_by_state(states)

        return filter(lambda plan: str(plan['picwell_id']) in pids, plans)

    def get_all(self):
        return self.get_by_state(self.all_states)
