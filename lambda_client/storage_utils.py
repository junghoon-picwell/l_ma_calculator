# Absolute imports are not used because the code is shared with lambda_package:
#
# from __future__ import  absolute_import

import boto3
import datetime
import logging
import json
import os
import Queue
import threading

from config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)

logger = logging.getLogger()

def _json_from_s3(s3_bucket, s3_path, resource):
    content_object = resource.Object(s3_bucket, s3_path)

    file_content = content_object.get()
    decoded_body = file_content['Body'].read().decode('utf-8')

    return decoded_body


def _read_json(s3_bucket, s3_path, resource):
    lines_from_s3 = _json_from_s3(s3_bucket, s3_path, resource)
    return [json.loads(l) for l in lines_from_s3.splitlines()]


class ClaimsClient(object):
    __slots__ = ('_aws_info', '_s3_bucket', '_s3_path', '_table_name')

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

    def _get_from_s3(self, uid):
        session = boto3.Session(**self._aws_info)
        resource = session.resource('s3')

        file_name = os.path.join(self._s3_path, '{}.json'.format(uid))
        claims_list = _read_json(self._s3_bucket, file_name, resource)

        if not claims_list:
            message = 'No user data located at s3://{}'.format(file_name)
            raise Exception(message)

        return claims_list[0]

    def _get_from_dynamodb(self, uid):
        session = boto3.Session(**self._aws_info)
        resource = session.resource('dynamodb')
        table = resource.Table(self._table_name)

        response = table.get_item(Key={'uid': uid},
                                  ConsistentRead=False)
        if 'Item' not in response or not response['Item']:  # not sure exactly what happens
            message = 'No user data located in {} table'.format(self._table_name)
            raise Exception(message)

        return response['Item']

    def get(self, uid):
        return self._get_from_s3(uid) if self.use_s3 else self._get_from_dynamodb(uid)


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

        # Issue a thread for each state given:
        start = datetime.datetime.now()

        queue = Queue.Queue()
        threads = []
        for state in states:
            t = threading.Thread(target=lambda q, s: q.put(self._get_one_state(s)),
                                 args=(queue, state))
            threads.append(t)
            t.start()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to start all threads for get_by_state().'.format(time_elapsed))

        # Wait for all threads to finish:
        start = datetime.datetime.now()

        for t in threads:
            t.join()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to join all threads for get_by_state().'.format(time_elapsed))

        # Combine all the results:
        start = datetime.datetime.now()

        plans = []
        while not queue.empty():
            plans += queue.get()

        time_elapsed = (datetime.datetime.now() - start).total_seconds()
        logger.info('{} seconds to combine results for get_by_state().'.format(time_elapsed))

        return plans

    def get_by_pid(self, pids):
        # Ensure that PIDs are treated as strings:
        pids = set(str(pid) for pid in pids)

        # Read plans in only those necessasry states:
        states = {pid[-2:] for pid in pids}
        plans = self.get_by_state(states)

        return filter(lambda plan: str(plan['picwell_id']) in pids, plans)

    def get_all(self):
        return self.get_by_state(self.all_states)

