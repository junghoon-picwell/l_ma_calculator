import boto3
import json
import os
import Queue
import threading

from config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)

def _json_from_s3(s3_bucket, s3_path, resource):
    content_object = resource.Object(s3_bucket, s3_path)

    file_content = content_object.get()
    decoded_body = file_content['Body'].read().decode('utf-8')

    return decoded_body


def _read_json(s3_bucket, s3_path, resource):
    lines_from_s3 = _json_from_s3(s3_bucket, s3_path, resource)
    return [json.loads(l) for l in lines_from_s3.splitlines()]


class ClaimsClient(object):
    __slots__ = ('_resource', '_s3_bucket', '_s3_path', '_table_name')

    def __init__(self, aws_info,
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

        session = boto3.Session(**aws_info)
        self._resource = session.resource('s3' if self.use_s3 else 'dynamodb')

    @property
    def use_s3(self):
        return self._s3_bucket is not None

    def _get_from_s3(self, uid):
        file_name = os.path.join(self._s3_path, '{}.json'.format(uid))
        claims_list = _read_json(self._s3_bucket, file_name, self._resource)

        if not claims_list:
            message = 'No user data located at s3://{}'.format(file_name)
            raise Exception(message)

        return claims_list[0]

    def _get_from_dynamodb(self, uid):
        table = self._resource.Table(self._table_name)

        res = table.get_item(Key={'uid': uid},
                             ConsistentRead=False)
        if 'Item' not in res or not res['Item']:  # not sure exactly what happens
            message = 'No user data located in {} table'.format(self._table_name)
            raise Exception(message)

        return res['Item']

    def get(self, uid):
        return self._get_from_s3(uid) if self.use_s3 else self._get_from_dynamodb(uid)


class BenefitsClient(object):
    __slots__ = ('_resource', '_s3_bucket', '_s3_path')
    _ALL_STATES = ('01', '04', '05', '06', '08',
                   '09', '10', '11', '12', '13',
                   '15', '16', '17', '18', '19',
                   '20', '21', '22', '23', '24',
                   '25', '26', '27', '28', '29',
                   '30', '31', '32', '33', '34',
                   '35', '36', '37', '38', '39',
                   '40', '41', '42', '44', '45',
                   '46', '47', '48', '49', '50',
                   '51', '53', '54', '55', '56',
                   '72')

    def __init__(self, aws_info, s3_bucket=None, s3_path=None):
        use_config = (s3_bucket is None and s3_path is None)
        assert (use_config or
                (s3_bucket is not None and s3_path is not None))

        if use_config:
            configs = ConfigInfo(CONFIG_FILE_NAME)

            self._s3_bucket = configs.benefits_bucket
            self._s3_path = configs.benefits_path

        else:
            self._s3_bucket = s3_bucket
            self._s3_path = s3_path

        session = boto3.Session(**aws_info)
        self._resource = session.resource('s3')

    def _get_one_state(self, state):
        file_name = os.path.join(self._s3_path, '{}.json'.format(state))
        return _read_json(self._s3_bucket, file_name, self._resource)

    def _get_all_states(self, states):
        assert all(state in BenefitsClient._ALL_STATES for state in states)

        # Issue a thread for each state given:
        queue = Queue.Queue()
        threads = []
        for state in states:
            t = threading.Thread(target=lambda q, s: q.put(self._get_one_state(s)),
                                 args=(queue, state))
            threads.append(t)
            t.start()

        # Wait for all threads to finish:
        for t in threads:
            t.join()

        # Combine all the results:
        plans = []
        while not queue.empty():
            plans += queue.get()

        return plans

    def get_all(self):
        return self._get_all_states(BenefitsClient._ALL_STATES)

    def get(self, pids):
        pass


# TODO: the following functions should be deprecated:
def read_claims_from_s3(uid, s3_bucket, s3_path, aws_options):
    client = ClaimsClient(aws_options, s3_bucket=s3_bucket, s3_path=s3_path)
    return client._get_from_s3(uid)


def read_claims_from_dynamodb(uid, table_name, aws_options):
    client = ClaimsClient(aws_options, table_name=table_name)
    return client._get_from_dynamodb(uid)


def read_benefits_from_s3(s3_bucket, s3_path, aws_options):
    client = BenefitsClient(aws_options, s3_bucket=s3_bucket, s3_path=s3_path)
    return client.get_all()

