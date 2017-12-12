from __future__ import absolute_import

import itertools
import time
import random
import boto3

from .base import BaseStorage

_LOCAL_ENDPOINT_URL = 'http://localhost:8000'

_BATCH_READ_SIZE = 100  # cannot be larger than 100
_MAX_READ_RETRIES = 6  # corresponds to total net delay of 1.28 seconds


def _hyphenate(month, uid):
    return '{}-{}'.format(month, uid)


def _hash_key_from_item(item):
    return _hyphenate(item['month'], item['uid'])


# The default values match those of boto3.Session():
def _get_session(aws_access_key_id=None, aws_secret_access_key=None,
                 region_name=None, profile_name=None):
    # Delegate what information to be used to boto3.Session():
    return boto3.Session(aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name,
                         profile_name=profile_name)


def _get_db_reference(aws_options):
    """ Returns an object for the high-level DynamoDB API

    :param aws_options: Object containing the following:
                            {
                                'aws_access_key_id': <access key to use>,
                                'aws_secret_access_key': <secret access key to use>,
                                'region_name': <region name to use>,
                                'profile_name': <profile name to use>
                            }

                            region_name: region name to be used.
                            aws_access_key_id and aws_secret_access_key: access key and secret key to use
                            for AWS credentialing.

                        Use 'None' if you want to run DynamoDB locally, at the endpoint
                        specified by _LOCAL_ENDPOINT_URL.

    :return: a DynamoDB resource object.
    """

    use_local = True if aws_options is None else False
    if use_local:
        return boto3.resource('dynamodb', endpoint_url=_LOCAL_ENDPOINT_URL)

    else:
        session = _get_session(**aws_options)
        return session.resource('dynamodb')


# Use the given *dictionary* as is and add keys to it.
def as_is(x):
    return x


# when merging with pr #2, can inherit BaseStorage from BaseStorage
class DynamoDBStorage(BaseStorage):
    def __init__(self, table_name, aws_options=None, packer=as_is, unpacker=as_is):
        self.table_name = table_name

        self._packer = packer
        self._unpacker = unpacker

        self._db_reference = _get_db_reference(aws_options)

        self._table = self.get_table()

    def get_table(self):
        table = self._db_reference.Table(self.table_name)

        #table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)

        return table

    def create_cost_table(self, resource, table_name, read_capacity, write_capacity, blocking=True):
        # Create the DynamoDB table.
        table = resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'month-uid',
                    'KeyType': 'HASH',  # partition (hash) key specification
                },
                {
                    'AttributeName': 'state',
                    'KeyType': 'RANGE',  # sort key specification
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'month-uid',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'state',
                    'AttributeType': 'S',  # treat FIPS as string
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity,
            },
        )

        # Wait until table creation is completed:
        if blocking:
            table.wait_until_exists()

        return table

    @staticmethod
    def _convert_keys_to_dynamo_keys(key):
        month, uid, state = key
        return {
            'month-uid': _hyphenate(month, uid),
            'state': state
        }

    @staticmethod
    def _build_key_for_dynamo_item(item):
        month, uid = str(item['month-uid']).split('-')
        return month, uid, item['state']

    # TODO: the implementation is not very efficient, switching between iterators and lists.
    # A template for implementing the exponential delay comes form
    #     https://phvu.net/2014/09/30/how-to-handle-provisionedthroughputexceededexception-in-dynamodb/
    def get_items(self, keys):
        keys_1, keys_2 = itertools.tee(keys)

        # batch_get_item() only accepts distinct keys:
        unique_keys = set(keys_1)
        unprocessed_keys = list(DynamoDBStorage._convert_keys_to_dynamo_keys(key)
                                for key in unique_keys)

        retries = 0

        # The random state is restored at the end of the function in case the calling function
        # wants a reproducible behavior:
        random_state = random.getstate()
        random.seed()  # this may not work well under parallelization

        # List of dictionaries is kept to minimize call to pandas:
        responses = []
        while unprocessed_keys:
            # batch_get_item() capacity is limited to the lower of 100 items or 16 MB payload:
            try:
                response = self._db_reference.batch_get_item(
                    RequestItems={
                        self.table_name: {
                            'Keys': unprocessed_keys[:_BATCH_READ_SIZE],
                            'ConsistentRead': False,  # eventually consistent reads
                        },
                    },
                    ReturnConsumedCapacity='TOTAL',
                )

                responses += response['Responses'].get(self.table_name, [])
                remaining_keys = (response['UnprocessedKeys'].get(self.table_name, {})
                                  .get('Keys', []))

            except Exception as e:
                # The ProvisionedThroughputExceededException mentioned in boto3 1.4.4.
                # documentation cannot be found.
                # raise e
                print e
                remaining_keys = unprocessed_keys[:_BATCH_READ_SIZE]

            # We will not reach the 16 MB capacity limit. So, all 100 items should be
            # process with sufficient read capacity.
            if remaining_keys:
                retries += 1
                if retries > _MAX_READ_RETRIES:
                    random.setstate(random_state)
                    raise Exception('Maximum number of retries reached for reads.')

                # Exponential delay algorithm:
                max_sleep_time = 2.0 ** retries / 100.0  # start with 20 ms delay
                time.sleep(random.uniform(0, max_sleep_time))
                print 'read delayed (retries: {})'.format(retries)

            else:
                retries = 0

            # Process whatever is unprocessed from this iteration and what remains in the
            # unprocessed_keys:
            unprocessed_keys = remaining_keys + unprocessed_keys[_BATCH_READ_SIZE:]

        random.setstate(random_state)

        response_dict = dict((DynamoDBStorage._build_key_for_dynamo_item(response),
                              self._unpacker(response['oops'])) for response in responses)

        return ((key, response_dict[key]) for key in keys_2)

    def add_items(self, cost_items):
        # prevent the caller's items from being modified

        with self._table.batch_writer() as batch:
            # No de-duplication is specified. See
            #     http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html#DynamoDB.Table.batch_writer
            for cost_item in cost_items:
                db_item = {
                    'month-uid': _hash_key_from_item(cost_item),
                    'state': cost_item['state'],
                    'oops': self._packer(cost_item['oops']),
                }

                batch.put_item(db_item)

    def update_items(self, cost_items):
        if self._table.table_status != 'ACTIVE':
            raise Exception('Table {} is not ready to update items.'.format(self._table.name))

        for item in cost_items:
            key_for_item = _hash_key_from_item(item)

            for pid, oop_cost in item['oops'].iteritems():
                oop_str = str(oop_cost)
                state = item['state']
                try:
                    self._table.update_item(
                        Key={
                            'month-uid': key_for_item,
                            'state': state
                        },
                        UpdateExpression='SET oops.#pid = :t',
                        ExpressionAttributeNames={
                            '#pid': str(pid)
                        },
                        ExpressionAttributeValues={
                            ':t': oop_str
                        },
                    )
                except Exception:
                    raise Exception(
                        'Error: Cannot update. Item with month-uid {} and state {} does not exist.'.format(
                            key_for_item, state))

    def delete_items(self, cost_items):
        if self._table.table_status != 'ACTIVE':
            raise Exception('Table {} is not ready to delete items.'.format(self._table.name))

        for item in cost_items:
            with self._table.batch_writer() as batch:
                batch.delete_item(
                    Key={
                        'month-uid': _hash_key_from_item(item),
                        'state': item['state']
                    },
                )
