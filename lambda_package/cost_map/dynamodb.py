from __future__ import absolute_import

from lambda_package.storage import DynamoDBStorage
from .utils import read_cost_json

_BATCH_WRITE_SIZE = 25  # cannot be larger than 25
_MAX_WRITE_RETRIES = 9  # corresponds to total net delay of 1022 seconds (enough for auto scaling to work)


class DynamoDBCostMap(object):
    @staticmethod
    def _pack_oops(oop_dict):
        return {pid: '{0:.2f}'.format(oop) for pid, oop in oop_dict.iteritems()}

    @staticmethod
    def _unpack_oops(oop_dict):
        return {pid: float(oop) for pid, oop in oop_dict.iteritems()}

    @staticmethod
    def _key_tuple_to_dict(tuple):
        return {
            'month': tuple[0],
            'uid': tuple[1],
            'state': tuple[2],
        }

    @staticmethod
    def _key_dict_to_tuple(dct):
        return dct['month'], dct['uid'], dct['state']

    @staticmethod
    def _key_value_pair_to_item(key_value_pair):
        key, value = key_value_pair
        return dict(DynamoDBCostMap._key_tuple_to_dict(key), oops=value)

    def __init__(self, table_name, aws_options):
        """ Construct a DynamoDBStorage object

        :param table_name: table name that stores the costs.
        :param aws_options: Information related to AWS access and credentials to use to access the DynamoDB instance
                            to be used. Leave 'None' if you wish to run locally.
                            Otherwise, it should be an object containing the following information:
                            {
                                'aws_access_key_id': <access key to use>,
                                'aws_secret_access_key': <secret key to use>,
                                'region_name': <region name to use>. Defaults to 'us-east-1' if not specified,
                                'profile_name': profile name to use instead of access key to use.
                            }

        """

        aws_options.setdefault('region_name', 'us-east-1')

        self._storage = DynamoDBStorage(table_name=table_name,
                                        aws_options=aws_options,
                                        packer=self._pack_oops,
                                        unpacker=self._unpack_oops)

    def add_items(self, cost_items):
        self._storage.add_items(cost_items)

    def update_items(self, cost_items):
        self._storage.update_items(cost_items)

    def delete_items(self, cost_items):
        self._storage.delete_items(cost_items)

    def get_items(self, cost_item_keys):
        '''
        :param cost_item_keys: a list of tuples containing key values for which to retrieve values.
        Example: the key columns are 'month', 'uid', and 'state', so if you want to query for records with a
        month of '01', a uid of 'ABCD', and a FIPS code of '05', cost_item_keys should be:
        [{
            'month': '01',
            'uid':   'ABCD',
            'state':  '05'
        }]
        :return: List of items with the keys passed in and their value:
         Example: {
            'month': '01',
            'uid':   'ABCD',
            'state':  '05',
            'oops':  <dict of OOP costs>
        '''
        keys = (DynamoDBCostMap._key_dict_to_tuple(dct) for dct in cost_item_keys)
        key_value_pairs = self._storage.get_items(keys)
        return (DynamoDBCostMap._key_value_pair_to_item(key_value_pair)
                for key_value_pair in key_value_pairs)

    @staticmethod
    def from_json(table_name, aws_options, filename):
        cost_map = DynamoDBCostMap(table_name, aws_options)
        cost_map.add_items(read_cost_json(filename))

        return cost_map
