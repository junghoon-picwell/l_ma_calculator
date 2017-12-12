import json
import os


def read_cost_json(filename):
    # We assume that each line of the json file looks like
    #     {'month': '01', 'uid': 'ajkljs', 'state': 'AL', 'oops': {'pid 1': value_1, ...}}
    filename = os.path.expanduser(filename)
    if not os.path.isfile(filename):
        raise Exception('Input JSON file {} does not exist.'.format(filename))

    with open(filename, 'rb') as fp:
        for line in fp.readlines():
            yield json.loads(line)
