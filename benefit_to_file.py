"""
Convert a JSON MA benefit file into a file containing Python dictionaries.

Run like this:
    python benefit_to_file.py s3://picwell.sandbox.medicare/ma_benefits/cms_2018_pbps_20171005.json lambda_package/benefits_2018.py
"""

import argparse
import pprint

from etltools import s3


def benefit_to_string(plans):
    yield 'MA_PLANS = [\n'

    for plan in plans:
        yield pprint.pformat(plan) + ',\n'

    yield ']\n'


if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description=__doc__)

    parser.add_argument('json_file', type=str, help='JSON MA benefit file')
    parser.add_argument('py_file', type=str, help='python file')

    args = parser.parse_args()

    plans = s3.read_json(args.json_file)
    lines = benefit_to_string(plans)

    with open(args.py_file, 'w') as fp:
        fp.writelines(lines)


