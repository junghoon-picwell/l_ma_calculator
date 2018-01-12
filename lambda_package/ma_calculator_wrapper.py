#!/usr/bin/env python3
from __future__ import print_function

import json
import logging
import os

from batch_api import run_batch
if os.path.isfile('benefits.py'):
    from benefits import MA_PLANS  # load benefits at load time
from config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)
from breakdown_api import run_breakdown
from package_utils import (
    message_failure,
    message_success,
)
from shared_utils import (
    ClaimsClient,
    BenefitsClient,
    TimeLogger,
)

logging.basicConfig()
logger = logging.getLogger()


class DummyBenefitsClient(object):
    """ Provides BenefitsClient interface for MA_PLANS.
    """
    def get(self, pids):
        pids = set(str(pid) for pid in pids)
        return filter(lambda plan: str(plan['picwell_id']) in pids, MA_PLANS)


def _configure_logging(logger, log_level):
    if log_level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif log_level == 'INFO':
        logger.setLevel(logging.INFO)
    elif log_level == 'WARNING':
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.ERROR)


def _run_calculator(run_options, aws_options):
    configs = ConfigInfo(CONFIG_FILE_NAME)
    _configure_logging(logger, configs.log_level)

    with TimeLogger(logger,
                    start_message='Clock started at {time}.',
                    end_message='Clock stopped at {time} (elapsed: {elapsed} seconds)') as tl:
        # Setup clients to load claims and benefits:
        claims_client = ClaimsClient(aws_options)

        if configs.use_s3_for_benefits:
            benefits_client = BenefitsClient(aws_options)

        else:
            benefits_client = DummyBenefitsClient()

        service = run_options['service']
        if service == 'batch':
            try:
                uid = run_batch(claims_client, benefits_client, configs.claims_year, run_options,
                                configs.costs_table, aws_options)

                message = 'Batch calculation complete for {} (elapsed: {} seconds).'.format(uid, tl.elapsed)
                result = message_success(message)

            except Exception as e:
                result = message_failure(e.message)

        elif service == 'breakdown':
            try:
                costs = run_breakdown(claims_client, benefits_client, configs.claims_year,
                                      run_options)

                message = 'Cost breakdown complete (elapsed: {} seconds).'.format(tl.elapsed)
                result = message_success(message, costs)

            except Exception as e:
                result = message_failure(e.message)

        else:
            result = message_failure('Unrecognized service: {}'.format(service))

    return result


def lambda_handler(event, context):
    """
    This function is the lambda entry point.

    :param event: dict containing info passed in from lambda environment.
                  Query String values end up in event['queryStringParameters']
                    The only needed parameter is 'uid'. 'states' is an optional list of FIPS codes.
                  HTTP Method ends up in event['httpMethod']

    :param context: see docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    :return: HTTP Response based on success/failure of operation
    """
    aws_options = {
        'region_name': 'us-east-1',
    }
    operations = {
        'GET': lambda run_options: _run_calculator(run_options, aws_options)
    }

    operation = event['httpMethod']
    if operation in operations:
        payload = (event['queryStringParameters'] if operation == 'GET'
        else json.loads(event['body']))

        return operations[operation](payload)

    else:
        return message_failure('Unsupported method "{}"'.format(operation))


if __name__ == '__main__':
    # This is the entry point of this code when run locally. In general, this is
    # run as a lambda, where the `main()` function is directly called.

    from argparse import ArgumentParser

    parser = ArgumentParser(description="Medicare Advantage On-Demand OOP Cost Calculator")
    args = parser.parse_args()

    aws_options = {
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
        'region_name': None,
        'profile_name': 'sandbox',
    }

    run_options = {
        'service': 'batch',
        'uid': '1175404001',
        'months': ['01'],
        'states': ['42', '15'],
    }

    print('BATCH RUN')
    print()
    print(_run_calculator(run_options, aws_options))
    print()

    run_options = {
        'service': 'breakdown',
        'uids': ['1302895801', '3132439001', '2294063501', '1280937802', '31812914701'],
        'pids': ['2820028008119', '2820088001036'],
        'month': '01',
    }

    print('BREAKDOWN RUN')
    print()
    print(_run_calculator(run_options, aws_options))