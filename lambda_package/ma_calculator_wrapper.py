#!/usr/bin/env python3
from __future__ import print_function

import json
import logging
from datetime import datetime
import os

from batch_api import run_batch
if os.path.isfile('benefits.py'):
    from benefits import MA_PLANS  # load benefits at load time
from config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)
from detailed_api import run_detailed
from utils import (
    fail_with_message,
)
from storage_utils import (
    ClaimsClient,
    BenefitsClient,
)

logger = logging.getLogger()
logging.basicConfig()


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


def main(run_options, aws_options):
    configs = ConfigInfo(CONFIG_FILE_NAME)
    _configure_logging(logger, configs.log_level)

    start_time = datetime.now()
    logger.info('Clock started at {}'.format(str(start_time)))

    if 'uid' not in run_options:
        return {
            'statusCode': '400',
            'message': 'missing "uid"',
        }
    uid = run_options['uid']

    # look up claims:
    logger.info('Retrieving claims for {}...'.format(uid))
    claim_time = datetime.now()

    try:
        claims_client = ClaimsClient(aws_options)
        person = claims_client.get(uid)

    except Exception as e:
        logger.error(e.message)
        return fail_with_message(e.message)

    claim_elapsed = (datetime.now() - claim_time).total_seconds()
    logger.info('Finished retrieving claims for {} in {} seconds.'.format(uid, claim_elapsed))

    # look up plans from s3
    logger.info('Retrieving benefits file...')
    benefit_time = datetime.now()

    try:
        if configs.use_s3_for_benefits:
            benefits_client = BenefitsClient(aws_options)

        else:
            benefits_client = DummyBenefitsClient()

    except Exception as e:
        logger.error(e.message)
        return fail_with_message(e.message)

    benefit_elapsed = (datetime.now() - benefit_time).total_seconds()
    logger.info('Finished retrieving benefits file in {} seconds.'.format(benefit_elapsed))

    service = run_options.get('service', 'batch')
    if service == 'batch':
        return run_batch(person, benefits_client, configs.claims_year, run_options,
                         configs.costs_table, aws_options,
                         logger, start_time)

    elif service == 'detailed':
        return run_detailed(person, benefits_client, configs.claims_year, run_options,
                            logger, start_time)

    else:
        return fail_with_message('Unrecognized service: {}'.format(service))


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
        'GET': lambda run_options: main(run_options, aws_options)
    }

    operation = event['httpMethod']
    if operation in operations:
        payload = (event['queryStringParameters'] if operation == 'GET'
        else json.loads(event['body']))

        return operations[operation](payload)

    else:
        return fail_with_message('Unsupported method "{}"'.format(operation))


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

    # run_options = {
    #     'service': 'detailed',
    #     'uid': '1175404001',
    #     'pids': ['2820028008119', '2820088001036'],
    # }

    print(main(run_options, aws_options))
