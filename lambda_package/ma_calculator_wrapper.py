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
    read_claims_from_s3,
    read_claims_from_dynamodb,
    read_benefits_from_s3,
)

logger = logging.getLogger()
logging.basicConfig()


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


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
    config_values = ConfigInfo(CONFIG_FILE_NAME)
    _configure_logging(logger, config_values.log_level)

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
        if config_values.use_s3_for_claims:
            person = read_claims_from_s3(uid, config_values.claims_bucket, aws_options)
        else:
            person = read_claims_from_dynamodb(uid, config_values.dynamodb_claim_table,
                                               aws_options)
    except Exception as e:
        logger.error(e.message)
        return fail_with_message(e.message)

    claim_elapsed = (datetime.now() - claim_time).total_seconds()
    logger.info('Finished retrieving claims for {} in {} seconds.'.format(uid, claim_elapsed))

    # look up plans from s3
    logger.info('Retrieving benefits file...')
    benefit_time = datetime.now()

    try:
        if config_values.use_s3_for_benefits:
            plans = read_benefits_from_s3(config_values.benefit_bucket, aws_options)
        else:
            plans = MA_PLANS
    except Exception as e:
        logger.error(e.message)
        return fail_with_message(e.message)

    benefit_elapsed = (datetime.now() - benefit_time).total_seconds()
    logger.info('Finished retrieving benefits file in {} seconds.'.format(benefit_elapsed))

    service = run_options.get('service', 'batch')
    if service == 'batch':
        return run_batch(person, plans, config_values.claims_year, run_options,
                         config_values.dynamodb_cost_table, aws_options,
                         logger, start_time)

    elif service == 'detailed':
        return run_detailed(person, plans, config_values.claims_year, run_options,
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

        res = operations[operation](payload)
        if res['statusCode'] != '200':
            return respond(ValueError(res['message']), res['message'])

        return respond(None, res['message'])

    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))


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
