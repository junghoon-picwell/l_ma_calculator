#!/usr/bin/env python3
from __future__ import print_function

import boto3
import json
import logging
from datetime import datetime

from benefits_2018 import MA_PLANS  # see _read_benefits_from_dict()
from calc.calculator import calculate_oop
from config_info import ConfigInfo
from cost_map import DynamoDBCostMap
from s3_helpers import read_json

CLAIMS_PATH = 'junghoon/lambda_calculator'
BENEFITS_PATH = 'ma_benefits/cms_2018_pbps_20171005.json'
CONFIG_FILE_NAME = 'calculator.cfg'

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


def _succeed_with_message(message):
    return {
        'statusCode': '200',
        'message': message
    }


def _fail_with_message(message):
    return {
        'statusCode': '500',
        'message': message
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


def _read_claims_from_s3(uid, s3_bucket, aws_options):
    file_name = CLAIMS_PATH + '/{}.json'.format(uid)
    user_data = read_json(s3_bucket, file_name, aws_options)

    if not user_data:
        message = 'No user data located at {}/{}'.format(s3_bucket, file_name)
        raise Exception(message)

    return user_data[0]


def _read_claims_from_dynamodb(uid, table_name, aws_options):
    session = boto3.Session(**aws_options)
    resource = session.resource('dynamodb')
    table = resource.Table(table_name)

    res = table.get_item(Key={'uid': uid},
                         ConsistentRead=False)
    if 'Item' not in res or not res['Item']:  # not sure exactly what happens
        message = 'No user data located in {} table'.format(table_name)
        raise Exception(message)

    return res['Item']


def _read_benefits_from_s3(s3_bucket, aws_options):
    return read_json(s3_bucket, BENEFITS_PATH, aws_options)


def _read_benefits_from_dict():
    # from benefits_2018 import MA_PLANS  # importing here is much slower
    return MA_PLANS
    

def _filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    # TODO: should we short claims by admitted????
    # return sorted(filtered_claims, key=lambda claim: claim['admitted'])

    return filtered_claims


def _calculate_batch(person, plans, claim_year, fips_code, months, cost_map):
    claims = person.get('medical_claims', [])

    cost_items = []
    for start_month in (str(month).zfill(2) for month in months):
        claims_to_process = _filter_and_sort_claims(claims, claim_year, start_month)

        oops = {}
        for plan in plans:
            costs = calculate_oop(claims_to_process, plan, force_network='in_network',
                                  truncate_claims_at_year_boundary=False)
            oops[str(plan['picwell_id'])] = costs['oop']

        cost_items.append({
            'month': start_month,
            'uid': person['uid'],
            'state': fips_code,
            'oops': oops
        })

    # Write to cost map:
    cost_map.add_items(cost_items)


def _run_batch(person, plans, claim_year, run_options, table_name, aws_options, start_time):
    cost_map = DynamoDBCostMap(table_name=table_name, aws_options=aws_options)

    # Read states and propration periods to consider. If not given use default values (all
    # states among the plans and all proration periods, respectively).
    months = run_options.get('months',
                             [str(month + 1).zfill(2) for month in range(12)])
    states = set(run_options.get('states',
                                 (plan['state_fips'] for plan in plans)))

    setup_elapsed = (datetime.now() - start_time).total_seconds()
    logger.info('Total setup took {} seconds.'.format(setup_elapsed) +
                'Start calculation for batch processing:')

    for state in states:
        plans_for_state = filter(lambda plan: plan['state_fips'] == state, plans)

        if plans_for_state:
            _calculate_batch(person, plans_for_state, claim_year, state, months, cost_map)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info('Clock stopped at {}. Elapsed: {}'.format(str(end_time), str(elapsed)))

    return _succeed_with_message('batch calculation complete: {}'.format(elapsed))


def _calculate_detail(person, plans, claim_year, month):
    claims = person.get('medical_claims', [])
    claims_to_process = _filter_and_sort_claims(claims, claim_year, month)

    costs = []
    for plan in plans:
        cost = calculate_oop(claims_to_process, plan, force_network='in_network',
                             truncate_claims_at_year_boundary=False)

        cost['uid'] = person['uid']
        cost['picwell_id'] = plan['picwell_id']

        costs.append(cost)

    return costs


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
            person = _read_claims_from_s3(uid, config_values.claims_bucket, aws_options)
        else:
            person = _read_claims_from_dynamodb(uid, config_values.dynamodb_claim_table,
                                                aws_options)
    except Exception as e:
        logger.error(e.message)
        return _fail_with_message(e.message)

    claim_elapsed = (datetime.now() - claim_time).total_seconds()
    logger.info('Finished retrieving claims for {} in {} seconds.'.format(uid, claim_elapsed))

    # look up plans from s3
    logger.info('Retrieving benefits file...')
    benefit_time = datetime.now()

    try:
        # plans = _read_benefits_from_s3(config_values.benefit_bucket, aws_options)
        plans = _read_benefits_from_dict()
    except Exception as e:
        logger.error(e.message)
        return _fail_with_message(e.message)

    benefit_elapsed = (datetime.now() - benefit_time).total_seconds()
    logger.info('Finished retrieving benefits file in {} seconds.'.format(benefit_elapsed))

    return _run_batch(person, plans, config_values.claims_year, run_options,
                      config_values.dynamodb_cost_table, aws_options, start_time)


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
        'uid': '764308502',
    }

    main(run_options, aws_options)
