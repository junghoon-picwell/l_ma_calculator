#!/usr/bin/env python3
from __future__ import print_function
import json
import logging
import time

from datetime import timedelta
from s3_helpers import read_json
from config_info import ConfigInfo
from cost_map import DynamoDBCostMap
from calc.calculator import calculate_oop

CLAIMS_PATH = 'junghoon/lambda_calculator'
BENEFITS_PATH = 'ma_benefits/cms_2018_pbps_20171005.json'
CONFIG_FILE_NAME = 'calculator.cfg'


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


def _calculate_for_all_plans(person, plans, claim_year, fips_code, months, cost_map):
    claims = person.get('medical_claims', [])

    cost_items = []
    for start_month in (str(month).zfill(2) for month in months):
        start_date = '{}-{}-01'.format(claim_year, start_month)
        end_date = '{}-12-31'.format(claim_year)

        # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by the spark calculator)
        # uses `discharged`, so using `discharged` for consistency.
        filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

        oops = {}
        for plan in plans:
            costs = calculate_oop(filtered_claims, plan, force_network='in_network',
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


def main(run_options, aws_options):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logging.basicConfig()

    start = time.clock()
    config_values = ConfigInfo(CONFIG_FILE_NAME)

    if not run_options.has_key('uid'):
        return {
            'message': 'missing "uid"',
        }
    months = ['01']

    user_id = run_options['uid']

    cost_map = DynamoDBCostMap(table_name=config_values.dynamo_db_table, aws_options=aws_options)

    # look up claims from s3 for user
    logger.info('Retrieving claims for {}'.format(user_id))
    file_name = CLAIMS_PATH + '/{}.json'.format(user_id)
    user_data = read_json(config_values.claims_bucket, file_name)

    if len(user_data) == 0:
        missing_user_message = 'No user data located at {}/{}'.format(config_values.claims_bucket, file_name)
        logger.error(missing_user_message)
        return _fail_with_message(missing_user_message)

    person = user_data[0]

    # look up plans from s3
    logger.info('Retrieving benefits file')
    plans = read_json(config_values.benefit_bucket, BENEFITS_PATH)
    logger.info('Finished retrieving benefits file')

    # get FIPS for plans

    if 'fips' not in run_options or len(run_options['fips']) == 0:
        fips = {plan['state_fips'] for plan in plans}
    else:
        fips = set(run_options['fips'])

    logger.info('Start calculation for all states:')
    for single_state in fips:
        plans_for_state = filter(lambda plan: plan['state_fips'] == single_state, plans)

        if plans_for_state:
            _calculate_for_all_plans(person, plans_for_state, config_values.claims_year, single_state, months, cost_map)

    end = time.clock() - start
    elapsed = timedelta(seconds=end)
    logger.info('Finished calculation for all states. Elapsed: {}'.format(elapsed))
    return _succeed_with_message('calculation complete: {}'.format(elapsed))


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
    run_options = {}

    operations = {
        'GET': lambda: main(run_options, aws_options)
    }

    operation = event['httpMethod']

    if operation in operations:
        payload = event['queryStringParameters'] if operation == 'GET' else json.loads(event['body'])
        run_options['uid'] = payload['uid']
        run_options['fips'] = payload.get('states', [])
        res = operations[operation]()
        if res['statusCode'] != '200':
            return respond(ValueError(res['message']), res['message'])

        return respond(None, res)

    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))


if __name__ == '__main__':
    # This is the entry point of this code when run locally. In general, this is
    # run as a lambda, where the `main()` function is directly called.

    from argparse import ArgumentParser

    parser = ArgumentParser(description="Medicare Advantage On-Demand OOP Cost Calculator")
    args = parser.parse_args()

    aws_options = {
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'region_name': '',
        'profile_name': None
    }
    run_options = {
        'fips': [],
        'uid': '764308502'
    }

    main(run_options, aws_options)
