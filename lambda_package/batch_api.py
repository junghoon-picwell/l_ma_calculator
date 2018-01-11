import logging

from calc.calculator import calculate_oop
from cost_map import DynamoDBCostMap
from utils import (
    filter_and_sort_claims,
    fail_with_message,
    TimeLogger,
)

logger = logging.getLogger()


def _calculate_batch(person, plans, claim_year, fips_code, months):
    # TODO: should we inflate claims?
    claims = person.get('medical_claims', [])

    cost_items = []
    for start_month in (str(month).zfill(2) for month in months):
        claims_to_process = filter_and_sort_claims(claims, claim_year, start_month)

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

    return cost_items


def run_batch(claims_client, benefits_client, claim_year, run_options, table_name, aws_options):
    if 'uid' not in run_options:
        return {
            'statusCode': '400',
            'message': 'missing "uid"',
        }
    uid = run_options['uid']

    # Read states and propration periods to consider. If not given use default values (all
    # states among the plans and all proration periods, respectively).
    states = run_options.get('states', benefits_client.all_states)

    months = run_options.get('months', (month + 1 for month in range(12)))
    months = [str(month).zfill(2) for month in months]

    # look up claims:
    message = 'Claim retrieval for {}'.format(uid) + ' took {elapsed} seconds.'
    with TimeLogger(logger, end_message=message):
        try:
            # TODO: the claims need to be infalted!
            person = claims_client.get([uid])[0]

        except Exception as e:
            logger.error(e.message)
            return fail_with_message(e.message)

    with TimeLogger(logger,
                    end_message='Establishing connection to DynamoDB took {elapsed} seconds.'):
        cost_map = DynamoDBCostMap(table_name=table_name, aws_options=aws_options)

    cost_items = []
    for state in states:
        logger.info('Processing state {}:'.format(state))

        # look up plans from s3
        message = 'Benefit retrieval for {}'.format(state) + ' took {elapsed} seconds.'
        with TimeLogger(logger, end_message=message):
            try:
                plans = benefits_client.get_by_state([state])

            except Exception as e:
                logger.error(e.message)
                return fail_with_message(e.message)

        if plans:
            message = 'Calculation for {}'.format(state) + ' took {elapsed} seconds.'
            with TimeLogger(logger, end_message=message):
                cost_items += _calculate_batch(person, plans, claim_year, state, months)

    with TimeLogger(logger,
                    end_message='Write to DynamoDB took {elapsed} seconds.'):
        cost_map.add_items(cost_items)

    return uid