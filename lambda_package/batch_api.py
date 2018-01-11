import datetime
import logging

from calc.calculator import calculate_oop
from cost_map import DynamoDBCostMap
from utils import (
    filter_and_sort_claims,
    fail_with_message,
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
    logger.info('Retrieving claims for {}...'.format(uid))
    claim_time = datetime.datetime.now()

    try:
        # TODO: the claims need to be infalted!
        person = claims_client.get([uid])[0]

    except Exception as e:
        logger.error(e.message)
        return fail_with_message(e.message)

    claim_elapsed = (datetime.datetime.now() - claim_time).total_seconds()
    logger.info('Finished retrieving claims for {} in {} seconds.'.format(uid, claim_elapsed))

    logger.info('Establishing DynamoDB connection...')
    db_setup_time = datetime.datetime.now()

    cost_map = DynamoDBCostMap(table_name=table_name, aws_options=aws_options)

    db_setup_elapsed = (datetime.datetime.now() - db_setup_time).total_seconds()
    logger.info('Established connection in {} seconds.'.format(db_setup_elapsed))

    cost_items = []
    for state in states:
        logger.info('Processing state {}...'.format(state))

        # look up plans from s3
        logger.info('Retrieving benefits...'.format(state))
        benefit_time = datetime.datetime.now()

        try:
            plans = benefits_client.get_by_state([state])

        except Exception as e:
            logger.error(e.message)
            return fail_with_message(e.message)

        benefit_elapsed = (datetime.datetime.now() - benefit_time).total_seconds()
        logger.info('Retrieved benefits in {} seconds.'.format(benefit_elapsed))

        if plans:
            logger.info('Calculating...'.format(state))
            compute_time = datetime.datetime.now()

            cost_items += _calculate_batch(person, plans, claim_year, state, months)

            compute_elapsed = (datetime.datetime.now() - compute_time).total_seconds()
            logger.info('Calculation done in {} seconds.'.format(compute_elapsed))

    logger.info('Writing to DynamoDB...'.format(state))
    write_time = datetime.datetime.now()

    cost_map.add_items(cost_items)

    write_elapsed = (datetime.datetime.now() - write_time).total_seconds()
    logger.info('Write done in {} seconds.'.format(write_elapsed))

    return uid