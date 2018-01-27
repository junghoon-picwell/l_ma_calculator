import collections
import logging

from calc.calculator import calculate_oop
from cost_map import DynamoDBCostMap
from package_utils import (
    filter_and_sort_claims,
    uids_need_to_be_split,
    split_uids_into_groups,
    get_costs_from_lambdas,
)
from lambda_package.shared_utils import (
    TimeLogger,
)

logger = logging.getLogger()


def _run_calculator(people, plans, claim_year, fips_code, months):
    cost_items = []
    for person in people:
        # TODO: should we inflate claims?
        claims = person.get('medical_claims', [])

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


def _calculate_costs(benefits_client, claims_client, run_options, uids, claim_year):
    """
    Read states and propration periods to consider. If not given use default values (all
    states among the plans and all proration periods, respectively).
    """
    states = run_options.get('states', benefits_client.all_states)

    months = run_options.get('months', (month + 1 for month in range(12)))
    months = [str(month).zfill(2) for month in months]

    # look up claims:
    message = 'Claim retrieval for {}'.format(uids) + ' took {elapsed} seconds.'
    with TimeLogger(logger, end_message=message):
        people = claims_client.get(uids)

    cost_items = []
    for state in states:
        logger.info('Processing state {}:'.format(state))

        # look up plans from s3
        message = 'Benefit retrieval for {}'.format(state) + ' took {elapsed} seconds.'
        with TimeLogger(logger, end_message=message):
            plans = benefits_client.get_by_state([state])

        if plans:
            message = 'Calculation for {}'.format(state) + ' took {elapsed} seconds.'
            with TimeLogger(logger, end_message=message):
                cost_items += _run_calculator(people, plans, claim_year, state, months)

    return cost_items


def _save_costs(cost_items, table_name, aws_options):
    with TimeLogger(logger,
                    end_message='Write to DynamoDB took {elapsed} seconds.'):
        cost_map = DynamoDBCostMap(table_name=table_name, aws_options=aws_options)
        cost_map.add_items(cost_items)


def _count_items_by_uid(cost_items):
    """
    :return: dict mapping UIDs to counts of the times that UID occurred
    """
    # Count the number of items by uid:
    count_dict = collections.defaultdict(int)
    for cost_item in cost_items:
        count_dict[cost_item['uid']] += 1
    return count_dict


def run_batch(claims_client, benefits_client, claim_year, run_options,
              max_calculated_uids, max_lambda_calls,
              table_name, aws_options):
    """
    {
        'uids': List[UID],
        'states': List[2-digit strings] (optional),
        'months': List[2-digit strings] (optional),
    }
    """
    if 'uids' not in run_options:
        raise Exception('Missing "uids".')
    uids = run_options['uids']

    if uids_need_to_be_split(uids, max_calculated_uids):
        uid_groups = split_uids_into_groups(uids, max_calculated_uids, max_lambda_calls)
        logger.info('{} uids are broken into {} groups'.format(len(uids), len(uid_groups)))

        return get_costs_from_lambdas(uid_groups, run_options)

    else:
        # Calculate and write to DynamoDB:
        #
        # Write should happen in the terminal Lambdas since only the item counts by UID
        # are returned.
        cost_items = _calculate_costs(benefits_client, claims_client, run_options, uids, claim_year)
        _save_costs(cost_items, table_name, aws_options)

        count_dict = _count_items_by_uid(cost_items)
        return [(uid, count) for uid, count in count_dict.iteritems()]
