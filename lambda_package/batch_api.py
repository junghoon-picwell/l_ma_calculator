from datetime import datetime

from calc.calculator import calculate_oop
from cost_map import DynamoDBCostMap
from utils import (
    filter_and_sort_claims,
    succeed_with_message,
)


def _calculate_batch(person, plans, claim_year, fips_code, months, cost_map):
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

    # Write to cost map:
    cost_map.add_items(cost_items)


def run_batch(person, plans, claim_year, run_options, table_name, aws_options, logger, start_time):
    cost_map = DynamoDBCostMap(table_name=table_name, aws_options=aws_options)

    # Read states and propration periods to consider. If not given use default values (all
    # states among the plans and all proration periods, respectively).
    months = run_options.get('months', (month + 1 for month in range(12)))
    months = [str(month).zfill(2) for month in months]

    states = set(run_options.get('states',
                                 (plan['state_fips'] for plan in plans)))
    states = [str(state) for state in states]

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

    return succeed_with_message('batch calculation complete: {}'.format(elapsed))