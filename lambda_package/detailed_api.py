import json
from datetime import datetime

from calc.calculator import calculate_oop
from utils import (
    succeed_with_message,
    filter_and_sort_claims,
)


def _calculate_detail(person, plans, claim_year, month):
    # TODO: should we inflate claims?
    claims = person.get('medical_claims', [])
    claims_to_process = filter_and_sort_claims(claims, claim_year, month)

    costs = []
    for plan in plans:
        cost = calculate_oop(claims_to_process, plan, force_network='in_network',
                             truncate_claims_at_year_boundary=False)

        cost['uid'] = person['uid']
        cost['picwell_id'] = str(plan['picwell_id'])

        costs.append(cost)

    return costs


def run_detailed(person, benefits_client, claim_year, run_options, logger, start_time):
    # If no pids is given, run for all available plans:
    if 'pids' in run_options:
        plans = benefits_client.get_by_pid(run_options['pids'])

    else:
        plans = benefits_client.get_all()

    # Use the full year if the proration period is not specified:
    month = str(run_options.get('month', 1)).zfill(2)

    setup_elapsed = (datetime.now() - start_time).total_seconds()
    logger.info('Total setup took {} seconds.'.format(setup_elapsed) +
                'Start calculation to return full calculation results:')

    costs = _calculate_detail(person, plans, claim_year, month)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info('Clock stopped at {}. Elapsed: {}'.format(str(end_time), str(elapsed)))

    return succeed_with_message(json.dumps(costs))