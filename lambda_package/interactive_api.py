import logging

from calc.calculator import calculate_oop
from package_utils import (
    filter_and_sort_claims,
    uids_need_to_be_split,
    split_uids_into_groups,
    get_costs_from_lambdas,
)
from shared_utils import (
    TimeLogger,
)

logger = logging.getLogger()


def _run_calculator(people, plans, claim_year, month, oop_only):
    costs = []
    for person in people:
        # TODO: should we inflate claims?
        claims = person.get('medical_claims', [])
        claims_to_process = filter_and_sort_claims(claims, claim_year, month)

        for plan in plans:
            cost = calculate_oop(claims_to_process, plan, force_network='in_network',
                                 truncate_claims_at_year_boundary=False)
            if oop_only:
                cost = {'oop': cost['oop']}

            cost.update({
                'uid': person['uid'],
                'picwell_id': str(plan['picwell_id']),
            })

            costs.append(cost)

    return costs


def _calculate_costs(benefits_client, claims_client, run_options, uids, pids, claim_year, oop_only):
    # Use the full year if the proration period is not specified:
    month = str(run_options.get('month', 1)).zfill(2)

    # look up claims:
    message = 'Claim retrieval for {}'.format(uids) + ' took {elapsed} seconds.'
    with TimeLogger(logger, end_message=message):
        # TODO: the claims need to be inflated!
        people = claims_client.get(uids)

    # look up plans from s3
    with TimeLogger(logger,
                    end_message='Benefit retrieval took {elapsed} seconds.'):
        plans = benefits_client.get_by_pid(pids)

    with TimeLogger(logger,
                    end_message='Calculation took {elapsed} seconds.'):
        costs = _run_calculator(people, plans, claim_year, month, oop_only)

    return costs


def run_interactive(claims_client, benefits_client, claim_year, run_options,
                    max_calculated_uids, max_lambda_calls, oop_only):
    """
    The run_options looks like
    {
        'uids': List[UID],
        'pids': List[PID],
        'month': 2-digit string (optional),
    }
    """
    if 'uids' not in run_options:
        raise Exception('Missing "uids".')
    uids = run_options['uids']

    if 'pids' not in run_options:
        raise Exception('Missing "pids".')
    pids = run_options['pids']

    if uids_need_to_be_split(uids, max_calculated_uids):
        uid_groups = split_uids_into_groups(uids, max_calculated_uids, max_lambda_calls)
        logger.info('{} uids are broken into {} groups'.format(len(uids), len(uid_groups)))

        return get_costs_from_lambdas(uid_groups, run_options)

    else:
        costs = _calculate_costs(benefits_client, claims_client, run_options, uids, pids,
                                 claim_year, oop_only)
        return costs
