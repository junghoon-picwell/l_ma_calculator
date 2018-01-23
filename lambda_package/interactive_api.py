import logging

from calc.calculator import calculate_oop
from package_utils import (
    filter_and_sort_claims,
    distribute_uids,
    call_itself,
)
from shared_utils import (
    MAX_THREADS,
    ThreadPool,
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

    if len(uids) > max_calculated_uids:
        uid_groups = distribute_uids(uids, max_calculated_uids, max_lambda_calls)
        logger.info('{} uids are broken into {} groups'.format(len(uids), len(uid_groups)))

        with TimeLogger(logger,
                        end_message='Distribution took {elapsed} seconds.'):
            pool = ThreadPool(MAX_THREADS)
            cost_groups = pool.imap(lambda uids: call_itself(uids, run_options), uid_groups)

        return sum(cost_groups, [])

    else:
        # Use the full year if the proration period is not specified:
        month = str(run_options.get('month', 1)).zfill(2)

        # look up claims:
        message = 'Claim retrieval for {}'.format(uids) + ' took {elapsed} seconds.'
        with TimeLogger(logger, end_message=message):
            # TODO: the claims need to be infalted!
            people = claims_client.get(uids)

        # look up plans from s3
        with TimeLogger(logger,
                        end_message='Benefit retrieval took {elapsed} seconds.'):
            plans = benefits_client.get_by_pid(pids)

        with TimeLogger(logger,
                        end_message='Calculation took {elapsed} seconds.'):
            costs = _run_calculator(people, plans, claim_year, month, oop_only)

        return costs
