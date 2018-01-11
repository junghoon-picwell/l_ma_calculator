import logging

from calc.calculator import calculate_oop
from package_utils import (
    fail_with_message,
    filter_and_sort_claims,
)
from shared_utils import TimeLogger

logger = logging.getLogger()


def _calculate_breakdown(person, plans, claim_year, month):
    # TODO: should we inflate claims?
    claims = person.get('medical_claims', [])
    claims_to_process = filter_and_sort_claims(claims, claim_year, month)

    costs = []
    for plan in plans:
        cost = calculate_oop(claims_to_process, plan, force_network='in_network',
                             truncate_claims_at_year_boundary=False)

        cost.update({
            'uid': person['uid'],
            'picwell_id': str(plan['picwell_id']),
        })

        costs.append(cost)

    return costs


def run_breakdown(claims_client, benefits_client, claim_year, run_options):
    if 'uid' not in run_options:
        return {
            'statusCode': '400',
            'message': 'missing "uid"',
        }
    uid = run_options['uid']

    # Use the full year if the proration period is not specified:
    month = str(run_options.get('month', 1)).zfill(2)

    # look up claims:
    message = 'Claim retrieval for {}'.format(uid) + ' took {elapsed} seconds.'
    with TimeLogger(logger, end_message=message):
        try:
            # TODO: the claims need to be infalted!
            person = claims_client.get([uid])[0]

        except Exception as e:
            logger.error(e.message)
            return fail_with_message(e.message)

    # look up plans from s3
    with TimeLogger(logger,
                    end_message='Benefit retrieval took {elapsed} seconds.'):
        try:
            plans = benefits_client.get_by_pid(run_options['pids'])

        except Exception as e:
            logger.error(e.message)
            return fail_with_message(e.message)

    with TimeLogger(logger,
                    end_message='Calculation took {elapsed} seconds.'):
        costs = _calculate_breakdown(person, plans, claim_year, month)

    return costs