import boto3
import json
import logging

from calc.calculator import calculate_oop
from package_utils import (
    message_failure,
    filter_and_sort_claims,
)
from shared_utils import (
    MAX_THREADS,
    ThreadPool,
    TimeLogger,
)

# Both should be less than MAX_THREADS used for ThreadPool:
_MAX_UIDS_TO_CALCULATE = min(10, MAX_THREADS)
_MAX_LAMBDA_CALLS = min(20, MAX_THREADS)

logger = logging.getLogger()


def _calculate_breakdown(people, plans, claim_year, month):
    costs = []
    for person in people:
        # TODO: should we inflate claims?
        claims = person.get('medical_claims', [])
        claims_to_process = filter_and_sort_claims(claims, claim_year, month)

        for plan in plans:
            cost = calculate_oop(claims_to_process, plan, force_network='in_network',
                                 truncate_claims_at_year_boundary=False)

            cost.update({
                'uid': person['uid'],
                'picwell_id': str(plan['picwell_id']),
            })

            costs.append(cost)

    return costs


def _distribute_uids(uids, max_uids_to_calculate, max_lambda_calls):
    # TODO: there may be a better way to do this:
    if len(uids) < max_uids_to_calculate*max_lambda_calls:
        # Try to minimize AWS Lambda calls:
        return (uids[start:(start + max_uids_to_calculate)]
                for start in xrange(0, len(uids), max_uids_to_calculate))

    else:
        # Distribute the uids into max_lambda_call "buckets":
        return (uids[start::max_lambda_calls] for start in range(max_lambda_calls))


def _call_itself(uids, run_options):
    request = {
        'httpMethod': 'GET',
        'queryStringParameters': run_options.copy(),
    }
    request['queryStringParameters']['uids'] = uids

    client = boto3.client('lambda')

    encoded_payload = bytes(json.dumps(request)).encode('utf-8')
    response = client.invoke(
        FunctionName='ma_calculator',
        InvocationType='RequestResponse',
        LogType='None',
        Payload=encoded_payload,
    )

    # Only collect successful calculations:
    if response['StatusCode'] == 200:
        calculator_response = json.loads(response['Payload'].read())
        if calculator_response['StatusCode'] == 200:
            return calculator_response['Return']

    return []


def run_breakdown(claims_client, benefits_client, claim_year, run_options):
    """
    The run_options looks like
    {
        'uids': List[UID],
        'pids': List[PID],
        'months': 2-digit string (optional),
        'max_uids_to_calculate': int (optional),
        'max_lambda_calls: int (optional),
    }
    """
    if 'uids' not in run_options:
        return message_failure('Missing "uids".')
    uids = run_options['uids']

    max_uids_to_calculate = run_options.get('max_uids_to_calculate', _MAX_UIDS_TO_CALCULATE)
    if len(uids) > max_uids_to_calculate:
        max_lambda_calls = run_options.get('max_lambda_calls', _MAX_LAMBDA_CALLS)
        uid_groups = _distribute_uids(uids, max_uids_to_calculate, max_lambda_calls)

        with TimeLogger(logger,
                        end_message='Distribution took {elapsed} seconds.'):
            pool = ThreadPool(MAX_THREADS)
            cost_groups = pool.imap(lambda uids: _call_itself(uids, run_options), uid_groups)

        return sum(cost_groups, [])

    else:
        if 'pids' not in run_options:
            return message_failure('Missing "pids".')
        pids = run_options['pids']

        # Use the full year if the proration period is not specified:
        month = str(run_options.get('month', 1)).zfill(2)

        # look up claims:
        message = 'Claim retrieval for {}'.format(uids) + ' took {elapsed} seconds.'
        with TimeLogger(logger, end_message=message):
            try:
                # TODO: the claims need to be infalted!
                people = claims_client.get(uids)

            except Exception as e:
                logger.error(e.message)
                return message_failure(e.message)

        # look up plans from s3
        with TimeLogger(logger,
                        end_message='Benefit retrieval took {elapsed} seconds.'):
            try:
                plans = benefits_client.get_by_pid(pids)

            except Exception as e:
                logger.error(e.message)
                return message_failure(e.message)

        with TimeLogger(logger,
                        end_message='Calculation took {elapsed} seconds.'):
            costs = _calculate_breakdown(people, plans, claim_year, month)

    return costs
