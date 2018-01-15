import boto3
import json
import logging
import random
import time

from calc.calculator import calculate_oop
from package_utils import (
    filter_and_sort_claims,
)
from shared_utils import (
    MAX_THREADS,
    ThreadPool,
    TimeLogger,
)

_MAX_RETRIES = 7
_RETRY_DELAY = 0.1

# Both should be less than MAX_THREADS used for ThreadPool:
_MAX_CALCULATED_UIDS = min(1, MAX_THREADS)
_MAX_LAMBDA_CALLS = min(10, MAX_THREADS)

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


def _distribute_uids(uids, max_calculated_uids, max_lambda_calls):
    # TODO: there may be a better way to do this:
    if len(uids) < max_calculated_uids*max_lambda_calls:
        # Try to minimize AWS Lambda calls:
        return [uids[start:(start + max_calculated_uids)]
                for start in xrange(0, len(uids), max_calculated_uids)]

    else:
        # Distribute the uids into max_lambda_call "buckets":
        return [uids[start::max_lambda_calls] for start in range(max_lambda_calls)]


def _call_itself(uids, run_options):
    # This can fail. However, I am not sure whether I want to follow the advice in
    # this post:
    #
    # https://github.com/boto/boto3/issues/801
    #
    # I think this also shows that boto3 is not really ready for multi-threading.
    client = None
    while not client:
        try:
            client = boto3.client('lambda')
        except:
            client = None

    request = {
        'httpMethod': 'GET',
        'queryStringParameters': run_options.copy(),
    }
    request['queryStringParameters']['uids'] = uids
    encoded_payload = bytes(json.dumps(request)).encode('utf-8')

    retries = 0
    while retries < _MAX_RETRIES:
        response = client.invoke(
            FunctionName='ma_calculator',
            InvocationType='RequestResponse',
            LogType='None',
            Payload=encoded_payload,
        )

        if response['StatusCode'] == 200 and 'FunctionError' not in response:
            break

        retries += 1

        # No delay is needed for the last iteration:
        if retries < _MAX_RETRIES:
            # Exponential delay:
            max_sleep_time = 2.0 ** retries / 100.0  # start with 20 ms delay
            time.sleep(random.uniform(0, max_sleep_time))

    if retries < _MAX_RETRIES:
        return json.loads(response['Payload'].read())
    else:
        # Give up a few people after maximum number of retires:
        logger.info('Giving up after {} retires.'.format(_MAX_RETRIES))
        return []


def run_breakdown(claims_client, benefits_client, claim_year, run_options):
    """
    The run_options looks like
    {
        'uids': List[UID],
        'pids': List[PID],
        'months': 2-digit string (optional),
        'max_calculated_uids': int (optional),
        'max_lambda_calls: int (optional),
    }
    """
    if 'uids' not in run_options:
        raise Exception('Missing "uids".')
    uids = run_options['uids']

    if 'pids' not in run_options:
        raise Exception('Missing "pids".')
    pids = run_options['pids']

    max_calculated_uids = run_options.get('max_calculated_uids', _MAX_CALCULATED_UIDS)
    if len(uids) > max_calculated_uids:
        max_lambda_calls = run_options.get('max_lambda_calls', _MAX_LAMBDA_CALLS)
        uid_groups = _distribute_uids(uids, max_calculated_uids, max_lambda_calls)

        logger.info('{} uids are broken into {} groups'.format(len(uids), len(uid_groups)))

        with TimeLogger(logger,
                        end_message='Distribution took {elapsed} seconds.'):
            pool = ThreadPool(MAX_THREADS)
            cost_groups = pool.imap(lambda uids: _call_itself(uids, run_options), uid_groups)

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
            costs = _calculate_breakdown(people, plans, claim_year, month)

    return costs
