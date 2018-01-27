import boto3
import json
import logging
import random
import time

from lambda_package.shared_utils import (
    RandomStateProtector,
    MAX_THREADS,
    ThreadPool,
)

_MAX_CLIENT_TRIES = 10
_MAX_LAMBDA_TRIES = 7

logger = logging.getLogger()


def message_api_gateway(status_code, body):
    # An object of this format should be used when results are returned to the API
    # gateway when Lambda Proxy is used. Otherwise, the API gateway will issue a
    # 502 error "malformed Lambda proxy response".
    #
    # See
    # https://aws.amazon.com/premiumsupport/knowledge-center/malformed-502-api-gateway/
    # https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html
    return {
        'statueCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        'isBase64Encoded': False,  # not sure whether this is correct
        'body': body,
    }


def filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    return filtered_claims


def uids_need_to_be_split(uids, max_uids):
    return len(uids) > max_uids


def split_uids_into_groups(uids, max_calculated_uids, max_lambda_calls):
    # TODO: there may be a better way to do this:
    if len(uids) < max_calculated_uids*max_lambda_calls:
        # Try to minimize AWS Lambda calls:
        return [uids[start:(start + max_calculated_uids)]
                for start in xrange(0, len(uids), max_calculated_uids)]

    else:
        # Distribute the uids into max_lambda_call "buckets":
        return [uids[start::max_lambda_calls] for start in range(max_lambda_calls)]


def call_lambda_again(uids, run_options):
    # This can fail. However, I am not sure whether I want to follow the advice in
    # this post:
    #
    # https://github.com/boto/boto3/issues/801
    #
    # I think this also shows that boto3 is not really ready for multi-threading.
    client = None
    tries = 0
    while not client and tries < _MAX_CLIENT_TRIES:
        try:
            client = boto3.client('lambda')
        except KeyError:
            pass

        tries += 1

    if not client:
        logger.info('Boto3 client cannot be created even after {} tries.'.format(_MAX_CLIENT_TRIES))
        return []

    request = {
        'httpMethod': 'GET',
        'queryStringParameters': run_options.copy(),
    }
    request['queryStringParameters']['uids'] = uids
    encoded_payload = bytes(json.dumps(request)).encode('utf-8')

    with RandomStateProtector():
        tries = 0
        while tries < _MAX_LAMBDA_TRIES:
            response = client.invoke(
                FunctionName='ma_calculator',
                InvocationType='RequestResponse',
                LogType='None',
                Payload=encoded_payload,
            )

            if response['StatusCode'] == 200 and 'FunctionError' not in response:
                break

            tries += 1

            # No delay is needed for the last iteration:
            if tries < _MAX_LAMBDA_TRIES:
                # Exponential delay:
                max_sleep_time = 2.0 ** tries / 100.0  # start with 20 ms delay
                sleep_time = random.uniform(0, max_sleep_time)

                logger.info('Retrying after {} seconds.'.format(sleep_time))

                time.sleep(sleep_time)

    if tries < _MAX_LAMBDA_TRIES:
        return json.loads(response['Payload'].read())
    else:
        # Give up a few people after maximum number of retires:
        logger.info('Giving up after {} tries.'.format(_MAX_LAMBDA_TRIES))
        return []


def get_costs_from_lambdas(uid_groups, run_options):
    """
    Calls the MA Calculator lambda again, once per group in uid_groups.

    :return: Sum of costs from lambda calls
    """

    with TimeLogger(logger,
                    end_message='Distribution took {elapsed} seconds.'):
        pool = ThreadPool(MAX_THREADS)
        cost_groups = pool.imap(lambda uid_group: call_lambda_again(uid_group, run_options), uid_groups)

    return sum(cost_groups, [])

