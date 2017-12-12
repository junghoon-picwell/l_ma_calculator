import boto3

from s3_helpers import read_json

CLAIMS_PATH = 'junghoon/lambda_calculator'
BENEFITS_PATH = 'ma_benefits/cms_2018_pbps_20171005.json'


def succeed_with_message(message):
    return {
        'statusCode': '200',
        'message': message
    }


def fail_with_message(message):
    return {
        'statusCode': '500',
        'message': message
    }


def filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    # TODO: should we short claims by admitted????
    # return sorted(filtered_claims, key=lambda claim: claim['admitted'])

    return filtered_claims


# TODO: expose these functions as part of the client interface?
def read_claims_from_s3(uid, s3_bucket, aws_options):
    file_name = CLAIMS_PATH + '/{}.json'.format(uid)
    user_data = read_json(s3_bucket, file_name, aws_options)

    if not user_data:
        message = 'No user data located at {}/{}'.format(s3_bucket, file_name)
        raise Exception(message)

    return user_data[0]


def read_claims_from_dynamodb(uid, table_name, aws_options):
    session = boto3.Session(**aws_options)
    resource = session.resource('dynamodb')
    table = resource.Table(table_name)

    res = table.get_item(Key={'uid': uid},
                         ConsistentRead=False)
    if 'Item' not in res or not res['Item']:  # not sure exactly what happens
        message = 'No user data located in {} table'.format(table_name)
        raise Exception(message)

    return res['Item']


def read_benefits_from_s3(s3_bucket, aws_options):
    return read_json(s3_bucket, BENEFITS_PATH, aws_options)