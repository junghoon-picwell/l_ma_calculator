import json
import pytest
from ma_calculator_wrapper import main

import boto3
from ma_calculator_wrapper import main


def _get_boto_client():
    return boto3.client('lambda')


def test_for_user_ids():
    client = _get_boto_client()

    request = {
        "httpMethod": "GET",
        "queryStringParameters": {
            "uid": "1009901"
        }
    }

    response = client.invoke(
        FunctionName='ma_calculator',
        InvocationType='RequestResponse',  # "Event" makes this async. Options are 'Event' | 'RequestResponse' | 'DryRun'
        LogType='None',  # Only used if InvocationType is 'RequestResponse'.  If so, you can set this to 'Tail'
        ClientContext='Test Client',  # not needed
        Payload=bytes(json.dumps(request)),
        # JSON that you want to provide to your Lambda function as input.
        # Example: 'fileb://file-path/input.json',
        # Qualifier='string'
        # You can use this optional parameter to specify a Lambda function version or alias name. If you specify a
        # function version, the API uses the qualified function ARN to invoke a specific Lambda function. If you
        # specify an alias name, the API uses the alias ARN to invoke the Lambda function version to which the alias
        # points.
        # If you don't provide this parameter, then the API uses unqualified function ARN which results in
        # invocation
        #  of the $LATEST version.
    )

    # result = main(run_options, aws_options)

    '''
    Response:

    {
        'StatusCode': 123,
        'FunctionError': 'string',
        'LogResult': 'string',
        'Payload': StreamingBody(),
        'ExecutedVersion': 'string'
    }
    '''

    print response
    assert response['statusCode'] == '202'
