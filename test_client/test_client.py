from __future__ import absolute_import

import json
import boto3

from .invocation_types import InvocationType


class LambdaCalculatorTestClient(object):

    def __init__(self, aws_options=None):
        if aws_options is None:
            self._client = boto3.client('lambda')
        else:
            session = boto3.Session(**aws_options)
            self._client = session.client('lambda')

    def _calculate_with_invocation_type(self, uid, months_list, states_list, invocation_type):
        request = {
            "httpMethod": "GET",
            "queryStringParameters": {
                "uid": uid,
            }
        }

        if len(months_list) > 0:
            request['months'] = months_list

        if len(states_list) > 0:
            request['states'] = states_list

        encoded_payload = bytes(json.dumps(request)).encode('utf-8')

        response = self._client.invoke(
            FunctionName='ma_calculator',
            InvocationType=invocation_type,
            # "Event" makes this async. Options are 'Event' | 'RequestResponse' | 'DryRun'
            LogType='None',  # Only used if InvocationType is 'RequestResponse'.  If so, you can set this to 'Tail'
            Payload=encoded_payload,
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
        return response

    def calculate_sync(self, uid, months_list, states_list):
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
        return self._calculate_with_invocation_type(uid, months_list=months_list, states_list=states_list, invocation_type=InvocationType.RequestResponse)

    def calculate_async(self, uid, months_list, states_list):
        return self._calculate_with_invocation_type(uid, months_list=months_list, states_list=states_list, invocation_type=InvocationType.Event)
