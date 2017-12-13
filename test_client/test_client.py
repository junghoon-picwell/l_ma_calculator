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

    def _calculate_with_invocation_type(self, uid, months, states, invocation_type):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'batch',
                'uid': uid,
            }
        }

        if months:
            request['queryStringParameters']['months'] = months

        if states:
            request['queryStringParameters']['states'] = states

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

    def calculate_sync(self, uid, months=[], states=[]):
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
        return self._calculate_with_invocation_type(uid, months=months, states=states,
                                                    invocation_type=InvocationType.RequestResponse)

    def calculate_async(self, uid, months=[], states=[]):
        return self._calculate_with_invocation_type(uid, months=months, states=states,
                                                    invocation_type=InvocationType.Event)

    def calculate_breakdown(self, uid, pids=[], month='01'):
        request = {
            'httpMethod': 'GET',
            'queryStringParameters': {
                'service': 'detailed',
                'uid': uid,
                'month': month,
            }
        }

        if pids:
            request['queryStringParameters']['pids'] = pids

        encoded_payload = bytes(json.dumps(request)).encode('utf-8')
        response = self._client.invoke(
            FunctionName='ma_calculator',
            InvocationType=InvocationType.RequestResponse,
            LogType='None',
            Payload=encoded_payload,
        )

        # TODO: there must be a better way to process the response than this.
        if response['StatusCode'] != 200:
            raise Exception(json.loads(response['Payload'].read())['body'])
        else:
            return json.loads(json.loads(response['Payload'].read())['body'])

