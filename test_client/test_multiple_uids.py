from __future__ import absolute_import
import json
import pytest

import boto3

from test_client import LambdaCalculatorTestClient


def test_for_user_ids_sync():
    test_client = LambdaCalculatorTestClient()

    uid_to_test = "1006243201"
    response = test_client.calculate_sync(uid_to_test)

    print(response)

    response_payload = json.loads(response['Payload'].read().decode())
    print(response_payload)
    assert response['StatusCode'] == 200

def test_for_user_ids_async():
    test_client = LambdaCalculatorTestClient()

    uid_to_test = "1009901"
    response = test_client.calculate_async(uid_to_test)

    print(response)
    assert response['StatusCode'] == 202
