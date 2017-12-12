import pytest
from lambda_package.ma_calculator_wrapper import main

def test_single_calculation():
    run_options = {
        'uid': '1102836901'
    }
    aws_options = {
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
        'region_name': None,
        'profile_name': None
    }

    result = main(run_options, aws_options)

    assert result['statusCode'] == '200'
