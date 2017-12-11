import pytest
from ma_calculator import main

def test_single_calculation():
    run_options = {
        'uid': '1102836901'
    }
    aws_options = {
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'region_name': '',
        'profile_name': None
    }

    result = main(run_options, aws_options)

    assert result['statusCode'] == '200'
