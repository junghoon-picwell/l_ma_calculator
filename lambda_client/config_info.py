import ConfigParser
import os

# This assumes that the config file is in the same directory as config_info.py:
CONFIG_FILE_NAME = os.path.join(os.path.dirname(__file__),
                                'lambda.cfg')


class ConfigInfo(object):

    __slots__ = (
        'claims_table',
        'claims_bucket',
        'claims_path',
        'benefits_bucket',
        'benefits_path',
        'costs_table',
        'use_s3_for_claims',
        'claims_year',
        'use_s3_for_benefits',
        'all_states',
        'log_level',
        'max_calculated_uids',
        'max_lambda_calls',
    )

    def __init__(self, config_file_name):
        config_parser = ConfigParser.RawConfigParser()
        # config_parser.read(os.path.expanduser(config_file_name))
        config_parser.read(config_file_name)

        self.claims_table = config_parser.get('aws', 'DYNAMODB_CLAIM_TABLE')

        self.claims_bucket = config_parser.get('aws', 'CLAIMS_BUCKET')
        self.claims_path = config_parser.get('aws', 'CLAIMS_PATH')

        self.benefits_bucket = config_parser.get('aws', 'BENEFITS_BUCKET')
        self.benefits_path = config_parser.get('aws', 'BENEFITS_PATH')

        self.costs_table = config_parser.get('aws', 'DYNAMODB_COST_TABLE')

        self.use_s3_for_claims = config_parser.get('claims', 'USE_S3') == 'TRUE'
        self.claims_year = config_parser.get('claims', 'CLAIMS_YEAR')

        self.use_s3_for_benefits = config_parser.get('benefits', 'USE_S3') == 'TRUE'
        self.all_states = [state.strip() for state in
                           config_parser.get('benefits', 'ALL_STATES').split(',')]

        self.log_level = config_parser.get('general', 'LOG_LEVEL')

        self.max_calculated_uids = int(config_parser.get('general', 'MAX_CALCULATED_UIDS'))
        self.max_lambda_calls = int(config_parser.get('general', 'MAX_LAMBDA_CALLS'))

