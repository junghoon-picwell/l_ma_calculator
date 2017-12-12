import ConfigParser


class ConfigInfo(object):

    def __init__(self, config_file_name):
        config_parser = ConfigParser.RawConfigParser()

        self.config = config_parser.read(config_file_name)
        self.claims_bucket = config_parser.get('aws', 'CLAIMS_BUCKET_NAME')
        self.benefit_bucket = config_parser.get('aws', 'BENEFITS_BUCKET_NAME')
        self.dynamo_db_table = config_parser.get('aws', 'DYNAMODB_TABLE_NAME')
        self.claims_year = config_parser.get('claims', 'CLAIMS_YEAR')
        self.log_level = config_parser.get('general', 'LOG_LEVEL')
