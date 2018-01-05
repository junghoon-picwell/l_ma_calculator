import os

from lambda_client.config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)


if __name__ == '__main__':
    config = ConfigInfo(os.path.join('lambda_client', CONFIG_FILE_NAME))

    if not config.use_s3_for_benefits:
        if config.benefits_bucket:
            print 's3://' + os.path.join(config.benefits_bucket, config.benefits_path)
        else:
            print config.benefits_path
    else:
        # Don't return anything for make:
        pass
