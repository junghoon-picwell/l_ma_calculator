from lambda_package.config_info import (
    CONFIG_FILE_NAME,
    ConfigInfo,
)

if __name__ == '__main__':
    config = ConfigInfo(CONFIG_FILE_NAME)

    if not config.use_s3_for_benefits:
        print 'true'
    else:
        # Don't return anything for make:
        pass
