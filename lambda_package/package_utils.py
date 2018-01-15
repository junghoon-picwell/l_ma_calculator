def message_api_gateway(status_code, body):
    # An object of this format should be used when results are returned to the API
    # gateway when Lambda Proxy is used. Otherwise, the API gateway will issue a
    # 502 error "malformed Lambda proxy response".
    #
    # See
    # https://aws.amazon.com/premiumsupport/knowledge-center/malformed-502-api-gateway/
    # https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html
    return {
        'statueCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        'isBase64Encoded': False,  # not sure whether this is correct
        'body': body,
    }


def filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    return filtered_claims
