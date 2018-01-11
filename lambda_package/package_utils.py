def message_success(message, value=None):
    dct = {
        'StatusCode': 200,
        'Message': message
    }
    if value is not None:
        dct['Return'] = value

    return dct


def message_failure(message):
    return {
        'StatusCode': 500,
        'Message': message
    }


def filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    return filtered_claims
