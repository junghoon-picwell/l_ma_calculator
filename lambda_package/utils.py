def succeed_with_message(message):
    return {
        'statusCode': '200',
        'message': message
    }


def fail_with_message(message):
    return {
        'statusCode': '500',
        'message': message
    }


def filter_and_sort_claims(claims, claim_year, start_month):
    start_date = '{}-{}-01'.format(claim_year, start_month)
    end_date = '{}-12-31'.format(claim_year)

    # Used to use Admitted, but `calculate_oop__proration()` (which used to be called by
    # the spark calculator) uses `discharged`, so using `discharged` for consistency.
    filtered_claims = [claim for claim in claims if start_date <= claim['discharged'] <= end_date]

    # TODO: should we short claims by admitted????
    # return sorted(filtered_claims, key=lambda claim: claim['admitted'])

    return filtered_claims
