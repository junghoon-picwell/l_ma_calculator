import datetime


def succeed_with_message(message):
    return {
        'StatusCode': 200,
        'Message': message
    }


def fail_with_message(message):
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


class TimeLogger(object):
    __slots__ = (
        '_logger',
        '_start_message',
        '_end_message',
        '_start_time',
    )

    def __init__(self, logger, end_message='', start_message=''):
        """
        Use {time} and {elapsed} to capture the current time and time elapsed. {elapsed} is
        only meaningful for the end_message.
        """
        self._logger = logger
        self._start_message = start_message
        self._end_message = end_message

    def __enter__(self):
        self._start_time = datetime.datetime.now()

        if self._start_message:
            self._logger.info(self._start_message.format(time=self._start_time))

        return self._start_time

    # TODO: introduce better error handling?
    def __exit__(self, exception_type, exception_value, traceback):
        time = datetime.datetime.now()
        elapsed = (time - self._start_time).total_seconds()

        if self._end_message:
            self._logger.info(self._end_message.format(time=time, elapsed=elapsed))

