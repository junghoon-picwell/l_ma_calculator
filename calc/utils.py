import copy
from datetime import (
    datetime,
    timedelta,
)

# 25 : In-Patient: hospital
# 26 : In-Patient: mental health
# 44 : Skilled Nursing Facility
INPATIENT_CATEGORIES = {'25', '26'}
SNF_CATEGORIES = {'44'}

_ALL_BENEFIT_CATEGORIES = set(str(benefit_category) for benefit_category in range(1, 50))
PART_A_CATEGORIES = INPATIENT_CATEGORIES | SNF_CATEGORIES
PART_B_CATEGORIES = _ALL_BENEFIT_CATEGORIES - PART_A_CATEGORIES

THIRTY_DAYS = timedelta(days=30)
SIXTY_DAYS = timedelta(days=60)

NETWORK_TYPES = ['in_network', 'out_network']


def cache_benefit_fun(fun):
    """ Cache function that takes benefits as a first argument. This decorator can be used
    only for functions without kwargs.
    NOTE: this decorator exists to improve performance of MA calculator, so make sure to
        test its speed when you extend it.
    """
    cache = {}

    def wrapped(benefits, *args):
        h = hash(args) + hash(benefits['picwell_id'])
        if h not in cache:
            cache[h] = fun(benefits, *args)
        return cache[h]

    return wrapped


def as_date(datestring):
    return datetime.strptime(datestring, '%Y-%m-%d')


_sentinel = object()


def chained_get(dct, keys, default=_sentinel):
    """ Generalization of get() method for a multi-level dictionary (dictionary of dictionaries).

    The existing get() may be more convenient to use for a single-level dictionary. The function
    will throw a KeyError if no defaults are given.

    :param dct: a dictionary to
    :param keys: a list of keys to look up, starting with the top-level dictionary.
    :param default: default value to return when a key is not found.
    :return: the value corresponding to the keys.
    """
    # dct should be checked for none before keys:
    if dct is _sentinel:
        if default is _sentinel:
            raise KeyError(str(keys))
        else:
            return default
    if not keys:
        return dct

    return chained_get(dct.get(keys[0], _sentinel), keys[1:], default)


def filter_claim_list(claim_list, year):
    """ For only keeping claims from the year you want """
    return filter(lambda x: x['date'][0:4] == str(year), claim_list)


def in_same_year(first_date, second_date):
    return first_date.year == second_date.year


def adjust_part_a_claim_for_year_overflow(claim):

    admitted = as_date(claim['admitted'])

    if not in_same_year(admitted, as_date(claim['discharged'])):

        new_length_of_stay = (datetime(admitted.year + 1, month=1, day=1) - admitted).days

        claim['cost'] *= float(new_length_of_stay)/float(claim['length_of_stay'])
        claim['length_of_stay'] = new_length_of_stay
        claim['discharged'] = (datetime(year=admitted.year + 1, month=1, day=1)
                                        .strftime('%Y-%m-%d'))


def is_part_a_claim(benefit_category):
    return benefit_category in PART_A_CATEGORIES


def is_part_b_claim(benefit_category):
    return benefit_category in PART_B_CATEGORIES


def is_inpatient_claim(benefit_category):
    return benefit_category in INPATIENT_CATEGORIES


def is_snf_claim(benefit_category):
    return benefit_category in SNF_CATEGORIES


def check_discharged_after(claim, threshold_date):
    # TODO: should we use discharged instead?
    # Note that admitted + length of stay is used instead of discharged date. This may be
    # suboptimal, when admitted overlaps with discharged.
    if claim is not None:
        discharged = as_date(claim['admitted']) + timedelta(days=claim['length_of_stay'])
        return threshold_date <= discharged
    else:
        return False


def check_admitted_before(claim, threshold_date):
    return as_date(claim['admitted']) <= threshold_date if claim is not None else False


def get_discharge_date(claim):
    return as_date(claim['admitted']) + timedelta(days=claim['length_of_stay'])
