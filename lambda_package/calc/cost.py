from __future__ import absolute_import

from .utils import (
    cache_benefit_fun,
    chained_get,
    PART_A_CATEGORIES
)


def patch_categories(category):
    """
    whenever there isn't any benefit data provided, we can use a "patched" category
    to provide it. this started in 2015 as a mis-categorization fix, but has become
    a basic part of the MA calculators benefit processing.

    For Benefit Categories 1 & 2 (ambulance service), 18 & 19 (emergency service), and
    47 & 48 (urgently needed service), our claims canonicalization step puts all claims
    in Benefit Categories 1, 18, and 47 that provide more generous benefits (admitted
    after) than their counterparts (not admitted after). These benefit categories are
    patched temporarily to provide less generous benefits.

    :param category: the benefit category
    :type category: str|int
    """

    category = str(category)

    # TODO: remove this once we improve claims canonicalization.
    if category == '1':  # ambulance
        return '2'
    elif category == '18':  # emergency care
        return '19'
    elif category == '47':  # urgently needed care
        return '48'

    # These are "legit" patching due to missing benefit information:
    if category == '21':  # hearing services
        return '20'
    elif category == '22':
        return '20'
    elif category == '23':
        return '20'
    elif category == '27':  # renal disease
        return '28'
    elif category == '33':  # outpatient mental
        return '37'
    elif category == '34':
        return '37'
    elif category == '36':
        return '35'
    elif category == '38':
        return '37'

    return category


@cache_benefit_fun
def _get_plan_parameter(benefits, category, parameter, default_value):
    """
    provides the value of the parameters at all levels
    (plan, share, category) provided a category and plan benefits
    structure.

    note:
        - default value for deductibles is 0.0 (immediately into cost sharing)
        - default value for moops is infinity (no limit)

    :param benefits: the benefits dict as produced by BenefitsParser
     :type benefits: dict

    :param category: the category of interest
    :type category: str|int

    :param parameter: the parameter you'd like {deductibles | moops}
    :type parameter: str

    :param default_value: the default value you'd like it to take on
    :type default_value: float

    :return: dict[str, float]
        {
            'composite': <float: composite value>
            'in_network': <float: in_network value>,
            'out_network': <float: out_network value>,
            'category': <float: category value>
        }
    """

    param_values = {
        'composite': default_value,
        'in_network': default_value,
        'out_network': default_value,
        'category': default_value
    }

    patched_category = patch_categories(category)

    for network_category in ('composite', 'in_network', 'out_network'):
        threshold = chained_get(benefits, [parameter, network_category], None)

        if threshold is not None and patched_category in threshold['categories']:
            # All plan-wide deductibles and OOP limits are by year:
            assert threshold['period'] == 365
            param_values[network_category] = float(threshold['amount'])

        else:
            param_values[network_category] = float(default_value)

    # TODO: this needs an update in the next iteration
    param_values['category'] = \
        float(chained_get(benefits, ['benefits', 'categories', patched_category, parameter],
                          default_value))

    return param_values


def get_moops(benefits, category):
    """
    note: when a limit isn't represented, it is defaulted to infinity

    :param benefits: a PlanBenefits style dict
    :type benefits: dict

    :param category: the category we're interested in
    :type category: str|int

    :return: a dict containing the limits that are relevant to the plan

    {
      'category': float,
      'composite': float,
      'in_network': float,
      'out_network': float,
    }

    """
    return _get_plan_parameter(benefits, category, 'oop_limits', float('infinity'))


def get_deductibles(benefits, category):
    """
    note: when a deductible isn't represented, it is defaulted to infinity

    :param benefits: the benefits dict as produced by BenefitsParser
     :type benefits: dict

    :param category: the category of interest
    :type category: str|int

    :return:
        {
            'composite': <float: composite deductible>
            'in_network': <float: in_network deductible>,
            'out_network': <float: out_network deductible>,
            'category': <float: category deductible>
        }
    """
    return _get_plan_parameter(benefits, category, 'deductibles', float('infinity'))


@cache_benefit_fun
def get_shared_cost_tiers(plan, category, network_type):
    """
    :param plan: the benefits dict as produced by BenefitsParser
     :type plan: dict

    :param category: the category of interest
    :type category: str|int

    :param network_type: either 'in_network' or 'out_network'
    :type network_type: str

    :return: list of dict if benefit information exists
        [
            {
                'copay': {
                    'min': 0,
                    'max': 10,
                    'per_day': False,  # <-- whether min|max is per-day cost
                    'interval_max': 10, # <-- ending day of the day interval#
                },
                'coinsurance': {
                    'min': 0,
                    'max': 10,
                    'per_day': False,  # <-- interval_max left out is infinity
                }
            },
            ...
        ]
        None if the benefit category is uncovered.
    """
    # Look for information under the composite network if no benefit information is provided under
    # the specified network:
    for augmented_network_type in [network_type, 'composite']:
        keys = ['benefits', 'categories', category, augmented_network_type]
        if str(category) in PART_A_CATEGORIES:
            keys.append('day_intervals')

        interval_dict = chained_get(plan, keys, None)
        if interval_dict is not None:
            break

    if interval_dict is not None:
        if str(category) in PART_A_CATEGORIES:
            # we return a sorted list of tiers
            # interval_keys = sorted(interval_dict.keys())
            # intervals = [interval_dict[t] for t in interval_keys]

            interval_keys = sorted(int(day_interval) for day_interval in interval_dict.keys())
            intervals = [interval_dict[str(day_interval)] for day_interval in interval_keys]

        else:
            # There is always one:
            intervals = [interval_dict]
    else:
        # The benefit category is uncovered:
        intervals = None

    return intervals


def get_sharing_value(sharing_params):
    """
    :param sharing_params: the bundle at the end of cost sharing by BenefitsParser
        see the return of `get_shared_cost_tiers` and inside each "share type"
    :type sharing_params: dict

    :return: float|None, pick max first, then min because we'd rather over-estimate
    """
    if 'max' in sharing_params:
        return float(sharing_params['max'])
    elif 'min' in sharing_params:
        return float(sharing_params['min'])
    else:
        return None


def get_copay_value(copay_params, days):
    """
    :param copay_params: the bundle at the end of cost sharing by BenefitsParser
        see the return of `get_shared_cost_tiers` and inside "copay"
    :type copay_params: dict

    :param days: the number of days you've spent in copay
     type days: int
    :return:
    """
    multiplier = 1.0
    if copay_params.get('per_day', False):
        multiplier = days
    share_value = get_sharing_value(copay_params)
    copay = share_value if share_value is not None else 0.0
    return multiplier * copay


def get_shared_oop(shared_cost, intervals, category, day_count_start, day_count_end):
    """
    :param shared_cost: the amount of covered cost where cost sharing applies
    :type shared_cost: float

    :param intervals: cost sharing information returned by get_shared_cost_tiers()
     :type intervals: list of dicts

    :param category: the category of interest
    :type category: str|int

    :param length_of_stay: the length of stay of the claim corresponding only to the covered costs
    :type length_of_stay: int

    :return: float, the shared out of pocket costs
    """
    # extreme safety net! all claims MUST have length_of_stay (in- AND out- patient)
    # this also allows flexibility with possible mistakes in our length_of_stay
    # calculation in the canonicalizer.
    length_of_stay = day_count_end - day_count_start + 1

    # we want to apply in-patient cost sharing to Part A benefit categories:
    if str(category) in PART_A_CATEGORIES:
        # TODO: junghoon would like us to review SNF length of stay calculation
        shared_oop = get_shared_inpatient_cost(shared_cost, intervals, day_count_start,
                                               day_count_end)
    else:
        # claims that are not inpatient are assumed to not be multi-day
        # which is the reason for ignoring the day intervals. that being
        # said, we won't ignore it in any cost sharing that category may
        # have.
        coinsurance_cost = 0.0
        if 'coinsurance' in intervals[0]:
            percentage = get_sharing_value(intervals[0]['coinsurance']) / 100.0
            coinsurance_cost = percentage * shared_cost

        copay_cost = 0.0
        if 'copay' in intervals[0]:
            copay_cost = get_copay_value(intervals[0]['copay'], length_of_stay)

        shared_oop = max(coinsurance_cost, copay_cost)

    # if the `shared_oop` > `shared_cost`: a deductible + copay situation
    # then we want to output `shared_cost` (lesser-of rule).
    return min(shared_oop, shared_cost)


def get_shared_inpatient_cost(shared_cost, intervals, day_count_start, day_count_end):
    """
    :param shared_cost: the amount of covered costs where cost sharing applies
     :type shared_cost: float

    :param intervals: a list of tiers that follow the benefit parser structure
        see `get_shared_cost_tiers` for structure of a tier
    :type intervals: list of dict

    :param length_of_stay: the length of stay corresponding only to the covered costs
    :type length_of_stay: int

    :return: float : cost of the claim choosing the max(copay, coinsurance) along tiers
    """
    cost = 0.0
    current_day_counter = day_count_start  # min value is 1
    cost_per_day = float(shared_cost) / (day_count_end - day_count_start + 1)

    # We don't want to go through tiers that may not apply anymore.
    # If a claim is in the same benefit period as a prior claim,
    # it should resume cost sharing from the tier that the prior claim left off on.
    for tier in intervals:
        max_cost = None

        for share_type in ('coinsurance', 'copay'):

            if share_type not in tier:
                continue

            interval_max = float(tier[share_type].get('interval_max', 'infinity'))
            if current_day_counter > interval_max:
                continue

            if day_count_end < interval_max:
                days_in_tier = max(day_count_end - current_day_counter + 1, 0)
                share_day_counter = day_count_end + 1

            else:
                days_in_tier = max(interval_max - current_day_counter + 1, 0)
                share_day_counter = interval_max + 1

            if share_type == 'coinsurance':
                cost_for_tier = cost_per_day * days_in_tier
                share_value = get_sharing_value(tier['coinsurance'])
                percentage = 1.0 if share_value is None else (share_value / 100.0)
                current_cost = percentage * cost_for_tier

            else:
                current_cost = get_copay_value(tier['copay'], days_in_tier)

            if max_cost is None or current_cost >= max_cost:
                max_cost = current_cost
                max_current_day = share_day_counter

        if max_cost is not None:
            cost += max_cost
            current_day_counter = max_current_day

        if current_day_counter > day_count_end:
            break

    return cost


def normalize_prices(claims_list, pricing, state_fips, in_network):
    """
        DEPRECATED!

        this function has been deprecated:
        > we are not maintaining the pricing dictionary any more [junghoon]

        this code is still here because:
        > comes down to if/how we want to maintain out-of-network cost estimates.
        > I suspect we will want to revisit this normalization strategy, if we do get
        > back to delivering OON estimates [ani]

        Looks up and changes the prices in claims to normalized ones, instead of just
       using the cost information in the claim
    """
    network = 'in_network' if in_network else 'out_network'

    for claim in claims_list:
        normalized_price = chained_get(pricing,
                                       [str(state_fips), str(claim['benefit_category']), network],
                                       None)
        if normalized_price is not None:
            claim['cost'] = normalized_price
        else:
            claim['cost'] = pricing['national'][str(claim['benefit_category'])][network]

    return claims_list


def get_benefit_period_type(plan, benefit_category, network):
    return chained_get(plan,
                       ['benefits', 'categories', benefit_category, network, 'benefit_period'],
                       None)


def get_combine_inpatient_day_count(plan):
    # TODO: the in-network and out-of-network cannot have different rules for combining. This may be a schema design mistake.
    # If plan has no 'benefits', claims against this plan will be considered 'uncovered', and the
    # calendar will never be used. The default value of 'False' here will never be used either.
    return chained_get(plan, ['benefits', 'combine_inpatient_day_count'], False)


def get_required_days(plan, network):
    # Even though 'required_days' is a required field if Benefit Category 44 is covered, the
    # lookup happens at the setup stage when ClaimStore objects are created.
    return chained_get(dct=plan,
                       keys=['benefits', 'categories', '44', network, 'required_days'],
                       default=0)
