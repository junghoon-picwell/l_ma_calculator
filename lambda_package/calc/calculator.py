"""
MA Cost Calculator computes the out of pocket cost for an individual under
a given Medicare plan.

Individuals are represented as aggregations of claims from Truven
(see etl/truven/claims.py) in JSON format.

Medicare plans are represented in a Picwell canonicalized format in JSON
(see rfc/medicare/plan.md)

* For example local usage, refer to test/test_calculator.py
* For additional documentation, refer to README.md
"""

from __future__ import absolute_import

from collections import defaultdict

from .calendar import Calendar
from .cost import (
    get_moops,
    get_deductibles,
    get_shared_oop,
    normalize_prices,
    get_shared_cost_tiers,
    patch_categories
)
from .utils import (
    adjust_part_a_claim_for_year_overflow,
    is_snf_claim,
    is_part_a_claim,
    SNF_CATEGORIES
)

ALL_MONTHS = ['%02d' % i for i in xrange(1, 13)]

inf = float('infinity')


def _get_threshold_amounts_for(amounts_container, costs, key_name,
                               network_type, benefit_category):
    # comp_paid_out = how much "space left" there is to the composite limit
    comp_amount = amounts_container['composite'] - costs[key_name]['composite']

    # net_paid_out = how much "space left" there is to the network limit
    net_amount = amounts_container[network_type] - costs[key_name][network_type]

    # cat_paid_out = how much "space left" there is to the category limit
    cat_amount = amounts_container['category'] - costs[key_name]['categories'][benefit_category]

    return comp_amount, net_amount, cat_amount


def _get_amount_to_threshold_func(costs, benefits, benefit_category, network_type):
    def _amount_to_threshold(threshold):
        if threshold == 'oop_limits':
            # we use infinity with limits as a placeholder for "not listed"
            limits = get_moops(benefits, benefit_category)

            (comp_amount,
             net_amount,
             cat_amount) = _get_threshold_amounts_for(limits, costs,
                                                      'covered_breakdown', network_type,
                                                      benefit_category)

        elif threshold == 'deductible':
            deductibles = get_deductibles(benefits, benefit_category)

            (comp_amount,
             net_amount,
             cat_amount) = _get_threshold_amounts_for(deductibles, costs,
                                                      'deductible_breakdown', network_type,
                                                      benefit_category)

        else:
            raise ValueError(
                "Invalid value for 'threshold'. "
                "Threshold must be 'deductible' or 'oop_limits'.")

        comp_amount = comp_amount if comp_amount > 0 else 0
        net_amount = net_amount if net_amount > 0 else 0
        cat_amount = cat_amount if cat_amount > 0 else 0

        return min(cat_amount, net_amount, comp_amount)

    return _amount_to_threshold


def _claim_has_negative_cost(claim):
    return claim['cost'] <= 0


def _claim_is_not_categorized(claim):
    return claim['benefit_category'] == '0'


def _update_cost_totals_and_breakdowns(costs, claim,
                                       allowed, deductible, covered_oop, uncovered_oop):
    benefit_category = claim['benefit_category']
    network_type = claim['network_type']

    costs['allowed'] += allowed

    # if we're going to over a limit, then one of the "space left"s will
    # be non-infinite and will tell us our bounded pay out. if not,
    # we pay out the total oop because we aren't bounded.
    costs['covered_breakdown']['composite'] += covered_oop
    costs['uncovered_breakdown']['composite'] += uncovered_oop
    costs['deductible_breakdown']['composite'] += deductible

    #  update the totals and deductibles paid out : by network
    costs['covered_breakdown'][network_type] += covered_oop
    costs['uncovered_breakdown'][network_type] += uncovered_oop
    costs['deductible_breakdown'][network_type] += deductible

    #  update the totals and deductibles paid out : by category
    costs['covered_breakdown']['categories'][benefit_category] += covered_oop
    costs['uncovered_breakdown']['categories'][benefit_category] += uncovered_oop
    costs['deductible_breakdown']['categories'][benefit_category] += deductible

    costs['uncovered'] += uncovered_oop
    costs['oop'] += covered_oop


def _get_max_day_count(cost_sharing_intervals):
    last_day_interval = cost_sharing_intervals[-1]

    if 'copay' in last_day_interval:
        copay_interval_max = \
            float(last_day_interval['copay'].get('interval_max', 'infinity'))
    else:
        copay_interval_max = None

    if 'coinsurance' in last_day_interval:
        coinsurance_interval_max = \
            float(last_day_interval['coinsurance'].get('interval_max', 'infinity'))
    else:
        coinsurance_interval_max = None

    # Assume that interval_max for copay and coinsurance are the same:
    assert (copay_interval_max is None or
            coinsurance_interval_max is None or
            copay_interval_max == coinsurance_interval_max)

    return copay_interval_max or coinsurance_interval_max or inf

def _claim_eligible_for_coverage(claim, calendar, cost_sharing_intervals):
    """
    The only way a claim is not covered is if it is an SNF claim and does not have a
    preceding qualifying inpatient claim.
    """
    return ((calendar.is_snf_claim_covered(claim)
             if is_snf_claim(claim['benefit_category'])
             else True) and
            cost_sharing_intervals is not None)


def _determine_covered_portion(claim, calendar, cost_sharing_intervals):
    claim_eligible_for_coverage = _claim_eligible_for_coverage(claim, calendar,
                                                               cost_sharing_intervals)
    if claim_eligible_for_coverage:
        # A claim is at least partially covered:
        max_day_count = _get_max_day_count(cost_sharing_intervals)

        if is_part_a_claim(claim['benefit_category']):
            (day_count_start, day_count_end) = calendar.get_day_counts(claim)

            assert day_count_start <= day_count_end

        else:
            (day_count_start, day_count_end) = (0, claim['length_of_stay'])

        if max_day_count < day_count_start:
            covered_cost = 0
            covered_day_count_start, covered_day_count_end = None, None

        elif max_day_count < day_count_end:
            cost_per_day = float(claim['cost']) / (day_count_end - day_count_start + 1)
            covered_cost = cost_per_day * (max_day_count - day_count_start + 1)
            covered_day_count_start, covered_day_count_end = day_count_start, max_day_count

        else:
            covered_cost = claim['cost']
            covered_day_count_start, covered_day_count_end = day_count_start, day_count_end

    else:
        covered_cost, covered_day_count_start, covered_day_count_end = (
            0, None, None)

    return covered_cost, covered_day_count_start, covered_day_count_end


def _calculate_costs(costs, claim, plan, calendar):
    if _claim_has_negative_cost(claim) or _claim_is_not_categorized(claim):
        # allowed, deductible, covered_oop, uncovered_oop
        return 0, 0, 0, 0

    benefit_category = claim['benefit_category']
    network_type = claim['network_type']

    # Each claim is processed in five steps:
    #    (a) identify uncovered cost (cost sharing, including deductibles and OOP limits, does
    #        not apply to uncovered cost)
    #    (b) apply deductible
    #    (c) apply cost sharing (with lesser-of rule) for amounts exceeding
    #        deductible
    #    (d) apply OOP limit
    #    (e) adjust the deductible in case it is more than the OOP cost
    #    (f) update the state of the calculator

    # cost_sharing_intervals is None if benefit information does not exist. Otherwise, it is a list
    # of cost sharing information.
    cost_sharing_intervals = get_shared_cost_tiers(plan, benefit_category, network_type)

    covered_cost, day_count_start, day_count_end = _determine_covered_portion(
        claim, calendar, cost_sharing_intervals)
    uncovered_oop = claim['cost'] - covered_cost

    get_amount_to_threshold = _get_amount_to_threshold_func(costs, plan,
                                                            benefit_category, network_type)

    # you have to pay the least deductible left
    deductible_left = get_amount_to_threshold('deductible')

    # if it turns out that, because no deductibles were specified, that you have
    # infinity left (really, a place-holder for None in computation), it's 0.0
    if deductible_left == inf:
        deductible = 0.0
    else:
        deductible = min(covered_cost, deductible_left)

    shared_cost = covered_cost - deductible

    if shared_cost > 0.0:
        # TODO (junghoon): is it all right to used shared_cost (without deductible)
        # for inpatient claims
        shared_oop = get_shared_oop(shared_cost, cost_sharing_intervals, benefit_category,
                                    day_count_start, day_count_end)
    else:
        shared_oop = 0.0

    # Apply MOOPs:
    covered_oop_left = get_amount_to_threshold('oop_limits')
    covered_oop = min(covered_oop_left, deductible + shared_oop)

    # Composite or category specific OOP limits can additionally limit what goes towards
    # deductibles. For instance, see features/out_of_network.feature for an example.
    deductible = min(covered_oop, deductible)

    return claim['cost'], deductible, covered_oop, uncovered_oop


def _get_claim_info(claim, force_network=None):
    cost = float(claim.get('cost', 0.0))
    benefit_category = patch_categories(claim.get('benefit_category', 0))
    claim_network_type = claim.get('network_type', 'in_network')
    network_type = force_network or claim_network_type
    claim_info = {
        'cost': cost,
        'benefit_category': benefit_category,
        'network_type': network_type,
        'length_of_stay': claim['length_of_stay'],
        'admitted': claim['admitted'],
        'discharged': claim['discharged'],
    }

    return claim_info


def calculate_oop(claims, plan, force_network=None,
                  truncate_claims_at_year_boundary=False):
    """
    We go through claims sequentially and tally up
    costs taking care of the deductibles and limits
    of the plan, network-type, and category.

    OOP = out of pocket

    Along the way, we maintain a bundle of costs

        {
            'oop': 0.0,  the total OOP expenditure
            'allowed': 0.0, the total allowed costs (*)
            'covered_breakdown': {  a breakdown of the total OOPs towards the proper scope (+)
                'composite': 0.0,  these allow us to take care of limits
                'in_network': 0.0,
                'out_network': 0.0,
                'categories': defaultdict(float)
            },
            'uncovered': 0.0,  uncovered costs (due to unspecified benefits or long inpatient stays)
            'uncovered_breakdown': {  breakdown of uncovered costs
                'composite': 0.0,
                'in_network': 0.0,
                'out_network': 0.0,
                'categories': defaultdict(float),
            }
            'deductible_breakdown': { the amounts paid towards the scoped deductible
                'composite': 0.0,
                'in_network': 0.0,
                'out_network': 0.0,
                'categories': defaultdict(float),
            },
        }

    (*) allowed cost : if a claim is 2000$ but you only pay 100$ after
                    cost sharing, then `total` will go up by 100$
                    and `allowed` will go up by `2000$`.

    (+) covered_breakdown note: the sum of the covered_breakdown is NOT equivalent to the `oop`.
                    total = sum(covered_breakdown) - msa_deposit

    Args:
        claims: an ORDERED-BY-DATE list of claims
                [
                    {
                        'benefit_category': 12,
                        'cost': 100.00,
                    }
                    ...
                ]

        plan: the benefits dict as produced by the parser.

        force_network: one of 'in_network' | 'out_network' | None
        Whether to assume the claims are in network or out of network, or None if
        you want to use the network_type of the claim.

        truncate_claims_at_year_boundary: True if claims should be adjusted if they spill over into
        the next year; False if otherwise. Defaults to False because this is how the
        Medigap calculator currently works.
        We want to make both of these calculators work with truncation, but for now consistency
        between the two is important.

    Returns:
         A float value representing the total out-of-pocket cost
    """

    costs = {
        'oop': 0.0,
        'allowed': 0.0,
        # TODO: is it necessary to track composite breakdown?
        # It is always the sum of in-network and out-of-network values
        'covered_breakdown': {
            'composite': 0.0,
            'in_network': 0.0,
            'out_network': 0.0,
            'categories': defaultdict(float),
        },
        'uncovered': 0.0,
        # TODO: is it necessary to track composite breakdown?
        # It is always the sum of in-network and out-of-network values
        'uncovered_breakdown': {
            'composite': 0.0,
            'in_network': 0.0,
            'out_network': 0.0,
            'categories': defaultdict(float),
        },
        # TODO: is it necessary to track composite breakdown?
        # It is always the sum of in-network and out-of-network values
        'deductible_breakdown': {
            'composite': 0.0,
            'in_network': 0.0,
            'out_network': 0.0,
            'categories': defaultdict(float),
        },
    }

    part_a_calendar = Calendar(plan)

    for claim in claims:
        claim = _get_claim_info(claim, force_network)
        if truncate_claims_at_year_boundary:
            adjust_part_a_claim_for_year_overflow(claim)

        (allowed,
         deductible,
         covered_oop,
         uncovered_oop) = _calculate_costs(costs, claim, plan, part_a_calendar)

        # Update the totals and deductibles paid out:
        _update_cost_totals_and_breakdowns(costs, claim,
                                           allowed, deductible, covered_oop, uncovered_oop)

    # for 2015 some plans include an msa deposit that can offset oop spending
    msa_deposit = float(plan.get('msa_deposit', 0.0))
    costs['oop'] = max(0.0, costs['oop'] + costs['uncovered'] - msa_deposit)

    return costs


def calculate_oops_proration(enrolid, canonical_claims, benefits_dict, claim_year,
                             pricing_dict=None, start_months=ALL_MONTHS):
    """
    Calculates in network and out of network oop and returns the tuple spark wants
    """
    if canonical_claims is None:
        canonical_claims = []

    if pricing_dict is not None:
        claims_inn = normalize_prices(canonical_claims, pricing_dict,
                                      benefits_dict.get('state_fips'),
                                      True)

        # 2016 Note: We aren't using out of network cost estimates this year
        # claims_out = normalize_prices(canonical_claims, pricing_dict,
        #                               benefits_dict.get('state_fips'),
        #                               False)
    else:
        claims_inn = canonical_claims

        # 2016 Note: We aren't using out of network cost estimates this year
        # claims_out = canonical_claims

    picwell_id = benefits_dict.get('picwell_id')

    results = []
    for start_month in start_months:
        start_date = '{}-{}-01'.format(claim_year, start_month)
        end_date = '{}-12-31'.format(claim_year)

        prorated_claims_inn = [claim for claim in claims_inn
                               if start_date <= claim['discharged'] <= end_date]

        # 2016 Note: We aren't using out of network cost estimates this year
        # prorated_claims_out = [claim for claim in claims_out
        #                        if start_date <= claim['discharged'] <= end_date]

        total_oop = calculate_oop(prorated_claims_inn, benefits_dict, force_network='in_network',
                                  truncate_claims_at_year_boundary=False)

        # 2016 Note: We aren't using out of network cost estimates this year
        # out_ntwk_oop = calculate_oop(prorated_claims_out, benefits_dict, False)
        results.append({'picwell_id': picwell_id,
                        'enrolid': enrolid,
                        'in_ntwk_oop': total_oop['oop'],
                        'allowed': total_oop['allowed'],
                        'out_ntwk_oop': 0.0,
                        'start_month': start_month})

    return results
