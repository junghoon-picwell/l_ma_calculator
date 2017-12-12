from __future__ import absolute_import

from datetime import (
    datetime,
    timedelta,
)

from .benefit_period import (
    AnnualBenefitPeriod,
    DummyBenefitPeriod,
    OriginalMedicareBenefitPeriod,
    PerStayBenefitPeriod,
)
from .claim_store import SnfClaimStore, InpatientClaimStore
from .cost import (
    get_benefit_period_type,
    get_combine_inpatient_day_count,
    get_required_days,
)
from .utils import (
    as_date,
    THIRTY_DAYS,
    PART_A_CATEGORIES,
    is_part_a_claim,
    is_part_b_claim,
    is_inpatient_claim,
    is_snf_claim,
    NETWORK_TYPES,
    check_admitted_before,
    check_discharged_after
)

_BENEFIT_PERIOD_MAP = {
    'stay': PerStayBenefitPeriod,
    'original_medicare': OriginalMedicareBenefitPeriod,
    'year': AnnualBenefitPeriod,
    None: DummyBenefitPeriod,
}


class Calendar(object):
    """
        Keeps track of the costs per day as well as which
        days would be covered if a Skilled Nursing Facility
        claim were to happen.

        It does this by using Benefit Periods to determine if a claim is covered
        or not.
    """

    _CLAIM_STORE_MAP = {
        'inpatient': InpatientClaimStore,
        'snf': SnfClaimStore
    }

    def __init__(self, benefits):
        self._claim_stores = {}
        # TODO: implement out-of-network and maybe composite as well?
        # Latest benefit periods:
        self._benefit_periods = {
            'in_network': {},
            'out_network': {},
        }
        self._last_part_a_admitted_date = datetime.min

        for benefit_category in PART_A_CATEGORIES:
            for network in NETWORK_TYPES:
                self._benefit_periods[network][benefit_category] = \
                    Calendar._create_benefit_period(benefits, benefit_category, network)

        self._required_days = {
            'in_network': get_required_days(benefits, 'in_network'),
            'out_network': get_required_days(benefits, 'out_network')
        }

        for claim_store_name, claim_store_value in self._CLAIM_STORE_MAP.iteritems():
            self._claim_stores[claim_store_name] = {
                network: claim_store_value(self._required_days[network])
                for network in NETWORK_TYPES
                }

    @staticmethod
    def _create_benefit_period(benefits, benefit_category, network):
        benefit_period_type = get_benefit_period_type(benefits, benefit_category, network)
        combine_inpatient_day_count = get_combine_inpatient_day_count(benefits)

        new_benefit_period = _BENEFIT_PERIOD_MAP[benefit_period_type](
            benefit_category, combine_inpatient_day_count)

        return new_benefit_period

    def _has_qualifying_inpatient_claim(self, network, starting_before, ending_after):
        """
        The eligibility check on inpatient claims is done in claim_store.py when a claim is checked
        for caching.
        Claims are only cached if they are valid/eligible for benefit period rules
        (Mostly length-of-stay and start/end day requirements, but these rules could always be
        extended).
        """
        prev_inpatient = self._claim_stores['inpatient'][network].cached_claim
        # The second condition is necessary for overlapping claims:
        return (check_discharged_after(prev_inpatient, ending_after) and
                check_admitted_before(prev_inpatient, starting_before))

    def _has_qualifying_snf_claim(self, network, ending_after):
        prev_snf = self._claim_stores['snf'][network].cached_claim
        return check_discharged_after(prev_snf, ending_after)

    def is_snf_claim_covered(self, claim):
        """ SNF Claims are covered if they:
              * have required_days = 0; or
              * are within 30 days of either:
                * an inpatient claim of the required length; or
                * a covered SNF claim.

        Args:
            claim: the SNF claim to check for coverage under the given plan and benefit
            period rules.

        Returns:
            True | False based on whether the SNF claim is covered or not.
        """
        assert(is_snf_claim(claim['benefit_category']))

        # check required_days; if 0, this claim is covered
        network = claim['network_type']
        required_days = self._required_days[network]
        if required_days == 0:
            return True

        admitted = as_date(claim['admitted'])
        starting_before = admitted - timedelta(days=required_days)
        ending_after = admitted - THIRTY_DAYS

        return (self._has_qualifying_inpatient_claim(network, starting_before, ending_after) or
                self._has_qualifying_snf_claim(network, ending_after))

    def _set_admitted_date(self, admitted_date):
        admitted = as_date(admitted_date)
        assert self._last_part_a_admitted_date <= admitted
        self._last_part_a_admitted_date = admitted

    def get_day_counts(self, claim):
        """ Returns the starting and ending day counts for this claim, taking Benefit Period
            rules into account, based on the benefits assigned to the calendar and the
            benefit categories and benefit period of the claim passed in.

            The claim must be at least partially covered.
            Passing in a claim that is completely uncovered will cause a runtime error.

        Args:
            claim: the claim whose days to count under the given plan and benefit
            period rules.

        Returns:
            day_count_start: The number of days to be considered as being used up by claims
            in this benefit category before this claim was processed. Used to calculate
            coverage and eligibility.

            day_count_end: The number of days to consider as being used up by claims
            in this benefit category after this claim was processed. Used to calculate
            coverage and eligibility.
        """
        (start_day_count,
         end_day_count) = (1, claim['length_of_stay'])

        network = claim['network_type']
        benefit_category = claim['benefit_category']

        # The claim is either (partially covered) Part A claim or Part B claim. No fully
        # uncovered claim should reach this point.
        assert (is_inpatient_claim(benefit_category) or
                is_part_b_claim(benefit_category) or
                self.is_snf_claim_covered(claim))

        if not is_part_a_claim(benefit_category):
            return start_day_count, end_day_count

        self._set_admitted_date(claim['admitted'])

        for benefit_period in self._benefit_periods[network].itervalues():
            count_tuple = benefit_period.add_claim(claim)

            if count_tuple is not None:
                start_day_count, end_day_count = count_tuple

        for claim_store in self._claim_stores.itervalues():
            claim_store[network].cache_claim_if_applicable(claim)

        return start_day_count, end_day_count
