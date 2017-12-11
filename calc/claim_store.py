from __future__ import absolute_import

from .utils import (
    INPATIENT_CATEGORIES,
    get_discharge_date,
)


class ClaimStore(object):
    def __init__(self, _):
        self.cached_claim = None

    def _check_claim(self, _):
        raise NotImplementedError()

    def _cached_claim_discharged_date_before(self, claim):
        # Need to cache the claim with the latest discharge date, not the latest admitted date:
        return (self.cached_claim is None or
                get_discharge_date(self.cached_claim) < get_discharge_date(claim))

    def cache_claim_if_applicable(self, claim_to_check):
        if self._check_claim(claim_to_check):
            self.cached_claim = claim_to_check


class SnfClaimStore(ClaimStore):
    def _check_claim(self, claim):
        return (claim['benefit_category'] == '44' and
               self._cached_claim_discharged_date_before(claim))


class InpatientClaimStore(ClaimStore):
    def __init__(self, required_days):
        self._required_days = required_days
        super(InpatientClaimStore, self).__init__(required_days)

    def _check_claim(self, claim):
        return ((claim['benefit_category'] in INPATIENT_CATEGORIES and
                 claim['length_of_stay'] >= self._required_days) and
                self._cached_claim_discharged_date_before(claim))
