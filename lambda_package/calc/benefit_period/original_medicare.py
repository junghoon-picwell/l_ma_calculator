from __future__ import absolute_import

from datetime import datetime
from ..utils import (
    as_date,
    get_discharge_date,
    INPATIENT_CATEGORIES,
    SIXTY_DAYS)

from .base import BenefitPeriodBase


class OriginalMedicareBenefitPeriod(BenefitPeriodBase):
    def __init__(self, benefit_category, combine_inpatient_day_count, *args, **kwargs):
        super(OriginalMedicareBenefitPeriod, self).__init__(benefit_category, args, kwargs)
        self.end_date = kwargs.pop('end_date', datetime.min)
        self.combine_inpatient_day_count = combine_inpatient_day_count

    def __repr__(self):
        if self.end_date > datetime.min:
            return self._repr(self.combine_inpatient_day_count,
                              end_date="'{}'".format(self.end_date.strftime('%Y-%m-%d')))
        else:
            return self._repr(self.combine_inpatient_day_count)

    def _is_open(self, start_date):
        # A newly initialized Original-Medicare benefit period is always open:
        return as_date(start_date) <= self.end_date + SIXTY_DAYS

    def _is_snf(self, new_claim):
        return self.benefit_category == '44' and new_claim['benefit_category'] == '44'

    def _benefit_category_matches_claim_without_counts_combined(self, new_claim):
        return (not self.combine_inpatient_day_count and
                self.benefit_category == new_claim['benefit_category'])

    def _inpatient_claim_with_counts_combined(self, new_claim):
        return (self.combine_inpatient_day_count and
                new_claim['benefit_category'] in INPATIENT_CATEGORIES)

    def add_claim(self, new_claim):
        if not self._is_open(new_claim['admitted']):
            # Reset benefit period
            self.day_count = 0
            self.end_date = datetime.min

        # Any Part A claim can extend a Original Medicare benefit period:
        # TODO: maybe should should sort out the discharged versus admitted + length of stay.
        self.end_date = max(self.end_date, get_discharge_date(new_claim))

        if (self._is_snf(new_claim) or
                self._benefit_category_matches_claim_without_counts_combined(new_claim) or
                self._inpatient_claim_with_counts_combined(new_claim)):
            start_day_count = self.day_count + 1
            self.day_count += new_claim['length_of_stay']

        if self.benefit_category == new_claim['benefit_category']:
            # Only return the day range when the benefit category match exactly:
            return start_day_count, self.day_count

        else:
            return None
