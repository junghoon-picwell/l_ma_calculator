from __future__ import absolute_import

from .base import BenefitPeriodBase


class AnnualBenefitPeriod(BenefitPeriodBase):
    def __repr__(self):
        return self._repr()

    def add_claim(self, new_claim):
        if self.benefit_category == new_claim['benefit_category']:

            # TODO: this assumes that only claims within one year are given.
            start_day_count = self.day_count + 1
            self.day_count += new_claim['length_of_stay']
            return start_day_count, self.day_count

        else:
            return None
