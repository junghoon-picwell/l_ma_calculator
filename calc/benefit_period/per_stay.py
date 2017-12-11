from __future__ import absolute_import

from .base import BenefitPeriodBase


class PerStayBenefitPeriod(BenefitPeriodBase):
    def __repr__(self):
        return self._repr()

    def add_claim(self, new_claim):
        if self.benefit_category == new_claim['benefit_category']:
            # day counts do not accumulate on per-stay benefit period claims
            assert new_claim['length_of_stay'] > 0
            self.day_count = new_claim['length_of_stay']
            return 1, self.day_count

        else:
            return None
