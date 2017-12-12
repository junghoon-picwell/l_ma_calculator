from __future__ import absolute_import

from .base import BenefitPeriodBase


class DummyBenefitPeriod(BenefitPeriodBase):
    def add_claim(self, new_claim):
        return None
