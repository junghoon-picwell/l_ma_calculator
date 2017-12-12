class BenefitPeriodBase(object):
    def __init__(self, benefit_category, *args, **kwargs):
        self.benefit_category = benefit_category
        self.day_count = kwargs.pop('day_count', 0)

    def _repr(self, *args, **kwargs):
        return_str = "{}('{}'".format(self.__class__, self.benefit_category)
        if args:
            return_str += ', ' + ', '.join(str(arg) for arg in args)
        if self.day_count > 0:
            return_str += ', day_count={}'.format(self.day_count)
        if kwargs:
            return_str += ', ' + ', '.join(
                '{}={}'.format(key, value) for key, value in kwargs.iteritems())
        return_str += ')'

        return return_str

    def add_claim(self, new_claim):
        raise NotImplementedError