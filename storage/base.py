from abc import ABCMeta, abstractmethod


class BaseStorage(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_items(self, keys):
        '''
        Return key-value pairs for the provided keys, throwing a KeyError on an unknown key.
        Results are not guaranteed to be in the provided order.
        :param keys:
        :return: an iterable of key-value pairs
        :rtype: Iterable[Tuple[key, value]]
        '''
        pass

        # def __getitem__(self, keys):
        #     return self.get_items(keys)
