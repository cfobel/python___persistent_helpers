from durus.persistent import Persistent
from durus.persistent_dict import PersistentDict
from durus.persistent_list import PersistentList


class PersistentOrderedDict(Persistent):
    '''
    This class implements the same interface as the `collections.OrderedDict`
    class from the standard library, but uses `persistent` data types for Durus
    support.
    '''
    def __init__(self, items=None):
        self.key_index = PersistentList()
        self.data = PersistentDict()
        if items:
            for k, v in items:
                self[k] = v

    def keys(self):
        return self.key_index[:]

    def __setitem__(self, k, v):
        if k not in self.data:
            self.key_index.append(k)
        self.data[k] = v

    def items(self):
        return [(k, v) for k, v in self.iteritems()]

    def iteritems(self):
        for k in self.key_index:
            yield k, self.data[k]

    def values(self):
        return [v for k, v in self.iteritems()]

    def get(self, key):
        return self.data.get(key)

    def __delitem__(self, key):
        del self.data[key]
        i = self.key_index.index(key)
        del self.key_index[i]

    def __getitem__(self, key):
        return self.data[key]

    def move_to_end(self, key, last=True):
        assert(key in self)
        items = []
        for k, v in self.items():
            if k != key:
                items.append((k, v))
                del self[k]
        if last:
            items.append((key, self[key]))
            del self[key]
        for k, v in items:
            self[k] = v

    def __contains__(self, key):
        return key in self.data

    def __len__(self):
        return len(self.key_index)
