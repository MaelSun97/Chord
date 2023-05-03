class HashTable:
    _hashtable = dict()

    def __init__(self):
        _hashtable = dict()

    def getHT(self):
        return self._hashtable

    def setHT(self, hashtable):
        self._hashtable = hashtable

    def insert(self, key, value):
        if key in self._hashtable:
            if self._hashtable[key] == value:
                return True
            else:
                return False
        else:
            self._hashtable[key] = value
            return True

    def lookup(self, key):
        if key not in self._hashtable:
            return None
        else:
            return self._hashtable[key]

    def remove(self, key):
        if key not in self._hashtable:
            return True
        else:
            self._hashtable.pop(key)
            return True

    def query(self, subkey, subvalue):
        res = {}
        for key, val in self._hashtable.items():
            if type(val) is dict and subkey in val and val[subkey] == subvalue:
                res[key] = val
        return res

    def slice(self, prev, curr):
        res = {}
        new_ht = {}
        for key, val in self._hashtable.items():
            if key <= prev or key > curr:
                res[key] = val
            else:
                new_ht[key] = val
        self._hashtable = new_ht
        return res
