class KVstorage():

    def __init__(self):
        self.dict = {}

    def __getitem__(self, key):
        return self.dict[key]

    def __setitem__(self, key, value):
        self.dict[key] = value

    def values(self):
        return self.dict.values()

    def keys(self):
        return self.dict.keys()
