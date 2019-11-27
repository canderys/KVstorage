import os
import sys


class KVstorage:

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



class DictMemoryStorage:
    def __init__(self):
        self.data = {}

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value
        print('dict size: ' + str(self.size()))

    def size(self):
        return sys.getsizeof(self.data)


class MemoryStorage:
    def __init__(self, data=str()):
        self.data = data

    def __del__(self):
        self._save()

    def print(self):
        print(self.data)

    def append(self, key, value):
        if not self._can_append_item():
            self._save()
            self.data = str()

        self.data = self.data + '{} {} {}\n'.format(hash(str(key) + str(value)), key, value)


    def get(self, key):
        pass
        # self.data[key]

    def _can_append_item(self):
        return True

    def _save(self):
        pass


class FileStorage:
    pass


# class FileStorage:
#     def __init__(self):
#         file_name = 'file_storage.fs'
#         if not os.path.exists(file_name):
#             self.file = open(file_name, 'rw')
#
#     def __del__(self):
#         if self.file:
#             self.file.close()
#
#     def append(self, key, value):
#         pass
#
#     def find(self, key):
#         pass

