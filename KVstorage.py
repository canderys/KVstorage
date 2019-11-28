import os
import sys
import pickle


class KVstorage:

    def __init__(self):
        self.dict = {}
        self.file_number = 0

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
        self.dict = {}
        self.max_file_number = 0
        filename = self.get_last_filename()
        self.load_data(filename)

    def __del__(self):
        filename = self.get_filename()
        self.save(filename)

    def __getitem__(self, key):
        if key in self.dict:
            return self.dict[key]
        else:
            self.save({"data/{}.dt"}.format(self.max_file_number))
            self.max_file_number = self.max_file_number + 1
        return None

    def __setitem__(self, key, value):
        self.dict[key] = value
        print('dict size: ' + str(self.size()))

    def size(self):
        return sys.getsizeof(self.dict)

    def save(self, filename):
        with open(filename, 'wb') as file:
            pickle.dump(self.dict, file)
        self.dict = {}

    def get_filename(self):
        while True:
            filename = 'data/{}.dt'.format(self.max_file_number)
            if not os.path.isfile(filename):
                return filename
            self.max_file_number += 1

    @staticmethod
    def get_last_filename():
        filename_list = os.listdir('data')
        return filename_list[-1]

    def load_data(self, filename):
        with open(filename, 'rb') as file:
            self.dict = pickle.load(file)

    def find(self, key):
        filename_list = os.listdir('data')
        for filename in filename_list:
            self.load_data(filename)
            if key in self.dict:
                return self.dict[key]
        return None


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

