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
        self.temp_dict = {}
        self.current_filename = self.get_last_filename()
        self.load_data(self.current_filename)
        self.max_size = 10000

    def __del__(self):
        filename = self.get_filename()
        self.save(filename)

    def __getitem__(self, key):
        if key in self.dict:
            return self.dict[key]
        else:
            value = None
            self.save(self.current_filename)
            filename_list = os.listdir('data')
            for filename in filename_list:
                self.load_data(filename)
                if key in self.dict:
                    value = self.dict[key]
            self.load_data(self.current_filename)
            return value

    def __setitem__(self, key, value):
        if sys.getsizeof(key) + sys.getsizeof(value) > self.max_size:
            print("object too big!")
            return
        # found_in_file = self.find_and_open(key)
        # self.append(key, value)
        # if found_in_file:
        #     self.dict = self.temp_dict

        if key in self.dict:
            self.append(key, value)
        else:
            self.temp_dict = self.dict
            filename = self.find_and_open(key)
            if filename is not None:
                self.append(key, value, filename)

    def find_and_open(self, key):
        filename_list = os.listdir('data')
        for filename in filename_list:
            self.load_data(filename)
            if key in self.dict:
                return filename
        return None

    def append(self, key, value, filename=None):
        self.dict[key] = value
        if sys.getsizeof(self.dict) > self.max_size:
            del self.dict[key]
            if filename is None:
                filename = self.get_filename()
            self.save(filename)
            self.dict[key] = value

    def save(self, filename):
        with open(filename, 'wb') as file:
            pickle.dump(self.dict, file)
        self.dict = {}

    def get_filename(self):
        file_number = 0
        while True:
            filename = 'data/{}.dt'.format(file_number)
            if not os.path.isfile(filename):
                return filename
            file_number += 1

    def load_data(self, filename):
        if os.path.isfile(filename):
            with open(filename, 'rb') as file:
                self.dict = pickle.load(file)
