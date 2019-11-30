import os
import sys
import pickle


class KVStorage:
    def __init__(self):
        self.dict = {}
        self.temp_dict = {}
        self.max_size = 10000

    def __del__(self):
        filename = self.get_filename()
        self.save(filename)

    def __getitem__(self, key):
        if key in self.dict:
            return self.dict[key]
        else:
            value = None
            self.temp_dict = self.dict
            if find_and_open(self, key):
                value = self.dict[key]
            self.dict = self.temp_dict
            return value

    def __setitem__(self, key, value):
        # если объем пары ключ-значение больше допустимого размера просто выходим
        if sys.getsizeof(key) + sys.getsizeof(value) > self.max_size:
            print("object to big!")
            return
        if key in self.dict:
        # нашли ключ в памяти
            filename = self.get_filename()
            append(key, values, filename)
        else:
        # не нашли ключ в памяти
            self.temp_dict = self.dict
            # ищем ключ в файлах, по очереди загружаем каждый файл, и смотрим есть ли в нем ключ
            file_with_key = self.find_and_open(key)
            if file_with_key is None:
            # не нашли ключ ни в памяти, ни в файлах
                self.dict = self.temp_dict
                filename = self.get_filename()
                append(key, values, filename)
            else:
            # нашли ключ в одном из файлов
                self.append(key, value, file_with_key)

    def append(self, key, value, filename):
        self.dict[key] = value
        if sys.getsizeof(self.dict) > self.max_size:
            del self.dict[key]
            self.save(filename)
            self.dict[key] = value

    def find_and_open(self, key):
        filename_list = os.listdir('data')
        for filename in filename_list:
            self.load_data(filename)
            if key in self.dict:
                return filename
        return None

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
