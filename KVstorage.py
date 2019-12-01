import os
import sys
import pickle


class KVStorage:
    def __init__(self):
        print('init')
        self.dict = {}
        self.temp_dict = {}
        self.max_size = 10000
        self.prev_filename = None
        self.append_new_files = None
        self.current_filename = None
        self.last_saved = None
        if not os.path.exists('data'):
            os.mkdir('data')
        filename_list = os.listdir('data')
        if filename_list:
            self.prev_filename = filename_list[-1]
            print('prev_file_name = {}'.format(prev_filename))
            self.load_data(self.prev_filename)

    def __del__(self):
        print('del')
        filename = self.get_filename()
        self.save(filename)

    def print_dict(self):
        print(self.dict)

    def __getitem__(self, key):
        print('getittem({})'.format(key))
        if key in self.dict:
            return self.dict[key]
        else:
            value = None
            self.temp_dict = self.dict
            if self.find_and_open(key):
                value = self.dict[key]
            self.dict = self.temp_dict
            return value

    def __setitem__(self, key, value):
        print('setitem({}, {})'.format(key, value))
        # если объем пары ключ-значение больше допустимого размера просто выходим
        if sys.getsizeof(key) + sys.getsizeof(value) > self.max_size:
            print("object to big!")
            return
        if key in self.dict:
        # нашли ключ в памяти
            self.append(key, values, filename)
        else:
        # не нашли ключ в памяти
            self.temp_dict = self.dict
            # ищем ключ в файлах, по очереди загружаем каждый файл, и смотрим есть ли в нем ключ
            file_with_key = self.find_and_open(key)
            if file_with_key is None:
            # не нашли ключ ни в памяти, ни в файлах
                self.dict = self.temp_dict
                self.append(key, value, filename)
            else:
            # нашли ключ в одном из файлов
                self.dict[key] = value
                if sys.getsizeof(self.dict) <= self.max_size:
                # если перезаписать значение в файле в котором оно уже было, размер файла не привысит лимит
                    self.save(file_with_key)
                    self.dict = self.temp_dict
                else:
                # если перезаписать значение в файле в котором оно уже было, то она привысит лимит объема,
                # поэтому нужно удалить пару ключ-значение из файла и добавить в текущий буфеер, если
                # это не получится (текущий буфер привысит лимит по размеру), то записываем текущий буфер и
                # создаем новый буфер
                    del self.dict[key]
                    self.append(key, value)

    def append(self, key, value, filename):
        self.dict[key] = value
        if sys.getsizeof(self.dict) > self.max_size:
            del self.dict[key]
            filename = self.get_filename()
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
        print('save({})'.format(filename))
        with open(filename, 'wb') as file:
            pickle.dump(self.dict, file)
            self.last_saved = filename
        self.dict = {}

    def get_filename(self):
        file_number = 0
        while True:
            filename = 'data/{}.dt'.format(file_number)
            if not os.path.isfile(filename):
                return filename
            file_number += 1

    def load_data(self, filename):
        print('load_data({})'.format(filename))
        if os.path.isfile(filename):
            with open(filename, 'rb') as file:
                self.dict = pickle.load(file)
                self.current_filename = filename
