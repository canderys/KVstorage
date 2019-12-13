import os
import sys
import pickle


class KVstorage:
    def __init__(self, max_size=300):
        self.dict = {}
        self.dict_file = None
        self.max_size = max_size
        if not os.path.exists('data'):
            os.mkdir('data')
            # print('create data')
        filename_list = os.listdir('data')
        # print('list of files: {}'.format(filename_list))
        if filename_list:
            filename = 'data/' + filename_list[-1]
            self.load_data(filename)
        else:
            self.init_new_storage()

    def __del__(self):
        # print('dict file: {}'.format(self.dict_file))
        self.save(self.dict_file)

    def __getitem__(self, key):
        # print('getittem({})'.format(key))
        if self.is_invalid_key(key):
            print('Error invalid key')
            return None
        if key in self.dict:
            return self.dict[key]
        else:
            value = None
            self.save(self.dict_file)
            current = self.dict_file
            if self.find_and_open(key):
                value = self.dict[key]
            self.load_data(current)
            return value

    def __setitem__(self, key, value):
        # print('current size: {}'.format(sys.getsizeof(self.dict)))
        # print('setitem({}, {})'.format(key, value))
        # если объем пары ключ-значение больше
        # допустимого размера просто выходим
        if self.is_invalid_key_or_value(key, value):
            print('Error: invalid key or value')
            return
        if sys.getsizeof(key) + sys.getsizeof(value) > self.max_size:
            print("Error: object to big!")
            return
        if key in self.dict:
            # нашли ключ в памяти
            self.append(key, value)
        else:
            # не нашли ключ в памяти
            self.save(self.dict_file)
            current = self.dict_file
            # ищем ключ в файлах, по очереди загружаем каждый файл,
            # и смотрим есть ли в нем ключ
            file_with_key = self.find_and_open(key)
            if file_with_key is None:
                # не нашли ключ ни в памяти, ни в файлах
                self.append(key, value)
            else:
                # нашли ключ в одном из файлов
                self.dict[key] = value
                if sys.getsizeof(self.dict) <= self.max_size:
                    # если перезаписать значение в файле в котором оно
                    # уже было, размер файла не привысит лимит
                    self.save('data/' + file_with_key)
                    self.load_data(current)
                else:
                    # если перезаписать значение в файле
                    # в котором оно уже было,
                    # то она привысит лимит объема,
                    # поэтому нужно удалить пару ключ-значение из файла и
                    # добавить в текущий буфер, если
                    # это не получится
                    # (текущий буфер привысит лимит по размеру),
                    # то записываем текущий буфер и
                    # создаем новый буфер
                    del self.dict[key]
                    self.load_data(current)
                    self.append(key, value)
        print(self)

    def __contains__(self, key):
        if self.is_invalid_key(key):
            print('Error: invalid key')
            return None
        if key in self.dict:
            return True
        else:
            self.save(self.dict_file)
            current = self.dict_file
            file_with_key = self.find_and_open(key)
            if file_with_key:
                self.load_data(current)
                return True
        return False

    def __repr__(self):
        self.save(self.dict_file)
        current = self.dict_file
        filename_list = os.listdir('data')
        info = 'KVstorage (current file is {}):'.format(current)
        for filename in filename_list:
            fullname = 'data/{}'.format(filename)
            self.load_data(fullname)
            info += '\nfilename: {}, size: {}, dict: {}'.format(fullname, sys.getsizeof(self.dict), self.dict)
        self.load_data(current)
        return info

    def append(self, key, value):
        self.dict[key] = value
        if sys.getsizeof(self.dict) > self.max_size:
            del self.dict[key]
            self.init_new_storage()
            self.dict[key] = value

    def find_and_open(self, key):
        filename_list = os.listdir('data')
        for filename in filename_list:
            fullname = 'data/{}'.format(filename)
            self.load_data(fullname)
            if key in self.dict:
                return fullname
        return None

    def load_data(self, filename):
        # print('load_data({})'.format(filename))
        if os.path.isfile(filename):
            with open(filename, 'rb') as file:
                self.dict = pickle.load(file)
                # print('loaded dict: {}'.format(self.dict))
                self.dict_file = filename

    def init_new_storage(self):
        if self.dict_file:
            self.save(self.dict_file)
        self.dict = {}
        self.dict_file = self.get_filename()
        self.save(self.dict_file)
        # print('init_new_storage: {}'.format(self.dict_file))

    def get_filename(self):
        file_number = 0
        while True:
            filename = 'data/{}.dt'.format(file_number)
            if not os.path.isfile(filename):
                return filename
            file_number += 1

    def save(self, filename):
        # print('save({})'.format(filename))
        with open(filename, 'wb') as file:
            pickle.dump(self.dict, file)

    def is_invalid_key(self, key):
        return not isinstance(key, (int, float, str))

    def is_invalid_key_or_value(self, key, value):
        return not (isinstance(key, (int, float, str)) and isinstance(value, (int, float, str)))


if __name__ == '__main__':
    # print('Welcome to KVstorage!')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('op')
    parser.add_argument('key_value', nargs='+')
    args = parser.parse_args()
    # print('path: {}, operation: {}, key and values: {}'.format(args.path, args.op, args.key_value))

    k = KVstorage()

    if args.op == 'set':
        # print('it is set operation')
        if len(args.key_value) < 2 or len(args.key_value) % 2 != 0:
            print('invalid key_value for set operation')
            del k
            exit(0)
        k[args.key_value[0]] = args.key_value[1]
    elif args.op == 'get':
        # print('it is get operation')
        if not args.key_value:
            print('invalid key_value for get operation')
            del k
            exit(0)
        print(k[args.key_value[0]])
    elif args.op == 'in':
        print('it is in operation')
    else:
        print('invalid operation!')

    del k
