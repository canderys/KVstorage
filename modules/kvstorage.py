import os
from pympler import asizeof
from .exceptions import StorageError
from .file_manager import FileManager
from .file_manager import KeyNotFoundError


def _get_size(obj):
    return asizeof.asizeof(obj)


class DataError(StorageError):
    CODE = 3


class MemoryError(StorageError):
    CODE = 2


class KVstorage:
    KEY_TYPES = (int, float, str)
    VALUE_TYPES = (int, float, str)

    def __init__(self, max_size=3000, path='.', delete_TMP=True):
        self.storage_RAM = {}
        self.max_size = int(max_size)
        self.path = os.path.join(path, "kv_storage_data")
        os.makedirs(self.path, exist_ok=True)
        self.file_manager = FileManager(self.path, max_size,
                                        _get_size, delete_TMP=delete_TMP)

    def __getitem__(self, key):
        if not KVstorage.is_valid_key(key):
            raise DataError("Error invalid key")
        if key in self.storage_RAM:
            return self.storage_RAM[key]
        else:
            return self.file_manager.get_value(key)

    def __setitem__(self, key, value):
        if not(KVstorage.is_valid_key(key) and
               KVstorage.is_valid_value(value)):
            raise DataError("Error: invalid key or value")
        if _get_size({key: value}) > self.max_size:
            raise MemoryError("Error: key value too big!")
        self.append(key, value)

    def __contains__(self, key):
        if not KVstorage.is_valid_key(key):
            raise DataError("Error: invalid key")
        if key in self.storage_RAM:
            return True
        try:
            self.file_manager.get_value(key)
        except KeyNotFoundError:
            return False
        return True

    def __delitem__(self, key):
        if not KVstorage.is_valid_key(key):
            raise DataError("Error: invalid key")
        if key in self.storage_RAM:
            del self.storage_RAM[key]
            return
        self.file_manager.delete_key(key)

    def append(self, key, value):
        self.storage_RAM[key] = value
        if _get_size(self.storage_RAM) > self.max_size:
            self.save_RAM()

    def save_RAM(self):
        for key, value in self.storage_RAM.items():
            self.file_manager.store_key_value(key, value)
        self.storage_RAM = {}

    def __del__(self):
        self.save_RAM()

    def delete_storage(self):
        self.storage_RAM = {}
        self.file_manager.clear()

    @staticmethod
    def is_valid_key(key):
        return isinstance(key, KVstorage.KEY_TYPES)

    @staticmethod
    def is_valid_value(key):
        return isinstance(key, KVstorage.VALUE_TYPES)

    def set_operation(self, key_value):
        try:
            key_value = [(key_value[i], key_value[i + 1])
                         for i in range(0, len(key_value), 2)]
        except IndexError:
            raise DataError("Error: Invalid key value data")
        for key, value in key_value:
            self[key] = value
        return ""

    def get_operation(self, keys):
        return [self[key] for key in keys]

    def in_operation(self, keys):
        return [key in self for key in keys]

    def del_operation(self, keys):
        for key in keys:
            del self[key]
        return ""
