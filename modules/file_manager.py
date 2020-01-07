import os
import pickle
import io
import shutil
import hashlib
from .exceptions import StorageError


def get_md5_hash(obj):
    value = hashlib.md5(str(obj).encode('utf-8')).hexdigest()
    return int(value, 16) % 1009


def is_empty(file):
    return os.stat(file).st_size == 0


class KeyNotFoundError(StorageError):
    CODE = 4


class FileManager:
    """class encapsulating file interaction"""

    def __init__(self, directory, max_size, get_size,
                 get_hash=get_md5_hash, delete_TMP=True):
        os.makedirs(directory, exist_ok=True)
        self.delete_TMP = delete_TMP
        self.extension = "fm"
        self.tmp_mark = "TMP"
        self.tmp_index = 1
        self.get_hash = get_hash
        self.directory = directory
        self.max_size = int(max_size)
        self.get_size = get_size
        self.cur_file = "0.{}".format(self.extension)
        self._initialization()

    def _initialization(self):
        for root, direct, files in os.walk(self.directory):
            for tmp_name in filter(self._is_tmp_file, files):
                tmp_path = os.path.join(root, tmp_name)
                file_name = os.path.join(root, self._get_file_by_tmp(tmp_name))
                if is_empty(tmp_path):
                    os.remove(tmp_path)
                    continue
                with open(tmp_path, "rb") as tmp,\
                        open(file_name, "wb") as cur_file:
                    cur_file.write(tmp.read())
                os.remove(tmp_path)

    def clear(self):
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)

    def get_value(self, key):
        key_hash = self.get_hash(key)
        self.path = os.path.join(self.directory, str(key_hash))
        for cur_dict in self._get_dicts():
            if key in cur_dict:
                return cur_dict[key]
        raise KeyNotFoundError("Error: There is no such key")

    def _get_file_by_tmp(self, tmp):
        splited_name = tmp.split(".")
        if len(splited_name) > 2:
            splited_name.remove(splited_name[self.tmp_index])
        return ".".join(splited_name)

    def get_full_path(self, file):
        return os.path.join(self.path, file)

    def _get_tmp_file(self):
        splited_name = self.cur_file.split(".")
        splited_name.insert(1, self.tmp_mark)
        return ".".join(splited_name)

    def _is_tmp_file(self, tmp_file):
        splited_name = tmp_file.split(".")
        return splited_name[self.tmp_index] == self.tmp_mark

    def _is_not_tmp(self, file):
        return self.is_storage_file(file)\
            and not self._is_tmp_file(file)

    def _save(self, obj):
        bytes_io = io.BytesIO()
        pickle.dump(obj, bytes_io)
        bytes_io.seek(0)
        self._write_data(bytes_io)

    def _write_data(self, buffer):
        tmp_file = self.get_full_path(self._get_tmp_file())
        cur_file = self.get_full_path(self.cur_file)
        with open(tmp_file, "wb") as file:
            file.write(buffer.read())
            buffer.seek(0)
        with open(cur_file, "wb") as file:
            file.write(buffer.read())
            buffer.seek(0)
        if self.delete_TMP:
            os.remove(tmp_file)

    def next_file(self):
        index = int(self.cur_file.split(".")[0]) + 1
        return "{}.{}".format(index, self.extension)

    def is_storage_file(self, file):
        return file.split(".")[-1] == self.extension

    def _get_dicts(self):
        for root, direct, files in os.walk(self.path):
            for file in filter(self._is_not_tmp, files):
                self.cur_file = file
                filename = os.path.join(root, file)
                with open(filename, "rb") as buffer:
                    yield pickle.load(buffer)

    def store_key_value(self, key, value, delete_TMP=True):
        key_hash = self.get_hash(key)
        self.path = os.path.join(self.directory, str(key_hash))
        os.makedirs(self.path, exist_ok=True)
        for cur_dict in self._get_dicts():
            if key in cur_dict:
                cur_dict[key] = value
                if self.get_size(cur_dict) > self.max_size:
                    del cur_dict[key]
                    self._save(cur_dict)
                    continue
            else:
                cur_dict[key] = value
            if self.get_size(cur_dict) <= self.max_size:
                self._save(cur_dict)
                return
        self.cur_file = self.next_file()
        self._save({key: value})

    def delete_key(self, key):
        key_hash = self.get_hash(key)
        self.path = os.path.join(self.directory, str(key_hash))
        for cur_dict in self._get_dicts():
            if key in cur_dict:
                del cur_dict[key]
                self._save(cur_dict)
                return
        raise KeyNotFoundError("Error: There is no such key")
