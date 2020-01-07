import unittest
import os
import pickle
import tempfile
from modules import KVstorage


class TestInrerruptKVStorage(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.path = tmp_dir
        self.kvstorage = KVstorage(path=self.path, delete_TMP=False)
        for i in range(300):
            self.kvstorage["test{}".format(i)] = i
        self.kvstorage.save_RAM()
        self.file_manager = self.kvstorage.file_manager
        self.path = self.file_manager.directory

    def test_load_tmp(self):
        for root, direct, files in os.walk(self.path):
            for file in filter(self.file_manager._is_tmp_file, files):
                file_path = os.path.join(root, file)
                cur_dict = None
                with open(file_path, "rb") as buffer:
                    cur_dict = pickle.load(buffer)
                    cur_dict["check"] = 666
                with open(file_path, "wb") as buffer:
                    pickle.dump(cur_dict, buffer)
        self.kvstorage.file_manager._initialization()
        for root, direct, files in os.walk(self.path):
            for file in filter(self.file_manager.is_storage_file, files):
                file_path = os.path.join(root, file)
                self.assertEqual(False, self.file_manager._is_tmp_file(file))
                with open(file_path, "rb") as buffer:
                    cur_dict = pickle.load(buffer)
                    self.assertEqual(cur_dict["check"], 666)

    def test_empty_tmp(self):
        for root, direct, files in os.walk(self.path):
            for file in filter(self.file_manager._is_tmp_file, files):
                file_path = os.path.join(root, file)
                with open(file_path, "w") as buffer:
                    buffer.write("")
        self.kvstorage.file_manager._initialization()
        for root, direct, files in os.walk(self.path):
            for file in filter(self.file_manager.is_storage_file, files):
                file_path = os.path.join(root, file)
                self.assertEqual(False, self.file_manager._is_tmp_file(file))
                with open(file_path, "rb") as buffer:
                    cur_dict = pickle.load(buffer)
                first = list(cur_dict.keys())[0]
                for key in cur_dict:
                    self.assertEqual(self.file_manager.get_hash(first),
                                     self.file_manager.get_hash(key))

    def tearDown(self):
        self.kvstorage.delete_storage()
