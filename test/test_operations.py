import unittest
from modules import KVstorage
from modules import KeyNotFoundError


class TestOperationsKVStorage(unittest.TestCase):
    def setUp(self):
        self.kvstorage = KVstorage()

    def test_set(self):
        self.kvstorage[123] = 1
        self.assertEqual(self.kvstorage[123], 1)
        self.kvstorage[123] = 7
        self.assertEqual(self.kvstorage[123], 7)
        self.kvstorage.save_RAM()
        self.assertEqual(self.kvstorage[123], 7)

    def test_get_wrong(self):
        with self.assertRaises(KeyNotFoundError):
            self.kvstorage["not exist key"]

    def test_del(self):
        self.kvstorage[76] = 1
        self.assertEqual(self.kvstorage[76], 1)
        del self.kvstorage[76]
        self.kvstorage[76] = 1
        self.assertEqual(self.kvstorage[76], 1)
        self.kvstorage.save_RAM()
        del self.kvstorage[76]
        with self.assertRaises(KeyNotFoundError):
            self.assertEqual(self.kvstorage[76], None)
        with self.assertRaises(KeyNotFoundError):
            del self.kvstorage["not exist key"]

    def test_files(self):
        for i in range(300):
            self.kvstorage["test{}".format(i)] = i
        self.kvstorage.save_RAM()
        for i in range(300):
            self.assertEqual(self.kvstorage["test{}".format(i)], i)

    def test_in(self):
        self.kvstorage["test_data"] = 15
        self.assertEqual(True, "test_data" in self.kvstorage)
        self.kvstorage.save_RAM()
        self.assertEqual(self.kvstorage.storage_RAM, {})
        self.assertEqual(True, "test_data" in self.kvstorage)
        self.assertEqual(False, "not exist key" in self.kvstorage)

    def tearDown(self):
        self.kvstorage.delete_storage()
