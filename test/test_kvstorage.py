import KVStorage
import unittest
import os


class TestKVStorage(unittest.TestCase):
    def test_simple(self):
        kvstorage = KVStorage.KVStorage()
        kvstorage["test"] = 123
        self.assertEqual(kvstorage["test"], 123)
