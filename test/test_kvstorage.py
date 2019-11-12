import KVstorage
import unittest
import os


class TestKVStorage(unittest.TestCase):
    def test_simple(self):
        kvstorage = KVstorage.KVstorage()
        kvstorage["test"] = 123
        self.assertEqual(kvstorage["test"], 123)
