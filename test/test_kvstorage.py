import unittest
import os
import sys
sys.path.append("..")
from kvstorage import KVstorage


class TestKVStorage(unittest.TestCase):

    def setUp(self):
        self.delete_list = []

    def test_count_files(self):
        kvstorage = KVstorage(1000)
        for i in range(100):
            kvstorage[i] = "test"
        filename_list = os.listdir(kvstorage.path)
        for count in range(3):
            self.assertTrue("{}.dt".format(count) in filename_list)
            count += 1
        for file in filename_list:
            self.delete_list.append(kvstorage.path + "/{}".format(file))

    def test_contains_data(self):
        kvstorage = KVstorage(1000)
        for i in range(100):
            kvstorage[i] = "test"
        kvstorage = KVstorage(1000)
        i = 101
        while i < 200:
            kvstorage[i] = "test"
            i += 1
        self.assertEqual(kvstorage[101], "test")
        self.assertEqual(kvstorage[1], "test")
        self.assertEqual(kvstorage[199], "test")
        for file in os.listdir(kvstorage.path):
            self.delete_list.append(kvstorage.path + "/{}".format(file))

    def test_standalone_operation(self):
        kvstorage = KVstorage(1000)
        key_value_set = []
        key_value_get = []
        key_value_get_res = []
        key_value_in_res = []
        for i in range(100):
            key_value_set.append(str(i))
            key_value_set.append('test {}'.format(i))
            key_value_get.append(str(i))
            key_value_get_res.append('test {}'.format(i))
            key_value_in_res.append(True)
        key_value_get.append('test_None')
        key_value_get_res.append(None)
        key_value_in_res.append(False)
        kvstorage.set_operation(key_value_set)
        test_get = kvstorage.get_operation(key_value_get)
        test_in = kvstorage.in_operation(key_value_get)
        self.assertEqual(test_get, key_value_get_res)
        self.assertEqual(test_in, key_value_in_res)

    def tearDown(self):
        for data in self.delete_list:
            try:
                os.remove(data)
            except OSError as e:
                print("unable to correctly delete test files", file=sys.stderr)
                print(e)


if __name__ == '__main__':
    unittest.main()
