from kvstorage import KVstorage
import unittest
import os
import sys
sys.path.append("..")


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
            kvstorage[i] = "test {}".format(i)
        kvstorage = KVstorage(1000)
        for i in range(101, 200):
            kvstorage[i] = "test {}".format(i)
            i += 1
        self.assertEqual(kvstorage[101], "test {}".format(101))
        self.assertEqual(kvstorage[1], "test {}".format(1))
        self.assertEqual(kvstorage[199], "test {}".format(199))
        for file in os.listdir(kvstorage.path):
            self.delete_list.append(kvstorage.path + "/{}".format(file))

    def test_standalone_app(self):
        import subprocess

        def invoke(path, cmd, key_values):
            # тут было две точки перед слешом, так и должно быть? просто у меня не работало
            cmd_format = 'python ./kvstorage.py {} {} {}'
            with subprocess.Popen(cmd_format.format(path, cmd, key_values),
                                  stdout=subprocess.PIPE) as proc:
                return proc.stdout.read()

        invoke('.', 'set', '1 test')
        res = invoke('.', 'get', '1')
        str_res = res.decode().replace('\r\n', '')
        self.assertEqual(str_res, 'test')
        import shutil
        shutil.rmtree('kv_storage_data', ignore_errors=False, onerror=None)

    def tearDown(self):
        for data in self.delete_list:
            try:
                os.remove(data)
            except OSError as e:
                print("unable to correctly delete test files", file=sys.stderr)
                print(e)


if __name__ == '__main__':
    unittest.main()
