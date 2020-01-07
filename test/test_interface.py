import unittest
from storage import parse, run
from modules.kvstorage import (MemoryError,
                               DataError)
from modules.file_manager import KeyNotFoundError


class TestInterfaceKVStorage(unittest.TestCase):
    def test_parse(self):
        parser = parse(
            ["--path", ".", "--size", "100", "--op", "set", "1", "2"])
        self.assertEqual(parser.size, 100)
        self.assertEqual(parser.path, ".")
        self.assertEqual(parser.op, "set")

    def test_run(self):
        string, kvstorage = run(["--path", ".", "--size", "500",
                                 "--op", "set", "1", "2", "3", "4"])
        kvstorage.save_RAM()
        self.assertEqual(kvstorage["1"], "2")
        self.assertEqual(kvstorage["3"], "4")
        string, kvstorage = run(["--op", "get", "1"])
        self.assertEqual(string, "2")
        string, kvstorage = run(["--op", "get", "3"])
        self.assertEqual(string, "4")
        string, kvstorage = run(["--op", "in", "1"])
        self.assertEqual("True", string)
        string, kvstorage = run(["--op", "in", "21"])
        self.assertEqual("False", string)
        string, kvstorage = run(["--op", "del", "1"])
        with self.assertRaises(KeyNotFoundError):
            string, kvstorage = run(["--op", "get", "1"])
        with self.assertRaises(MemoryError):
            run(["--path", ".", "--size", "1", "--op",
                 "set", "1", "2", "3", "4"])
        with self.assertRaises(DataError):
            run(["--path", ".", "--size", "1", "--op",
                 "set", "1", "2", "3"])
