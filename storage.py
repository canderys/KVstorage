from modules import KVstorage, StorageError
import argparse
import sys
import traceback

OPERATIONS = {
    "set": "set_operation",
    "get": "get_operation",
    "in": "in_operation",
    "del": "del_operation"
}


def parse(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=".", help="data storage directory")
    parser.add_argument("--size", type=int, default="1000", help="size factor")
    parser.add_argument(
        "--op", choices=OPERATIONS,
        help="operation", required=True)
    parser.add_argument("key_value", nargs="+")
    return parser.parse_args(args)


def run(args):
    parser = parse(args)
    kvstorage = KVstorage(parser.size, parser.path)
    op = parser.op
    return (", ".join(map(str, getattr(kvstorage,
                                       OPERATIONS[op])(parser.key_value))),
            kvstorage)


if __name__ == '__main__':
    try:
        print(run(sys.argv[1:])[0], file=sys.stdout)
    except StorageError as e:
        print(e, file=sys.stderr)
        sys.exit(e.CODE)
    except Exception:
        print(traceback.format_exc(), file=sys.stderr)
