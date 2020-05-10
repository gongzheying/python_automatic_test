import argparse
import logging.config
import re

from file_compare.compare import CompareType, CsiCompare, HotCompare


def parse_arg_type(v):
    # type: (str) -> None

    all_type = [CompareType.CSI, CompareType.HOT]
    for someType in all_type:
        if someType.name == str(v).upper():
            return someType.name

    all_type_name = [someType.name for someType in all_type]

    raise argparse.ArgumentTypeError("String '%s' does not match required format (choose %s)" % (v, all_type_name))


def parse_arg_bsp(v):
    # type: (str) -> str

    return str(v).upper()


def parse_arg_date(v):
    # type: (str) -> str

    try:
        return re.match(r"^(\d{4}-\d{2}-\d{2})$", v).group(0)
    except Exception:
        raise argparse.ArgumentTypeError("String '%s' does not match required format (yyyy-mm-dd )" % v)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", dest="type", help="file type", required=True, type=parse_arg_type)
    parser.add_argument("-b", dest="bsp", help="bsp code", required=True, type=parse_arg_bsp)
    parser.add_argument("-d", dest="date", help="create date", required=True, type=parse_arg_date)
    args = parser.parse_args()

    logging.config.fileConfig("logging.conf")

    compare_type = args.type
    bsp = args.bsp
    date = args.date

    comp = CsiCompare() if (compare_type == "CSI") else HotCompare()
    comp.run(bsp, date)
