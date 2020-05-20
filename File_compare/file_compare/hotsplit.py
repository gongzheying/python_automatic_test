import os
import shutil
from datetime import datetime
from os import path

CONF_PATH = "config.txt"
OLD_DEFAULT_PATH = "./oldPath"
NEW_DEFAULT_PATH = "./newPath"


def run(conf_path=CONF_PATH):
    # type: (str) -> bool

    old_src_dir = None
    new_src_dir = None
    old_split_dir = OLD_DEFAULT_PATH
    new_split_dir = NEW_DEFAULT_PATH

    with open(conf_path, "rt") as conf_file:
        for line in conf_file:
            if line.startswith("old="):
                old_src_dir = line[line.index("=") + 1:].strip()
            if line.startswith("new="):
                new_src_dir = line[line.index("=") + 1:].strip()
            if line.startswith("oldPath="):
                old_split_dir = line[line.index("=") + 1:].strip()
            if line.startswith("newPath="):
                new_split_dir = line[line.index("=") + 1:].strip()

    print "ISIS file path:{}".format(old_src_dir)

    if not path.exists(old_src_dir):
        print "Invalid old path!"
        return False

    print "[{}] Begin to cut old files.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
    HotSplit.cut_dir(old_src_dir, old_split_dir)
    print "[{}] Complete.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))

    print "ISIS2 file path:{}".format(new_src_dir)

    if not path.exists(new_src_dir):
        print "Invalid new path!"
        return False

    print "[{}] Begin to cut new files.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
    HotSplit.cut_dir(new_src_dir, new_split_dir)
    print "[{}] Complete.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
    return True

class HotSplit(object):

    @staticmethod
    def is01(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BFH" == s[0:3]

    @staticmethod
    def is02(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BCH" == s[0:3]

    @staticmethod
    def is03(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BOH" == s[0:3]

    @staticmethod
    def is06(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BKT" == s[0:3]

    @staticmethod
    def is24(s):
        # type: (str) -> bool
        return len(s) >= 13 and "BKS" == s[0:3] and "24" == s[11:13]

    @staticmethod
    def is93(s):
        # type: (str) -> bool
        return len(s) >= 13 and "BOT" == s[0:3] and "93" == s[11:13]

    @staticmethod
    def is94(s):
        # type: (str) -> bool
        return len(s) >= 13 and "BOT" == s[0:3] and "94" == s[11:13]

    @staticmethod
    def is95(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BCT" == s[0:3]

    @staticmethod
    def is99(s):
        # type: (str) -> bool
        return len(s) >= 3 and "BFT" == s[0:3]

    @staticmethod
    def is_a(s):
        # type: (str) -> bool
        return s.startswith("A")

    @staticmethod
    def cut_dir(hot_file, base_path):
        # type: (str,str) -> None

        if path.isdir(hot_file):
            for child_name in os.listdir(hot_file):
                HotSplit.cut_dir(path.join(hot_file, child_name), base_path)
        else:
            HotSplit.cut_file(hot_file, base_path)

    @staticmethod
    def cut_file(hot_file, base_path):
        # type: (str,str) -> None

        hot_file_name = path.basename(hot_file)

        print "cut file :{} is beginning!".format(hot_file_name)

        hot_file_root_path = path.join(base_path, path.basename(hot_file))
        if path.exists(hot_file_root_path):
            if path.isdir(hot_file_root_path):
                shutil.rmtree(hot_file_root_path)
            else:
                os.remove(hot_file_root_path)
        os.makedirs(hot_file_root_path)

        doc_num = None
        last_record = 1
        count01 = 2
        count02 = 1
        count95 = 1
        count99 = 1
        hot_file_agent_path = None

        hot_file_section = open(path.join(hot_file_root_path, "BFH01.txt"), "w+t")
        with open(hot_file, "rt") as hot_file_all:
            for line in hot_file_all:

                if HotSplit.is_a(line):
                    hot_file_section.close()
                    break

                if HotSplit.is01(line) and last_record != -1:
                    hot_file_section.close()

                    hot_file_section = open(path.join(hot_file_root_path, "BFH01-{}.txt".format(count01)), "w+t")
                    count01 += 1
                    doc_num = None
                    last_record = 1

                elif HotSplit.is02(line):
                    hot_file_section.close()

                    hot_file_section = open(path.join(hot_file_root_path, "BCH02-{}.txt".format(count02)), "w+t")
                    count02 += 1
                    doc_num = None
                    last_record = 2

                elif HotSplit.is03(line):
                    hot_file_section.close()

                    hot_file_agent_path = path.join(hot_file_root_path, line[13:21].strip())
                    if not path.exists(hot_file_agent_path):
                        os.makedirs(hot_file_agent_path)

                    hot_file_section = open(path.join(hot_file_agent_path, "BOH03.txt"), "w+t")
                    doc_num = None
                    last_record = 3

                elif HotSplit.is06(line):
                    hot_file_section.close()
                    hot_file_agent_name = hot_file_section.name
                    if "tmp" == path.basename(hot_file_agent_name):
                        hot_file_agent_rename = path.join(hot_file_agent_path, "{}.txt".format(doc_num))
                        if path.exists(hot_file_agent_rename):
                            os.remove(hot_file_agent_rename)
                        os.rename(hot_file_agent_name, hot_file_agent_rename)

                    hot_file_section = open(path.join(hot_file_agent_path, "tmp"), "w+t")
                    doc_num = None
                    last_record = 6

                elif HotSplit.is24(line):
                    doc_num = line[28:38].strip()

                elif HotSplit.is93(line):

                    if last_record != 93:
                        hot_file_section.close()
                        hot_file_agent_name = hot_file_section.name
                        if "tmp" == path.basename(hot_file_agent_name):
                            hot_file_agent_rename = path.join(hot_file_agent_path, "{}.txt".format(doc_num))
                            if path.exists(hot_file_agent_rename):
                                os.remove(hot_file_agent_rename)
                            os.rename(hot_file_agent_name, hot_file_agent_rename)

                        hot_file_section = open(path.join(hot_file_agent_path, "BOT93.txt"), "w+t")
                        doc_num = None

                    last_record = 93

                elif HotSplit.is94(line):
                    if last_record != 94:
                        hot_file_section.close()
                        hot_file_section = open(path.join(hot_file_agent_path, "BOT94.txt"), "w+t")
                        doc_num = None
                    last_record = 94

                elif HotSplit.is95(line):
                    if last_record != 95:
                        hot_file_section.close()
                        hot_file_section = open(path.join(hot_file_root_path, "BCT95-{}.txt".format(count95)), "w+t")
                        count95 += 1
                        doc_num = None
                    last_record = 95

                elif HotSplit.is99(line):

                    hot_file_section.close()
                    hot_file_section = open(path.join(hot_file_root_path, "BFT99-{}.txt".format(count99)), "w+t")
                    count99 += 1
                    doc_num = None
                    last_record = 99

                hot_file_section.write(line)
                hot_file_section.write("\n")
                hot_file_section.flush()
        hot_file_section.close()
        print "cut file :{} is complete!".format(hot_file_name)
