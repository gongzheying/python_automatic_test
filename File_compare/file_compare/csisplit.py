import os
import shutil
from datetime import datetime
from os import path


class CsiSplit:
    CONF_PATH = "config.txt"
    OLD_DEFAULT_PATH = "./oldPath"
    NEW_DEFAULT_PATH = "./newPath"

    def __init__(self):
        pass

    @staticmethod
    def is_cat(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CAT" == s[0:3]

    @staticmethod
    def is_cbh(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CBH" == s[0:3]

    @staticmethod
    def is_cbt(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CBT" == s[0:3]

    @staticmethod
    def is_cfh(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CFH" == s[0:3]

    @staticmethod
    def is_cft(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CFT" == s[0:3]

    @staticmethod
    def is_cih(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CIH" == s[0:3]

    @staticmethod
    def is_cit(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CIT" == s[0:3]

    @staticmethod
    def is_tbt(s):
        # type: (str) -> bool
        return len(s) >= 3 and "TBT" == s[0:3]

    @staticmethod
    def is_tfh(s):
        # type: (str) -> bool
        return len(s) >= 3 and "TFH" == s[0:3]

    @staticmethod
    def is_tfs(s):
        # type: (str) -> bool
        return len(s) >= 3 and "TFS" == s[0:3]

    @staticmethod
    def is_detail(s):
        # type: (str) -> bool
        return len(s) >= 3 and "CBR COR COT COA".find(s[0:3]) != -1

    def cut_dir(self, csi_file, base_path):
        # type: (str, str) -> None

        if path.isdir(csi_file):
            for child_name in os.listdir(csi_file):
                self.cut_dir(path.join(csi_file, child_name), base_path)
        else:
            csi_file_header = ""
            with open(csi_file, "rt") as csi_file_handle:
                csi_file_header = csi_file_handle.readline()

            if self.is_tfh(csi_file_header):
                self.cut_cap_file(csi_file, path.join(base_path, "cap"))
            elif self.is_cfh(csi_file_header):
                self.cut_file(csi_file, path.join(base_path, "dish"))

    def cut_cap_file(self, csi_file, base_path):
        # type: (str,str) -> None

        csi_file_name = path.basename(csi_file)

        print "cut file :{} is beginning!".format(csi_file_name)

        csi_file_root_path = path.join(base_path, csi_file_name)
        if path.exists(csi_file_root_path):
            if path.isdir(csi_file_root_path):
                shutil.rmtree(csi_file_root_path)
            else:
                os.remove(csi_file_root_path)
        os.makedirs(csi_file_root_path)

        with open(path.join(csi_file_root_path, "filename.txt"), "w+t") as csi_file_section:
            csi_file_section.write(csi_file_name)
            csi_file_section.write("\n")
            csi_file_section.flush()

        csi_file_section = open(path.join(csi_file_root_path, "TFH.txt"), "w+t")
        with open(csi_file, "rt") as csi_file_all:

            csf_file_section_data = []

            for line in csi_file_all:
                if self.is_detail(line):
                    csf_file_section_data.append(line)

                elif self.is_tbt(line):
                    csi_file_section.close()

                    merchant_no = line[12:27].strip()
                    csi_file_tbt_root = path.join(csi_file_root_path, "TBT{}".format(merchant_no))
                    os.makedirs(csi_file_tbt_root)

                    csi_file_section = open(path.join(csi_file_tbt_root, "{}Detail.txt".format(merchant_no)), "w+t")
                    csi_file_section.write("\n".join(csf_file_section_data))
                    csi_file_section.close()

                    csf_file_section_data = []
                    csi_file_section = open(path.join(csi_file_tbt_root, "TBT{}.txt".format(merchant_no)), "w+t")

                elif self.is_tfs(line):
                    csi_file_section.close()

                    csi_file_section = open(path.join(csi_file_root_path, "TFS.txt"), "w+t")
                    csi_file_section.write(line)
                    csi_file_section.write("\n")
                    csi_file_section.close()

                if not self.is_tfs(line):
                    csi_file_section.write(line)
                    csi_file_section.write("\n")
                    csi_file_section.flush()

        csi_file_section.close()
        print "cut file :{} is complete!".format(csi_file_name)

    def cut_file(self, csi_file, base_path):
        # type: (str,str) -> None

        csi_file_name = path.basename(csi_file)

        print "cut file :{} is beginning!".format(csi_file_name)

        csi_file_root_path = path.join(base_path, path.basename(csi_file))
        if path.exists(csi_file_root_path):
            if path.isdir(csi_file_root_path):
                shutil.rmtree(csi_file_root_path)
            else:
                os.remove(csi_file_root_path)
        os.makedirs(csi_file_root_path)

        with open(path.join(csi_file_root_path, "filename.txt"), "w+t") as csi_file_section:
            csi_file_section.write(csi_file_name)
            csi_file_section.write("\n")
            csi_file_section.flush()

        count_cfh = 1
        count_cih = 1
        count_cbh = 1
        is_detail = False

        csi_file_cbh_root = None
        csi_file_cih_root = None

        csi_file_cfh_root = path.join(csi_file_root_path, "CFH-{}".format(count_cfh))
        os.makedirs(csi_file_cfh_root)
        csi_file_section = open(path.join(csi_file_cfh_root, "cfh.txt"), "w+t")
        with open(csi_file, "rt") as csi_file_all:
            for line in csi_file_all:

                if self.is_cfh(line):
                    csi_file_section.close()

                    csi_file_cfh_root = path.join(csi_file_root_path, "CFH-{}".format(count_cfh))
                    os.makedirs(csi_file_cfh_root)

                    csi_file_section = open(path.join(csi_file_cfh_root, "cfh.txt"), "w+t")
                    count_cfh += 1
                    count_cih = 1

                elif self.is_cih(line):
                    csi_file_section.close()

                    cco_air = line[11:16]
                    csi_file_cih_root = path.join(csi_file_cfh_root, "CIH-{}".format(cco_air))
                    os.makedirs(csi_file_cih_root)

                    csi_file_section = open(path.join(csi_file_cih_root, "cih.txt"), "w+t")
                    count_cih += 1
                    count_cbh = 1

                elif self.is_cbh(line):
                    csi_file_section.close()

                    cco_agent = line[54:62]
                    csi_file_cbh_root = path.join(csi_file_cih_root, "CBH-{}".format(cco_agent))
                    os.makedirs(csi_file_cbh_root)

                    csi_file_section = open(path.join(csi_file_cbh_root, "cbh.txt"), "w+t")

                    count_cbh += 1

                elif self.is_detail(line):
                    if not is_detail:
                        csi_file_section.close()

                        csi_file_section = open(path.join(csi_file_cbh_root, "detail.txt"), "w+t")

                    csi_file_section.write(line)
                    csi_file_section.write("\n")

                    is_detail = True

                elif self.is_cbt(line):
                    csi_file_section.close()

                    csi_file_section = open(path.join(csi_file_cbh_root, "cbt.txt"), "w+t")

                elif self.is_cat(line):
                    csi_file_section.close()

                    csi_file_section = open(path.join(csi_file_cbh_root, "cat.txt"), "w+t")

                elif self.is_cit(line):
                    csi_file_section.close()

                    csi_file_section = open(path.join(csi_file_cih_root, "cit.txt"), "w+t")

                elif self.is_cft(line):
                    csi_file_section.close()

                    csi_file_section = open(path.join(csi_file_cfh_root, "cft.txt"), "w+t")

                csi_file_section.write(line)
                csi_file_section.write("\n")
                csi_file_section.flush()
                is_detail = False

        csi_file_section.close()
        print "cut file :{} is complete!".format(csi_file_name)

    def run(self, conf_path=CONF_PATH):

        old_src_dir = None
        new_src_dir = None
        old_split_dir = self.OLD_DEFAULT_PATH
        new_split_dir = self.NEW_DEFAULT_PATH

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
            return

        print "[{}] Begin to cut old files.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
        self.cut_dir(old_src_dir, old_split_dir)
        print "[{}] Complete.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))

        print "ISIS2 file path:{}".format(new_src_dir)

        if not path.exists(new_src_dir):
            print "Invalid new path!"
            return

        print "[{}] Begin to cut new files.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
        self.cut_dir(new_src_dir, new_split_dir)
        print "[{}] Complete.".format(datetime.now().strftime("%b %d %Y %H:%M:%S"))
