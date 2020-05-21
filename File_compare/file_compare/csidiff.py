import os
import re
from csv import DictWriter

from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew
from file_compare.filestructure import CompareRoot, CompareRecord, RecordFormat

CONF_PATH = "config.txt"
OLD_DEFAULT_PATH = "./oldPath"
NEW_DEFAULT_PATH = "./newPath"
RESULT_DEFAULT_PATH = "./result"

COLUMNS = ("ISIS File Name",
           "CSI Date",
           "PCA",
           "Airline",
           "Format",
           "Error type",
           "Error description",
           "CSI Record Name",
           "Document number",
           "ISIS VALUE",
           "ISIS2 VALUE",
           "Filed")


def run(conf_path=CONF_PATH):
    # type: (str) -> bool

    old_split_dir = OLD_DEFAULT_PATH
    new_split_dir = NEW_DEFAULT_PATH
    result_path = RESULT_DEFAULT_PATH

    with open(conf_path, "rt") as conf_file:
        for line in conf_file:
            if line.startswith("oldPath="):
                old_split_dir = line[line.index("=") + 1:].strip()
            if line.startswith("newPath="):
                new_split_dir = line[line.index("=") + 1:].strip()
            if line.startswith("resultPath="):
                result_path = line[line.index('=') + 1:].strip()

    with open(os.path.join(result_path, "csi_diff.csv"), "w+t") as diff_file:
        diff_writer = DictWriter(diff_file, COLUMNS)
        diff_writer.writeheader()
        CompareCSIDiff(old_split_dir, new_split_dir, result_path).process_compare(diff_writer)

    return True


class CompareCSIDiff(object):

    def __init__(self, old_split_dir, new_split_dir, result_path):
        # type: (str,str,str) -> CompareCSIDiff
        self.__old_split_dir = old_split_dir
        self.__new_split_dir = new_split_dir
        self.__result_path = result_path

        self.__curFileName = ""
        self.__pca = ""
        self.__airlineCode = ""
        self.__csiDate = ""
        self.__docNum = ""
        self.__csiFormat = ""
        self.__oldFileName = ""

        self.__logs = list()
        self.__root = CompareRoot.get_hot_root()

    def process_compare(self, diff_writer):
        # type: (DictWriter) -> None

        filedao.clear_data()

        # Get all files
        old_map = self.__refresh_file_list(self.__old_split_dir, False)
        new_map = self.__refresh_file_list(self.__new_split_dir, True)

        # Put the comparison in the same table
        filedao.into_same_record()

        # Processing missing files
        self.__compare_more_file(old_map, new_map, diff_writer, False)
        self.__compare_more_file(new_map, old_map, diff_writer, True)

        page = 0
        index = 0
        count = filedao.count_same_record()
        while index <= count:
            index = page * 500
            same_list = filedao.query_same_record(index)
            for vo in same_list:
                self.__compare_file(vo, diff_writer)

            page += 1

    def __compare_file(self, vo, diff_writer):
        # type: (SplitFileInfoVO, DictWriter) -> None

        old_file = vo.full_name
        new_file = vo.new_full_name

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.

        self.__process_log(old_file, diff_writer)

        self.__curFileName = old_file

        # /(cap|dish)/origin_csi_file/**/*.txt
        file_names = vo.path.split(os.path.sep)

        self.__csiFormat = file_names[1]
        self.__oldFileName = file_names[2]

        self.__get_fields(self.__oldFileName, self.__csiFormat)

        # read file
        old_list = [line.strip() for line in open(old_file) if len(line.strip()) > 0]
        new_list = [line.strip() for line in open(new_file) if len(line.strip()) > 0]

        self.__compare_file_list(self.__root.format(self.__csiFormat), old_list, new_list, True, diff_writer)

    def __compare_file_list(self, record_format, old_list, new_list, check_group, diff_writer):
        # type: (RecordFormat,list,list,bool,DictWriter) -> None
        index = 0
        new_index = 0
        while index < len(old_list) and new_index < len(new_list):
            line = old_list[index]
            new_line = new_list[new_index]

            if len(line) > 3:
                element = self.__get_element(line)
                new_element = self.__get_element(new_line)

                record_content = record_format.content(element)
                record = CompareRecord(line, record_content)

                new_record_content = record_format.content(new_element)
                new_record = CompareRecord(new_line, new_record_content)

                is_break = False
                while element != new_element:

                    # Find a correspondence in the old record, indicating that the current old
                    # record is out, record and skip the old record
                    if self.__find_same_record(new_element, old_list):
                        index += 1
                        comment = record_content.comment
                        comment_desc = record_content.comment_desc

                        diff_writer.writerow({
                            "ISIS File Name": self.__oldFileName,
                            "CSI Date": self.__csiDate,
                            "PCA": self.__pca,
                            "Airline": "",
                            "Format": self.__csiFormat,
                            "Error type": comment,
                            "Error description": comment_desc,
                            "CSI Record Name": line[0:11],
                            "Document number": "",
                            "ISIS VALUE": "",
                            "ISIS2 VALUE": "",
                            "Filed": ""
                        })
                        is_break = True
                        break
                    else:  # Otherwise the new record is out of the record and skips the new record

                        comment = new_record_content.comment
                        comment_desc = new_record_content.comment_desc

                        diff_writer.writerow({
                            "ISIS File Name": self.__oldFileName,
                            "CSI Date": self.__csiDate,
                            "PCA": self.__pca,
                            "Airline": "",
                            "Format": self.__csiFormat,
                            "Error type": comment,
                            "Error description": comment_desc,
                            "CSI Record Name": new_line[0:11],
                            "Document number": "",
                            "ISIS VALUE": "",
                            "ISIS2 VALUE": "",
                            "Filed": ""
                        })

                        new_index += 1
                        if new_index >= len(new_list):
                            is_break = True
                            break
                        new_line = new_list[new_index]
                        new_element = self.__get_element(new_line)
                        new_record_content = record_format.content(new_element)
                        new_record = CompareRecord(new_line, new_record_content)

                if is_break:
                    continue

                if check_group:
                    if record.group():
                        group_list = self.__get_group(record_format, index, old_list)
                        new_group_list = self.__get_group(record_format, new_index, new_list)
                        more = self.__is_same_group(record_format, group_list, new_group_list, diff_writer)
                        if "" == more:
                            self.__compare_file_list(record_format, group_list, new_group_list, False, diff_writer)
                            index += len(group_list)
                            new_index += len(new_group_list)
                        elif "old" == more:
                            index += len(group_list)
                        else:
                            new_index += len(new_group_list)
                        continue

                new_record_content = record_format.content(new_element)
                new_record = CompareRecord(new_line, new_record_content)
                self.__compare_element(record, new_record)

            index += 1
            new_index += 1

        # The same line after comparison to see if there is no record than the record

        self.__process_overplus_line(record_format, index, old_list, False)

        self.__process_overplus_line(record_format, new_index, new_list, True)

        # if end replace docNum
        if not check_group:
            self.__replace_log()

    def __find_same_record(self, new_element, old_list):
        # type: (str,list) -> bool

        for line in old_list:
            if new_element == self.__get_element(line):
                return True
        return False

    def __get_group(self, record_format, index, lines):
        # type: (RecordFormat,int,list) -> list

        group = list()
        while index < len(lines):
            line = lines[index]
            element = self.__get_element(line)
            record_content = record_format.content(element)
            record = CompareRecord(line, record_content)

            if record.group_end and len(group) >= 1:
                break

            if record.group():
                group.append(line)

            index += 1

        return group

    def __is_same_group(self, record_format, old_list, new_list, diff_writer):
        # type: (RecordFormat,list,list,DictWriter) -> str

        index = 0
        new_index = 0
        doc_num = "0"
        new_doc_num = "0"
        element = self.__get_element(old_list[0])
        end_element = self.__get_element(old_list[len(old_list) - 1])

        while index < len(old_list) and new_index < len(new_list):
            line = old_list[index]
            new_line = new_list[new_index]
            doc = self.__get_doc_num(line, record_format)
            new_doc = self.__get_doc_num(new_line, record_format)
            if "" != doc:
                doc_num = doc

            if "" != new_doc:
                new_doc_num = new_doc

            index += 1
            new_index += 1

        same_group = doc_num == new_doc_num
        if not same_group:
            num = long(doc_num.strip())
            new_num = long(new_doc_num.strip())
            if num > new_num:
                comment = "D_CSI_pymt"
                value = new_list[0][0: 11]
                safm = new_list[0][157: 168]
                if "00000000000" == safm:
                    comment = "D_CSI_72"

                diff_writer.writerow({
                    "ISIS File Name": self.__oldFileName,
                    "CSI Date": self.__csiDate,
                    "PCA": self.__pca,
                    "Airline": "",
                    "Format": self.__csiFormat,
                    "Error type": comment,
                    "Error description": "lack of payment",
                    "CSI Record Name": "{}-{}".format(element, end_element),
                    "Document number": ":{}".format(new_num),
                    "ISIS VALUE": "",
                    "ISIS2 VALUE": value,
                    "Filed": ""
                })

                return "new"
            else:
                value = old_list[0][0: 11]

                diff_writer.writerow({
                    "ISIS File Name": self.__oldFileName,
                    "CSI Date": self.__csiDate,
                    "PCA": self.__pca,
                    "Airline": "",
                    "Format": self.__csiFormat,
                    "Error type": "D_CSI_pymt",
                    "Error description": "lack of payment",
                    "CSI Record Name": "{}-{}".format(element, end_element),
                    "Document number": ":{}".format(num),
                    "ISIS VALUE": value,
                    "ISIS2 VALUE": "",
                    "Filed": ""
                })

                return "old"

        return ""

    def __compare_element(self, record, new_record):
        # type: (CompareRecord,CompareRecord) -> None

        record.parse_record()
        new_record.parse_record()

        old_map = record.element_map
        new_map = new_record.element_map

        doc_num_str = ""
        if record.group:
            doc_num_str = "docNum"

        for key in old_map:

            old_value = ":{}".format(old_map.get(key))
            new_value = ":{}".format(new_map.get(key))

            if record.csi_date(key):
                self.__csiDate = new_value

            if record.doc_num(key):
                self.__docNum = ":{}".format(old_value)

            if old_value != new_value:
                comment = record.comment(key)
                desc = record.comment_desc(key)

                error = {
                    "ISIS File Name": self.__oldFileName,
                    "CSI Date": "",
                    "PCA": "",
                    "Airline": "",
                    "Format": self.__csiFormat,
                    "Error type": comment,
                    "Error description": desc,
                    "CSI Record Name": record.content.name,
                    "Document number": doc_num_str,
                    "ISIS VALUE": old_value,
                    "ISIS2 VALUE": new_value,
                    "Filed": key
                }

                self.__logs.append(error)

    def __replace_log(self):
        # type: () -> None
        for log in self.__logs:
            if "docNum" == log.get("Document number"):
                log["Document number"] = self.__docNum
        self.__docNum = ""

    def __more_file(self, record_format, line_list, new):
        # type: (RecordFormat, list, bool) -> None
        index = 0
        while index < len(line_list):
            doc_num = ""
            group_list = self.__get_group(record_format, index, line_list)
            for line in group_list:
                doc = self.__get_doc_num(line, record_format)
                if "" != doc:
                    doc_num = ":{}".format(doc)
            error = {
                "ISIS File Name": self.__oldFileName,
                "CSI Date": self.__csiDate,
                "PCA": self.__pca,
                "Airline": "",
                "Format": self.__csiFormat,
                "Error type": "D_CSI_pymt",
                "Error description": "lack of payment",
                "CSI Record Name": "",
                "Document number": doc_num,
                "ISIS VALUE": "N" if new else "Y",
                "ISIS2 VALUE": "Y" if new else "N",
                "Filed": ""
            }

            self.__logs.append(error)

    def __process_overplus_line(self, record_format, index, line_list, new):
        # type: (RecordFormat, int,list,bool) -> None
        if index != len(line_list):
            line = line_list[index]
            element = self.__get_element(line)
            record_content = record_format.content(element)

            # Same ticket or not a row of tickets, new tickets are processed by more votes and fewer votes
            if record_content.group_end:

                self.__more_file(record_format, line_list[index:], new)

            else:
                for i in range(index, len(line_list)):
                    line = line_list[i]
                    element = self.__get_element(line)
                    record_content = record_format.content(element)

                    error = {
                        "ISIS File Name": self.__oldFileName,
                        "CSI Date": self.__csiDate,
                        "PCA": self.__pca,
                        "Airline": "",
                        "Format": self.__csiFormat,
                        "Error type": record_content.comment,
                        "Error description": record_content.comment_desc,
                        "CSI Record Name": line,
                        "Document number": "",
                        "ISIS VALUE": "N" if new else "Y",
                        "ISIS2 VALUE": "Y" if new else "N",
                        "Filed": ""
                    }

                    self.__logs.append(error)

    def __process_log(self, filename, diff_writer):
        # type: (str,DictWriter) -> None

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.

        if "" != self.__curFileName:
            cur_path = os.path.split(self.__curFileName)[0]
            path = os.path.split(filename)[0]
            if cur_path != path:
                for log in self.__logs:
                    log["Airline"] = self.__airlineCode
                    log["CSI Date"] = self.__csiDate
                    log["PCA"] = self.__pca
                    diff_writer.writerow(log)

                self.__airlineCode = ""
                self.__csiDate = ""
                self.__pca = ""
                self.__docNum = ""
                self.__logs = list()

    def __get_fields(self, old_name, csi_format):
        # type: (str,str) -> None
        if "cap" == csi_format:
            self.__pca = "AMEX"
        else:
            if old_name.startswith("c"):  # Complete CSI
                self.__pca = ""
                self.__airlineCode = old_name[len(old_name) - 3:]
            else:
                if len(old_name) == 7 or len(old_name) == 8 or len(old_name) == 13:
                    self.__pca = old_name[0: 2]

                elif len(old_name) == 12:
                    end = old_name[10:]
                    if re.search(".*([1-2]$)", end) is not None:
                        self.__pca = old_name[4:8]
                    else:
                        self.___pca = old_name[0:2]

    def __compare_more_file(self, file_map, ref_file_map, diff_writer, new):
        # type: (dict,dict,DictWriter,bool)->None

        #  Less entire source files
        file_names = ["'temp'"]
        for key in file_map:
            if key not in ref_file_map:
                file_names.append("'{}'".format(key))
                if new:
                    diff_writer.writerow({
                        "ISIS value": "N",
                        "ISIS2 value": "Y",
                        "ISIS File Name": key
                    })
                else:
                    diff_writer.writerow({
                        "ISIS value": "Y",
                        "ISIS2 value": "N",
                        "ISIS File Name": key
                    })

        more_files = ",".join(file_names)

        # Less tickets
        p = 0
        size = 500
        while size >= 500:
            i = p * 500
            file_infos = filedao.query_more_files(more_files, i, new)
            size = len(file_infos)
            for file_info in file_infos:
                csi_file_full_name = file_info.full_name

                # /cap/origin_csi_file/TBT{merchant_no}/{merchant_no}Detail.txt
                # /dish/origin_csi_file/CFH-{count_cfh}/CIH-{cco_air}/CBH-{cco_agent}/detail.txt
                key = file_info.path

                file_names = key.split(os.path.sep)
                csi_file_format_name = file_names[1]

                is_detail = file_names[len(file_names) - 1].lower().endswith("detail.txt")
                if is_detail:
                    lines = [line.strip() for line in open(csi_file_full_name, "rt")]
                    record_format = self.__root.format(csi_file_format_name)
                    self.__more_file(record_format, lines, new)

            p += 1

    def __get_doc_num(self, line, record_format):
        # type: (str, RecordFormat) -> str

        element = self.__get_element(line)
        record_content = record_format.content(element)
        record = CompareRecord(line, record_content)
        record.parse_record()
        element_map = record.element_map
        for key in element_map:
            value = element_map.get(key)
            if record.doc_num(key):
                return value

        return ""

    def __get_element(self, line):
        # type: (str) -> str
        return line[0:3]

    def __refresh_file_list(self, split_root_dir, new):
        # type: (str,bool)->dict
        entities = list()
        split_files = dict()
        for root, dirs, files in os.walk(split_root_dir):
            for name in files:
                full_name = os.path.join(root, name)

                # /(cap|dish)/origin_csi_file/**/*.txt
                path = full_name[len(split_root_dir):]

                file_names = path.split(os.path.sep)
                file_name = path[2]

                if new:
                    entities.append(SplitFileInfoNew(path, split_root_dir, full_name, file_name))
                else:
                    entities.append(SplitFileInfoOld(path, split_root_dir, full_name, file_name))
                split_files[file_name] = path

        if len(entities) > 0:
            filedao.add(entities, new)
        return split_files
