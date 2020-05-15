import os
from csv import DictWriter

from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew
from file_compare.filestructure import CompareRoot, CompareRecord, RecordContent, RecordFormat

CONF_PATH = "config.txt"
OLD_DEFAULT_PATH = "./oldPath"
NEW_DEFAULT_PATH = "./newPath"
RESULT_DEFAULT_PATH = "./result"

COLUMNS = (
    "Airline code", "Agent code", "Document number", "Record number",
    "Field Name", "ISIS value", "ISIS2 value", "Comments", "ISIS File Name")


def run(conf_path=CONF_PATH):
    old_split_dir = OLD_DEFAULT_PATH
    new_split_dir = NEW_DEFAULT_PATH
    result_path = RESULT_DEFAULT_PATH

    with open(conf_path, "rt") as conf_file:
        for line in conf_file:
            if line.startswith("oldPath="):
                old_split_dir = line[line.index("=") + 1:].strip()
            if line.startswith("newPath="):
                new_split_dir = line[line.index("=") + 1:].strip()
            if line.startsWith("resultPath="):
                result_path = line[line.index('=') + 1:].strip()

    with open(os.path.join(result_path, "hot_diff.csv"), "w+t") as diff_file:
        diff_writer = DictWriter(diff_file, COLUMNS)

        diff_writer.writeheader()

        CompareDiff(old_split_dir, new_split_dir, result_path).process_compare(diff_writer)


class CompareDiff:

    def __init__(self, old_split_dir, new_split_dir, result_path):
        # type: (str,str,str) -> CompareDiff
        self.__old_split_dir = old_split_dir
        self.__new_split_dir = new_split_dir
        self.__result_path = result_path
        self.__cur_file_name = ""
        self.__old_file_name = ""
        self.__agent_code = ""
        self.__doc_num = ""
        self.__airline_code = ""
        self.__logs = list()
        self.__root = CompareRoot.get_hot_root()
        self.__record_format = self.__root.format("default")

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

        # Contract
        page = 0
        index = 0
        count = filedao.count_same_record()
        while index <= count:
            index = page * 500
            same_list = filedao.query_same_record(index)
            for vo in same_list:
                self.__compare_file(vo.full_name, vo.new_full_name, diff_writer)

    def __refresh_file_list(self, split_root_dir, new):
        # type: (str,bool)->dict
        entities = list()
        files = dict()
        for root, dirs, files in os.walk(split_root_dir):
            for name in files:
                # split_root_dir/origin_hot_file/agent_code/ticket_number/***
                full_name = os.path.join(root, name)

                path = full_name[len(split_root_dir):]
                file_name = path[1:path.index(os.path.sep, 2)]

                if new:
                    entities.append(SplitFileInfoNew(path, split_root_dir, full_name, file_name))
                else:
                    entities.append(SplitFileInfoOld(path, split_root_dir, full_name, file_name))
                files[file_name] = path

        if len(entities) > 0:
            filedao.add(entities, new)
        return files

    def __compare_more_file(self, file_map, ref_file_map, diff_writer, new):
        # type: (dict,dict,DictWriter,bool)->None

        #  Less entire source files
        file_names = ['temp']
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
        i = 0
        p = 0
        size = 500
        while size >= 500:
            i = p * 500
            file_infos = filedao.query_more_files(more_files, i, new)
            size = len(file_infos)
            for file_info in file_infos:
                hot_file_full_name = file_info.full_name
                agent_code = ""

                # /origin_hot_file/agent_code/ticket_number/***
                key = file_info.path

                path_name_array = key.split(os.path.sep)
                hot_file_name = path_name_array[1]
                if len(path_name_array) > 3:
                    agent_code = path_name_array[2]
                doc_num = path_name_array[len(path_name_array) - 1].replace(".txt", "")
                comment = self.__get_comment(hot_file_full_name)

                if new:
                    diff_writer.writerow({
                        "Agent code": agent_code,
                        "Document number": doc_num,
                        "ISIS value": "N",
                        "ISIS2 value": "Y",
                        "Comments": comment,
                        "ISIS File Name": hot_file_name
                    })
                else:
                    diff_writer.writerow({
                        "Agent code": agent_code,
                        "Document number": doc_num,
                        "ISIS value": "Y",
                        "ISIS2 value": "N",
                        "Comments": comment,
                        "ISIS File Name": hot_file_name
                    })

            p += 1

    def __get_comment(self, file_name):
        # type: (str) -> str
        result = ""

        name_position = self.__root.content_name_position
        name_length = self.__root.length
        with open(file_name, "rt") as file:
            for line in file:
                line = line.strip()
                if len(line) < 3:
                    continue
                comment = self.__get_cstring(self.__record_format, name_position, name_length)
                if "" != comment:
                    result = comment
                    break
        return result

    def __get_cstring(self, record_format, line, name_position, name_length):
        # type: (RecordFormat, str,int,int) -> str

        element = self.__get_element(line, name_position, name_length)
        if name_position > 0:
            element += line[name_position - 1: name_position + name_length - 1]

        if "BKS24" == element or "BOT93" == element or "BAR64" == element or "BAR65" == element:
            record_content = record_format.content(element)
            record = CompareRecord(line, record_content)
            if "CANX" == record.element(record_content.element_name):
                return record_content.comment
        return ""

    def __get_element(self, line, name_position, name_length):
        # type : (str,int,int) -> str
        element = line[0:3]
        if name_position > 0:
            element += line[name_position - 1: name_position + name_length - 1]
        return element

    def __compare_file(self, old_file, new_file, diff_writer):
        # type: (str,str,DictWriter) -> None

        self.__process_log(old_file, diff_writer)

        self.__cur_file_name = old_file

        # agentCode,document number
        file_names = old_file.split(os.path.sep)

        self.__old_file_name = file_names[1]

        self.__agent_code = ""
        if len(file_names.length) > 3:
            self.__agent_code = file_names[2]

        self.__doc_num = file_names[len(file_names) - 1].replace(".txt", "")

        # read file
        old_list = [line.strip() for line in open(old_file) if len(line.strip()) > 0]
        new_list = [line.strip() for line in open(new_file) if len(line.strip()) > 0]

        self.__compare_file_list(old_list, new_list)

    def __process_log(self, filename, diff_writer):
        # type : (str,DictWriter) -> None

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.
        if "" != self.__cur_file_name:
            curpath = os.path.split(self.__cur_file_name)[0]
            path = os.path.split(filename)[0]
            if curpath != path:
                for log in self.__logs:
                    log["Airline code"] = self.__airline_code

                    diff_writer.writerow(log)

                self.__airline_code = ""
                self.__logs = list()

    def __is_canx(self, record_content, record, new_record_content, new_record):
        # type: (RecordContent, CompareRecord, RecordContent, CompareRecord) -> bool
        is_canx_old = "CANX" == record.element(record_content.element_name)
        is_canx_new = "CANX" == new_record.element(new_record_content.element_name)
        return not is_canx_old and is_canx_new

    def __is_same_trnc(self, old_record, new_record):
        # type:  (CompareRecord,CompareRecord) -> bool

        trnc_old = old_record.element("TRNC")
        trnc_new = new_record.element("TRNC")
        if trnc_old is None and trnc_new is None:
            return True

        if trnc_old is not None and trnc_new is not None and trnc_old == trnc_new:
            return True

        return False

    def __file_same_record(self, record_format, new_element, new_record, old_list, name_position, name_length):
        # type : (RecordFormat, str,CompareRecord,list, int, int) -> bool

        for line in old_list:
            element = self.__get_element(line, name_position, name_length)
            record_content = record_format.content(element)
            record = CompareRecord(line, record_content)
            if new_element == self.__get_element(line, name_position, name_length):
                if self.__is_same_trnc(record, new_record):
                    return True

        return False

    def __compare_file_list(self, old_list, new_list):
        # type : (list, list) -> Noe

        name_position = self.__root.content_name_position
        name_length = self.__root.length
        index = 0
        new_index = 0
        line_length = 3 + name_position + name_length

        for line in old_list:
            if new_index >= len(new_list):
                break

            if len(line) > line_length:
                element = self.__get_element(line, name_position, name_length)
                new_line = new_list[new_index]

                new_element = self.__get_element(new_line, name_position, name_length)
                record_content = self.__record_format.content(element)
                record = CompareRecord(line, record_content)

                new_record_content = self.__record_format.content(new_element)
                new_record = CompareRecord(new_line, new_record_content)

                is_break = False
                while (not (element == new_element and self.__is_same_trnc(record, new_record))
                       or self.__is_canx(record_content, record, new_record_content, new_record)):

                    if (self.__file_same_record(self.__record_format, new_element, new_record, old_list,
                                                name_position, name_length)
                            and not self.__is_canx(record_content, record, new_record_content, new_record)):
                        index += 1
                        error = dict()

                        error["Agent code"] = self.__agent_code
                        error["Document number"] = self.__doc_num
                        error["Record number"] = element
                        error["Field Name"] = ""
                        error["ISIS value"] = line
                        error["ISIS2 value"] = "N"
                        error["Comments"] = ""
                        error["ISIS File Name"] = self.__old_file_name
                        if "BAR65" == element or "BAR64" == element:
                            error["Comments"] = "D_HOT_3"

                        self.__logs.append(error)
                        is_break = True
                        break

                    else:
                        is_canx = False
                        if new_record_content.check_canx:
                            is_canx = "CANX" == record.element(new_record_content.element_name)

                        comment = ""
                        if is_canx or "CANX" == new_record.element(new_record_content.element_name):
                            comment = new_record_content.comment
                            if comment is None:
                                comment = ""

                        error = dict()
                        error["Agent code"] = self.__agent_code
                        error["Document number"] = self.__doc_num
                        error["Record number"] = new_element
                        error["Field Name"] = ""
                        error["ISIS value"] = "N"
                        error["ISIS2 value"] = new_line
                        error["Comments"] = comment
                        error["ISIS File Name"] = self.__old_file_name

                        self.__logs.append(error)
                        new_index += 1
                        if new_index >= len(new_list):
                            is_break = True
                            break

                        new_line = new_list[new_index]
                        new_element = self.__get_element(new_line, name_position, name_length)
                        new_record_content = self.__record_format.content(new_element)
                        new_record = CompareRecord(new_line, new_record_content)

                if is_break:
                    continue

                new_record_content = self.__record_format.content(new_element)
                new_record = CompareRecord(new_line, new_record_content)
                record.parse_record()
                new_record.parse_record()
                self.__compare_element(index, record, new_record)

            index += 1
            new_index += 1

        if index != len(old_list):
            for i in range(index, len(old_list)):
                comment = self.__get_cstring(self.__record_format, old_list[i], name_position, name_length)

                error = dict()
                error["Agent code"] = self.__agent_code
                error["Document number"] = self.__doc_num
                error["Record number"] = ""
                error["Field Name"] = ""
                error["ISIS value"] = "Y"
                error["ISIS2 value"] = "N"
                error["Comments"] = comment
                error["ISIS File Name"] = self.__old_file_name
                self.__logs.append(error)

        if new_index != len(new_list):
            for i in range(new_index, len(new_list)):
                comment = self.__get_cstring(self.__record_format, new_list[i], name_position, name_length)

                error = dict()
                error["Agent code"] = self.__agent_code
                error["Document number"] = self.__doc_num
                error["Record number"] = ""
                error["Field Name"] = ""
                error["ISIS value"] = "N"
                error["ISIS2 value"] = "Y"
                error["Comments"] = comment
                error["ISIS File Name"] = self.__old_file_name
                self.__logs.append(error)

    def __compare_element(self, index, record, new_record):
        # type : (int, CompareRecord,CompareRecord) -> None

        map = record.element_map
        new_map = new_record.element_map

        for key in map:
            value = map.get(key)
            new_value = new_map.get(key)
            if record.airline(key):
                self.__airline_code = "air:{}".format(value)

            if value is not None and new_value is not None:
                if value != new_value:
                    comment = record.comment(key)
                    element_old_value = record.old_value(key)
                    element_new_value = record.new_value(key)

                    if element_new_value is not None and new_value != element_new_value:
                        comment = ""

                    if element_old_value is not None and value != element_old_value:
                        comment = ""

                    if record.check_length(key) and value.strip() != "":
                        comment = ""

                    if "CARF" == key and new_value.find(value.strip()) > -1:
                        comment = "D_HOT_69"

                    error = dict()
                    error["Agent code"] = self.__agent_code
                    error["Document number"] = self.__doc_num
                    error["Record number"] = record.getContent().getName()
                    error["Field Name"] = key
                    error["ISIS value"] = value
                    error["ISIS2 value"] = new_value
                    error["Comments"] = comment
                    error["ISIS File Name"] = self.__old_file_name
                    self.__logs.append(error)
