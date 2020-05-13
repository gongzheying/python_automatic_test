import os
from csv import DictWriter

from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew
from file_compare.filestructure import CompareRoot, CompareRecord, RecordContent

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

    with open(os.path.join(result_path, "diff.csv"), "w+t") as diff_file:
        diff_writer = DictWriter(diff_file, COLUMNS)

        diff_writer.writeheader()

        CompareDiff(old_split_dir, new_split_dir, result_path).process_compare(diff_writer)


class CompareDiff:

    def __init__(self, old_split_dir, new_split_dir, result_path):
        # type: (str,str,str) -> CompareDiff
        self.__old_split_dir = old_split_dir
        self.__new_split_dir = new_split_dir
        self.__result_path = result_path
        self.__curFileName = ""
        self.__oldFileName = ""
        self.__agentCode = ""
        self.__docNum = ""
        self.__airlineCode = ""
        self.__loglist = list()

    @property
    def old_split_dir(self):
        return self.__old_split_dir

    @property
    def new_split_dir(self):
        return self.__new_split_dir

    @property
    def result_path(self):
        return self.__result_path

    def process_compare(self, diff_writer):
        # type: (DictWriter) -> None

        filedao.clear_data()

        # Get all files
        old_map = self.__refresh_file_list(self.old_split_dir, False)
        new_map = self.__refresh_file_list(self.new_split_dir, True)

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
        entity_type = SplitFileInfoNew if new else SplitFileInfoOld
        for root, dirs, files in os.walk(split_root_dir):
            for name in files:
                path = os.path.join(root, name)

                key = path[len(split_root_dir):]
                file_name = key[1:key.index(os.path.sep, 2)]

                entities.append(entity_type(key, split_root_dir, path, file_name))
                files[file_name] = key

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
        root = CompareRoot.get_hot_root()
        record_format = root.format("default")
        name_position = root.content_name_position
        name_length = root.length
        with open(file_name, "rt") as file:
            for line in file:
                line = line.strip()
                if len(line) < 3:
                    continue
                comment = self.__get_cstring(record_format, name_position, name_length)
                if "" != comment:
                    result = comment
                    break
        return result

    def __get_cstring(self, record_format, line, name_position, name_length):
        # type : (RecordFormat, str,int,int) -> str
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

        self.__curFileName = old_file

        # agentCode,document number
        fileNames = old_file.split(os.path.sep)

        self.__oldFileName = fileNames[1]

        self.__agentCode = ""
        if len(fileNames.length) > 3:
            self.__agentCode = fileNames[2]

        self.__docNum = fileNames[len(fileNames) - 1].replace(".txt", "")

        # read file
        old_list = [line.strip() for line in open(old_file) if len(line.strip()) > 0]
        new_list = [line.strip() for line in open(new_file) if len(line.strip()) > 0]

        root = CompareRoot.get_hot_root()

        self.__compare_file_list(root.format("default"), root.content_name_position, root.length, old_list, new_list)

    def __process_log(self, filename, diff_writer):
        # type : (str,DictWriter) -> None

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.
        if "" != self.__curFileName:
            curpath = os.path.split(self.__curFileName)[0]
            path = os.path.split(filename)[0]
            if curpath != path:
                for log in self.__loglist:
                    log["Airline code"] = self.__airlineCode

                    diff_writer.writerow(log)

                self.__airlineCode = ""
                self.__loglist = list()

    def __isCanx(self, recordContent, record, newrcc, newRecord):
        # type: (RecordContent, CompareRecord, RecordContent, CompareRecord) -> bool
        iscanxold = "CANX" == record.element(recordContent.element_name)
        iscanxnew = "CANX" == newRecord.element(newrcc.element_name)
        return not iscanxold and iscanxnew

    def __isSameTrnc(self, oldRecord, newRecord):
        # type:  (CompareRecord,CompareRecord) -> bool

        trncOld = oldRecord.element("TRNC")
        trncNew = newRecord.element("TRNC")
        if trncOld is None and trncNew is None:
            return True

        if trncOld is not None and trncNew is not None and trncOld == trncNew:
            return True

        return False

    def __findSameRecord(self, recordFormat, newelement, newRecord, oldList, namePosition, nameLength):
        # type : (RecordFormat, str,CompareRecord,list, int, int) -> bool
        for line in oldList:
            element = self.__get_element(line, namePosition, nameLength)
            recordContent = recordFormat.content(element)
            record = CompareRecord(line, recordContent)
            if newelement == self.__get_element(line, namePosition, nameLength):
                if self.__isSameTrnc(record, newRecord):
                    return True

        return False

    def __compare_file_list(self, record_format, line, name_position, name_length, old_list, new_list):
        # type : (RecordFormat, int, int, list, list) -> Noe
        index = 0
        newindex = 0
        lineLength = 3 + name_position + name_length

        iscanx = False
        for line in old_list:
            if newindex >= len(new_list):
                break

            if len(line) > lineLength:
                element = self.__get_element(line, name_position, name_length)
                newLine = new_list[newindex]

                newelement = self.__get_element(newLine, name_position, name_length)
                recordContent = record_format.content(element)
                record = CompareRecord(line, recordContent)

                newrecordContent = record_format.content(newelement)
                newRecord = CompareRecord(newLine, newrecordContent)

                isbreak = False
                while (not (element == newelement and self.__isSameTrnc(record, newRecord)) or
                       self.__isCanx(recordContent, record, newrecordContent, newRecord)):

                    # Find a correspondence in the old record, indicating that the current old record is out, record and skip the old record
                    if (self.__findSameRecord(record_format, newelement, newRecord, old_list, name_position,
                                              name_length) and
                            not self.__isCanx(recordContent, record, newrecordContent, newRecord)):
                        index += 1
                        error = dict()

                        error["Agent code"] = self.__agentCode
                        error["Document number"] = self.__docNum
                        error["Record number"] = element
                        error["Field Name"] = ""
                        error["ISIS value"] = line
                        error["ISIS2 value"] = "N"
                        error["Comments"] = ""
                        error["ISIS File Name"] = self.__oldFileName
                        if "BAR65" == element or "BAR64" == element:
                            error["Comments"] = "D_HOT_3"

                        self.__loglist.append(error)
                        isbreak = True
                        break

                    else:

                        if newrecordContent.check_canx:
                            iscanx = "CANX" == record.element(newrecordContent.element_name)

                        comment = ""
                        if iscanx or "CANX" == newRecord.element(newrecordContent.element_name):
                            comment = newrecordContent.comment
                            if comment is None:
                                comment = ""

                        error = dict()
                        error["Agent code"] = self.__agentCode
                        error["Document number"] = self.__docNum
                        error["Record number"] = newelement
                        error["Field Name"] = ""
                        error["ISIS value"] = "N"
                        error["ISIS2 value"] = newLine
                        error["Comments"] = comment
                        error["ISIS File Name"] = self.__oldFileName

                        self.__loglist.append(error)
                        newindex += 1
                        if newindex >= len(new_list):
                            isbreak = True
                            break

                        newLine = new_list[newindex]
                        newelement = self.__get_element(newLine, name_position, name_length)
                        newrecordContent = record_format.content(newelement)
                        newRecord = CompareRecord(newLine, newrecordContent)

                if isbreak:
                    continue

                newrecordContent = record_format.content(newelement)
                newRecord = CompareRecord(newLine, newrecordContent)
                record.parse_record()
                newRecord.parse_record()
                self.__compareElement(index, record, newRecord)

            index += 1
            newindex += 1

        if index != len(old_list):
            for i in range(index, len(old_list)):
                comment = self.__get_cstring(record_format, old_list[i], name_position, name_length)

                error = dict()
                error["Agent code"] = self.__agentCode
                error["Document number"] = self.__docNum
                error["Record number"] = ""
                error["Field Name"] = ""
                error["ISIS value"] = "Y"
                error["ISIS2 value"] = "N"
                error["Comments"] = comment
                error["ISIS File Name"] = self.__oldFileName
                self.__loglist.append(error)

        if newindex != len(new_list):
            for i in range(newindex, len(new_list)):
                comment = self.__get_cstring(record_format, new_list[i], name_position, name_length)

                error = dict()
                error["Agent code"] = self.__agentCode
                error["Document number"] = self.__docNum
                error["Record number"] = ""
                error["Field Name"] = ""
                error["ISIS value"] = "N"
                error["ISIS2 value"] = "Y"
                error["Comments"] = comment
                error["ISIS File Name"] = self.__oldFileName
                self.__loglist.append(error)

    def __compareElement(self, index, record, newRecord):
        # type : (int, CompareRecord,CompareRecord) -> None

        map = record.element_map
        newMap = newRecord.element_map

        for key in map:
            value = map.get(key)
            newValue = newMap.get(key)
            if record.airline(key):
                self.__airlineCode = "air:{}".format(value)

            if value is not None and newValue is not None:
                if value != newValue:
                    comment = record.comment(key)
                    elementOldValue = record.old_value(key)
                    elementNewValue = record.new_value(key)

                    if elementNewValue is not None and newValue != elementNewValue:
                        comment = ""

                    if elementOldValue is not None and value != elementOldValue:
                        comment = ""

                    if record.check_length(key) and value.strip() != "":
                        comment = ""

                    if "CARF" == key and newValue.find(value.strip()) > -1:
                        comment = "D_HOT_69"

                    error = dict()
                    error["Agent code"] = self.__agentCode
                    error["Document number"] = self.__docNum
                    error["Record number"] = record.getContent().getName()
                    error["Field Name"] = key
                    error["ISIS value"] = value
                    error["ISIS2 value"] = newValue
                    error["Comments"] = comment
                    error["ISIS File Name"] = self.__oldFileName
                    self.__loglist.append(error)
