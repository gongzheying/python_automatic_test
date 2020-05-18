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

    with open(os.path.join(result_path, "csi_diff.csv"), "w+t") as diff_file:
        diff_writer = DictWriter(diff_file, COLUMNS)

        diff_writer.writeheader()

        CompareCSIDiff(old_split_dir, new_split_dir, result_path).process_compare(diff_writer)


class CompareCSIDiff:

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
        # type : (SplitFileInfoVO, DictWriter) -> None

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

        self.__getFields(self.__oldFileName, self.__csiFormat)

        # read file
        old_list = [line.strip() for line in open(old_file) if len(line.strip()) > 0]
        new_list = [line.strip() for line in open(new_file) if len(line.strip()) > 0]

        self.__compareFileList(self.__root.format(self.__csiFormat), old_list, new_list, True, diff_writer)

    def __compareFileList(self, recordFormat, oldList, newList, checkGroup, diff_writer):
        # type: (RecordFormat,list,list,bool,DictWriter) -> None
        index = 0
        newindex = 0
        while index < len(oldList) and newindex < len(newList):
            line = oldList[index]
            newLine = newList[newindex]

            if len(line) > 3:
                element = self.__get_element(line)
                newelement = self.__get_element(newLine)

                recordContent = recordFormat.content(element)
                record = CompareRecord(line, recordContent)

                newrecordContent = recordFormat.content(newelement)
                newRecord = CompareRecord(newLine, newrecordContent)

                isbreak = False
                while element != newelement:

                    # Find a correspondence in the old record, indicating that the current old
                    # record is out, record and skip the old record
                    if self.__findSameRecord(newelement, oldList):
                        index += 1
                        comment = recordContent.comment
                        commentdesc = recordContent.comment_desc

                        diff_writer.writerow({
                            "ISIS File Name": self.__oldFileName,
                            "CSI Date": self.__csiDate,
                            "PCA": self.__pca,
                            "Airline": "",
                            "Format": self.__csiFormat,
                            "Error type": comment,
                            "Error description": commentdesc,
                            "CSI Record Name": line[0:11],
                            "Document number": "",
                            "ISIS VALUE": "",
                            "ISIS2 VALUE": "",
                            "Filed": ""
                        })
                        isbreak = True
                        break
                    else:  # Otherwise the new record is out of the record and skips the new record

                        comment = newrecordContent.comment
                        commentdesc = newrecordContent.comment_desc

                        diff_writer.writerow({
                            "ISIS File Name": self.__oldFileName,
                            "CSI Date": self.__csiDate,
                            "PCA": self.__pca,
                            "Airline": "",
                            "Format": self.__csiFormat,
                            "Error type": comment,
                            "Error description": commentdesc,
                            "CSI Record Name": newLine[0:11],
                            "Document number": "",
                            "ISIS VALUE": "",
                            "ISIS2 VALUE": "",
                            "Filed": ""
                        })

                        newindex += 1
                        if newindex >= len(newList):
                            isbreak = True
                            break
                        newLine = newList[newindex]
                        newelement = self.__get_element(newLine)
                        newrecordContent = recordFormat.content(newelement)
                        newRecord = CompareRecord(newLine, newrecordContent)

                if isbreak:
                    continue

                if checkGroup:
                    if record.group():
                        groupList = self.__getGroup(recordFormat, index, oldList)
                        newGroupList = self.__getGroup(recordFormat, newindex, newList)
                        more = self.__isSeamGroup(recordFormat, groupList, newGroupList, diff_writer)
                        if "" == more:
                            self.__compareFileList(recordFormat, groupList, newGroupList, False, diff_writer)
                            index += len(groupList)
                            newindex += len(newGroupList)
                        elif "old" == more:
                            index += len(groupList)
                        else:
                            newindex += len(newGroupList)
                        continue

                newrecordContent = recordFormat.content(newelement)
                newRecord = CompareRecord(newLine, newrecordContent)
                self.__compareElement(record, newRecord)

            index += 1
            newindex += 1

        # The same line after comparison to see if there is no record than the record

        self.__processOverplusLine(recordFormat, index, oldList, False)

        self.__processOverplusLine(recordFormat, newindex, newList, True)

        # if end replace docNum
        if not checkGroup:
            self.__replaceLog()

    def __findSameRecord(self, newelement, oldList):
        # type: (str,list) -> bool

        for line in oldList:
            if newelement == self.__get_element(line):
                return True
        return False

    def __getGroup(self, recordFormat, index, lines):
        # type : (RecordFormat,int,list) -> list

        group = list()
        while index < len(lines):
            line = lines[index]
            element = self.__get_element(line)
            recordContent = recordFormat.content(element)
            record = CompareRecord(line, recordContent)

            if record.group_end and len(group) >= 1:
                break

            if record.group():
                group.append(line)

            index += 1

        return group

    def __isSeamGroup(self, recordFormat, oldList, newList, diff_writer):
        # type: (RecordFormat,list,list,DictWriter) -> str

        index = 0
        newindex = 0
        docNum = "0"
        newDocNum = "0"
        element = self.__get_element(oldList[0])
        endElement = self.__get_element(oldList[len(oldList) - 1])

        while index < len(oldList) and newindex < len(newList):
            line = oldList[index]
            newLine = newList[newindex]
            doc = self.__get_doc_num(line, recordFormat)
            newDoc = self.__get_doc_num(newLine, recordFormat)
            if "" != doc:
                docNum = doc

            if "" != newDoc:
                newDocNum = newDoc;

            index += 1
            newindex += 1

        seamGroup = docNum == newDocNum
        if not seamGroup:
            num = long(docNum.strip())
            newNum = long(newDocNum.strip())
            if num > newNum:
                comment = "D_CSI_pymt"
                value = newList[0][0: 11]
                safm = newList[0][157: 168]
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
                    "CSI Record Name": "{}-{}".format(element, endElement),
                    "Document number": ":{}".format(newNum),
                    "ISIS VALUE": "",
                    "ISIS2 VALUE": value,
                    "Filed": ""
                })

                return "new"
            else:
                value = oldList[0][0: 11]

                diff_writer.writerow({
                    "ISIS File Name": self.__oldFileName,
                    "CSI Date": self.__csiDate,
                    "PCA": self.__pca,
                    "Airline": "",
                    "Format": self.__csiFormat,
                    "Error type": "D_CSI_pymt",
                    "Error description": "lack of payment",
                    "CSI Record Name": "{}-{}".format(element, endElement),
                    "Document number": ":{}".format(num),
                    "ISIS VALUE": value,
                    "ISIS2 VALUE": "",
                    "Filed": ""
                })

                return "old"

        return ""

    def __compareElement(self, record, newRecord):
        # type: (CompareRecord,CompareRecord) -> None

        record.parse_record()
        newRecord.parse_record()

        map = record.element_map
        newMap = newRecord.element_map

        docNumStr = "";
        if record.group:
            docNumStr = "docNum"

        for key in map:

            value = ":{}".format(map.get(key))
            newValue = ":{}".format(newMap.get(key))

            if record.csi_date(key):
                self.__csiDate = newValue

            if record.doc_num(key):
                self.__docNum = ":{}".format(value)

            if value != newValue:
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
                    "Document number": docNumStr,
                    "ISIS VALUE": value,
                    "ISIS2 VALUE": newValue,
                    "Filed": key
                }

                self.__logs.append(error)

    def __replaceLog(self):
        for log in self.__logs:
            if "docNum" == log.get("Document number"):
                log["Document number"] = self.__docNum
        self.__docNum = ""

    def __processOverplusLine(self, recordFromat, index, lineList, new):
        # type: (RecordFormat, int,list,bool) -> None
        if index != len(lineList):
            line = lineList[index]
            element = self.__get_element(line)
            recordContent = recordFromat.content(element)

            # Same ticket or not a row of tickets, new tickets are processed by more votes and fewer votes
            if recordContent.group_end:

                doc_num = ""
                group_list = self.__get_group(recordFromat, lineList[index:])
                for line in group_list:
                    doc = self.__get_doc_num(line, recordFromat)
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

            else:
                for i in range(index, len(lineList)):
                    line = lineList[i]
                    element = self.__get_element(line)
                    recordContent = recordFromat.content(element)

                    error = {
                        "ISIS File Name": self.__oldFileName,
                        "CSI Date": self.__csiDate,
                        "PCA": self.__pca,
                        "Airline": "",
                        "Format": self.__csiFormat,
                        "Error type": recordContent.comment,
                        "Error description": recordContent.comment_desc,
                        "CSI Record Name": line,
                        "Document number": "",
                        "ISIS VALUE": "N" if new else "Y",
                        "ISIS2 VALUE": "Y" if new else "N",
                        "Filed": ""
                    }

                    self.__logs.append(error)

    def __process_log(self, filename, diff_writer):
        # type : (str,DictWriter) -> None

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.

        if "" != self.__curFileName:
            curpath = os.path.split(self.__curFileName)[0]
            path = os.path.split(filename)[0]
            if curpath != path:
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

    def __getFields(self, oldname, csiFormat):
        # type: (str,str) -> None
        if "cap" == csiFormat:
            self.__pca = "AMEX";
        else:
            if oldname.startswith("c"):  # Complete CSI
                self.__pca = ""
                self.__airlineCode = oldname[len(oldname) - 3:]
            else:
                if len(oldname) == 7 or len(oldname) == 8 or len(oldname) == 13:
                    self.__pca = oldname[0: 2]

                elif len(oldname) == 12:
                    end = oldname[10:]
                    if re.search(".*([1-2]$)", end) is not None:
                        self.__pca = oldname[4:8]
                    else:
                        self.___pca = oldname[0:2]

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
        p = 0
        size = 500
        while size >= 500:
            i = p * 500
            file_infos = filedao.query_more_files(more_files, i, new)
            size = len(file_infos)
            for file_info in file_infos:
                csi_file_full_name = file_info.full_name
                csi_file_name = file_info.original_file

                # /cap/origin_csi_file/TBT{merchant_no}/{merchant_no}Detail.txt
                # /dish/origin_csi_file/CFH-{count_cfh}/CIH-{cco_air}/CBH-{cco_agent}/detail.txt
                key = file_info.path

                file_names = key.split(os.path.sep)
                csi_file_format_name = file_names[1]

                is_detail = file_names[len(file_names) - 1].lower().endswith("detail.txt")
                if is_detail:
                    lines = [line.strip() for line in open(csi_file_full_name, "rt")]

                    record_format = self.__root.format(csi_file_format_name)
                    doc_num = ""
                    group_list = self.__get_group(record_format, lines)
                    for line in group_list:
                        doc = self.__get_doc_num(line, record_format)
                        if "" != doc:
                            doc_num = ":{}".format(doc)

                    diff_writer.writerow({
                        "ISIS File Name": csi_file_name,
                        "CSI Date": "",
                        "PCA": "",
                        "Airline": "",
                        "Format": record_format.name,
                        "Error type": "D_CSI_pymt",
                        "Error description": "lack of payment",
                        "CSI Record Name": "",
                        "Document number": doc_num,
                        "ISIS VALUE": "N" if new else "Y",
                        "ISIS2 VALUE": "Y" if new else "N",
                        "Filed": ""
                    })

            p += 1

    def __get_doc_num(self, line, record_format):
        # type: (str, RecordFormat) -> str

        element = self.__get_element(line)
        record_content = record_format.content(element)
        record = CompareRecord(line, record_content)
        record.parse_record()
        map = record.element_map
        for key in map:
            value = map.get(key)
            if record.doc_num(key):
                return value

        return ""

    def __get_element(self, line):
        # type : (str) -> str
        return line[0:3]

    def __get_group(self, record_format, lines):
        # type: (RecordFormat, list) -> list
        group = list()
        for line in lines:

            element = self.__get_element(line)
            record_content = record_format.content(element)
            record = CompareRecord(line, record_content)

            if record.group_end and len(group) >= 1:
                break

            if record.group:
                group.append(line)

        return group

    def __refresh_file_list(self, split_root_dir, new):
        # type: (str,bool)->dict
        entities = list()
        files = dict()
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
                files[file_name] = path

        if len(entities) > 0:
            filedao.add(entities, new)
        return files
