import os
from csv import DictWriter

from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew, SplitFileInfoVO
from file_compare.filestructure import CompareRoot, CompareRecord, RecordContent, RecordFormat

CONF_PATH = "config.txt"
OLD_DEFAULT_PATH = "./oldPath"
NEW_DEFAULT_PATH = "./newPath"
RESULT_DEFAULT_PATH = "./result"

COLUMNS = ("ISIS file name",
           "ISIS2 file Name",
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
        self.__oldFileName = ""
        self.__newFileName = ""
        self.__pca = ""
        self.__airlineCode = ""
        self.__csiDate = ""
        self.__docNum = ""

        self.__logs = list()
        self.__root = CompareRoot.get_hot_root()

    def process_compare(self, diff_writer):
        # type: (DictWriter) -> None

        filedao.clear_data()

        # Get all files
        self.__refresh_file_list(self.__old_split_dir, False)
        self.__refresh_file_list(self.__new_split_dir, True)

        # Put the comparison in the same table
        filedao.into_same_record()

        # Processing missing files
        self.__compare_more_file(diff_writer, False)
        self.__compare_more_file(diff_writer, True)

        page = 0
        index = 0
        count = filedao.count_same_record()
        while index <= count:
            index = page * 500
            same_list = filedao.query_same_record(index)
            for vo in same_list:
                self.__compare_file(vo)

            page += 1




    def  __compare_file(self,vo):
        # type : (SplitFileInfoVO) -> None

        filename = vo.full_name
        newFile = vo.new_full_name

        # If the current file and the previous file directory are different, replace
        # the airline code, output logs, empty the log list and airline code.

        if "" != self.__curFileName:
            curpath = os.path.split(self.__curFileName)[0]
            path = os.path.split(filename)[0]
            if curpath != path:
                for error in self.__logs:
                    error["Airline"] = self.__airlineCode
                    error["CSI Date"] = self.__csiDate
                    error["PCA"] = self.__pca

                self.__airlineCode = ""
                self.__docNum = ""
                self.__logs = list()

        self.__curFileName = filename


        file = filename[len(self.__old_split_dir) + 1:]

		csiFormat = file[0, file.find(os.path.sep)]



		if (!curOldName.equals(vo.getNewFileName())) {
			@SuppressWarnings("static-access")
			Appender appender = Logger.getRootLogger().getAppender("RootConsoleAppender");
			if (appender instanceof FileAppender) {

				FileAppender fappender = (FileAppender) appender;

				String logname = vo.getNewFileName();

				fappender.setFile(resultPath.substring(0, resultPath.lastIndexOf(File.separator) + 1) + csiFormat
						+ File.separator + logname + ".csv");

				fappender.activateOptions();
				LogCategory.BUSINESS.error(
						"ISIS file name,ISIS2 file Name,CSI Date,PCA,Airline,Format,Error type,Error description,CSI Record Name,Document number,ISIS VALUE,ISIS2 VALUE,Filed");

			}
		}

		curOldName = vo.getNewFileName();
		self.__oldFileName = vo.getFilename();
		self.__newFileName = vo.getNewFileName();

		getPca(newFileName);

		// read file

		List<String> oldList = FileUtils.readFile(filename);
		List<String> newList = FileUtils.readFile(newFile);

		compareFileList(recordFromat, oldList, newList, true);

	}


    def __compare_more_file(self, diff_writer, new):
        # type: (DictWriter,bool)->None

        i = 0
        p = 0
        size = 500
        while size >= 500:
            i = p * 500
            file_infos = filedao.query_more_files('temp', i, new)
            size = len(file_infos)
            for file_info in file_infos:
                csi_file_full_name = file_info.full_name

                # /cap/origin_csi_file/TBT{merchant_no}/{merchant_no}Detail.txt
                # /dish/origin_csi_file/CFH-{count_cfh}/CIH-{cco_air}/CBH-{cco_agent}/detail.txt
                key = file_info.path

                path_name_array = key.split(os.path.sep)
                csi_file_format_name = path_name_array[1]

                is_detail = path_name_array[len(path_name_array) - 1].lower().endswith("detail.txt")
                if is_detail:
                    line_list = [line.strip() for line in open(csi_file_full_name, "rt")]
                    self.__more_file(self.__root.format(csi_file_format_name), line_list, diff_writer, new)

            p += 1

    def __more_file(self, record_format, lines, diff_writer, new):
        # type: (RecordFormat, list,DictWriter,bool)->None

        doc_num = ""
        group_list = self.__get_group(record_format, lines)
        for line in group_list:
            doc = self.__get_doc_num(line, record_format)
            if "" != doc:
                doc_num = ":{}".format(doc)

        error = dict()
        # "ISIS file name,ISIS2 file Name,CSI Date,PCA,Airline,Format,Error type,Error description,CSI Record Name,Document number,ISIS VALUE,ISIS2 VALUE,Filed"
        error["ISIS file name"] = self.__oldFileName
        error["ISIS2 file Name"] = self.__newFileName
        error["CSI Date"] = "csiDate"
        error["PCA"] = self.__pca
        error["Airline"] = ""
        error["Format"] = record_format.name
        error["Error type"] = "D_CSI_pymt"
        error["Error description"] = "lack of payment"
        error["CSI Record Name"] = ""
        error["Document number"] = doc_num
        error["ISIS VALUE"] = "N" if new else "Y"
        error["ISIS2 VALUE"] = "Y" if new else "N"
        error["Filed"] = ""

        self.__logs.append(error)

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
        # type: (str,bool)->None
        entities = list()
        file_name = ""
        for root, dirs, files in os.walk(split_root_dir):
            for name in files:
                full_name = os.path.join(root, name)

                # split_root_dir/(cap|dish)/origin_csi_file/filename.txt

                if "filename.txt" == name:
                    lines = [line.strip() for line in open(full_name, "rt")]
                    file_name = lines[0]
                    continue

                path = full_name[len(split_root_dir):]
                file_name = path[1:path.index(os.path.sep, 2)]

                if new:
                    entities.append(SplitFileInfoNew(path, split_root_dir, full_name, file_name))
                else:
                    entities.append(SplitFileInfoOld(path, split_root_dir, full_name, file_name))

        if len(entities) > 0:
            filedao.add(entities, new)
        return files
