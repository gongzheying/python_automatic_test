import os
import csv
from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew, SplitFileInfoVO
from file_compare.filestructure import CompareRoot, RecordFormat, RecordContent, CompareElement, CompareRecord


class CompareDiff:
    CONF_PATH = "config.txt"
    OLD_DEFAULT_PATH = "./oldPath"
    NEW_DEFAULT_PATH = "./newPath"
    RESULT_DEFAULT_PATH = "./result"

    COLUMNS = (
        "Airline code",
        "Agent code",
        "Document number",
        "Record number",
        "Field Name",
        "ISIS value",
        "ISIS2 value",
        "Comments",
        "ISIS File Name")

    def __init__(self):
        self.__old_split_dir = self.OLD_DEFAULT_PATH
        self.__new_split_dir = self.NEW_DEFAULT_PATH
        self.__result_path = self.RESULT_DEFAULT_PATH

    @property
    def old_split_dir(self):
        return self.__old_split_dir

    @property
    def new_split_dir(self):
        return self.__new_split_dir

    @property
    def result_path(self):
        return self.__result_path

    def run(self, conf_path=CONF_PATH):

        with open(conf_path, "rt") as conf_file:
            for line in conf_file:
                if line.startswith("oldPath="):
                    self.__old_split_dir = line[line.index("=") + 1:].strip()
                if line.startswith("newPath="):
                    self.__new_split_dir = line[line.index("=") + 1:].strip()
                if line.startsWith("resultPath="):
                    self.__result_path = line[line.index('=') + 1:].strip()

        with open(os.path.join(self.__result_path, "diff.csv"), "w+t") as diff_file:
            diff_writer = csv.DictWriter(diff_file, self.COLUMNS)

            diff_writer.writeheader()
            self.__process_compare(diff_writer)

    def __process_compare(self, diff_writer):
        # type: (csv.DictWriter) -> None

        filedao.clear_data()

        # Get all files
        new_map = self.__refresh_file_list(self.new_split_dir, True)
        old_map = self.__refresh_file_list(self.old_split_dir, False)

        # Put the comparison in the same table
        filedao.into_same_record()

        # Processing missing files
        self.__compare_more_file(new_map, old_map, diff_writer, True)
        self.__compare_more_file(old_map, new_map, diff_writer, False)

        # Contract
        page = 0
        index = 0
        count = filedao.count_same_record()
        while index <= count:
            index = page * 500
            same_list = filedao.query_same_record(index)
            for vo in same_list:
                compareFile(vo.full_name, vo.new_full_name)

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
        # type: (dict,dict,csv.DictWriter,bool)->None

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

    def __get_document_root(self):
        # type: () -> CompareRoot
        return CompareRoot.get_hot_root()

    def __get_document_format(self):
        # type : () -> RecordFormat
        return self.__get_document_root().format("default")



    def __get_comment(self, file_name):
        # type: (str) -> str
        result = ""
        with open(file_name, "rt") as file:
            for line in file:
                if len(line) < 3:
                    continue
        return result

    def __get_cstring(self, record_format, line, name_position, name_length):
        # type : (RecordFormat, str,int,int) -> str
        element = self.__get_element_name(line, name_position, name_length)
        if "BKS24" == element or "BOT93" == element or "BAR64" == element or "BAR65" == element:
            record_conent = record_format.element(element)
            record = CompareRecord(line, record_conent)
            iscanx = "CANX" == record.element(record_conent.element_name)
            if iscanx:
                return record_conent.comment
        return ""

    def __get_element_name(self, line, name_position, name_length):
        # type : (str,int,int) -> str
        element = line[0:3]
        if name_position > 0:
            element += line[name_position - 1: name_position + name_length - 1]
        return element

    # public static String getCString(RecordFromat recordFromat, String line, int namePosition, int nameLength) {
    # 	String element = getElement(line, namePosition, nameLength);
    # 	if ("BKS24".equals(element) || "BOT93".equals(element) || "BAR64".equals(element) || "BAR65".equals(element)) {
    #
# 		RecordContent recordContent = recordFromat.getElement(element);
# 		CompareRecord record = new CompareRecord(line, recordContent);
# 		boolean iscanx = "CANX".equals(record.getElement(recordContent.getElementname()));
# 		if (iscanx) {
# 			return recordContent.getComment();
# 		}
# 	}
# 	return "";
# }
#
# private static String getElement(String line, int namePosition, int nameLength) {
# 	String element = line.substring(0, 3);
# 	if (namePosition > 0) {
# 		element = element + line.substring((namePosition - 1), (namePosition + nameLength - 1));
# 	}
# 	return element;
# }

# private static String getComment(String fileName) {
# 	String result = "";
# 	CompareRoot root = CompareRoot.getCsiRoot(type);
# 	RecordFromat recordFromat = root.getElement(format);
# 	List<String> lineList = FileUtils.readFile(fileName);
# 	int namePosition = recordFromat.getRoot().getContentnameposition();
# 	int nameLength = recordFromat.getRoot().getLength();
# 	for (String line : lineList) {
# 		if (line.length() < 3) {
# 			continue;
# 		}
# 		String comment = getCString(recordFromat, line, namePosition, nameLength);
# 		if (!StringUtils.isBlank(comment)) {
# 			result = comment;
# 		}
# 	}
#
# 	return result;
# }

# "Airline code,Agent code,Document number,Record number,Field Name,ISIS value,ISIS2 value,Comments,ISIS File Name");
# 	private static void compareMoreNewFile(SplitFileInfoDao dao) {
# 		// Less entire source files
# 		String filenames = "";
# 		for (String key : newFileMap.keySet()) {
# 			String value = oldFileMap.get(key);
# 			if (value == null) {
# 				System.out.println("Missing old file:" + key);
# 				String error = ",,,,,N,Y,," + key + "";
# 				LogCategory.BUSINESS.error(error);
# 				filenames += "'" + key + "',";
# 			}
# 		}
# 		filenames = filenames + "'temp'";
# 		// Less tickets
# 		int i = 0;
# 		int p = 0;
# 		List<SplitFileInfoNew> list = null;
# 		int size = 500;
# 		while (size >= 500) {
# 			i = p * 500;
# 			list = dao.queryMoreNew(filenames, i);
# 			size = list.size();
# 			for (SplitFileInfoNew newInfo : list) {
#
# 				String newValue = newInfo.getFullName();
# 				String agent = "";
# 				String key = newInfo.getPath();
# 				String[] newFileNames = FileUtils.splitFileName(newValue.substring(newPath.length()));
# 				String newFileName = newFileNames[1];
# 				if (newFileNames.length > 3) {
# 					agent = newFileNames[2];
# 				}
# 				String docnum = key.substring(key.lastIndexOf(fileseparator) + 1).replaceAll(".txt", "");
# 				String comment = getComment(newValue);
# 				String error = "," + agent + "," + docnum + ",,,N,Y," + comment + "," + newFileName + ",";
# 				LogCategory.BUSINESS.error(error);
# 			}
# 			p++;
# 		}
#
# 	}
#

# 	public static void processCompare() {
# 		LogCategory.BUSINESS.error(
# 				"Airline code,Agent code,Document number,Record number,Field Name,ISIS value,ISIS2 value,Comments,ISIS File Name");
#
# 		SplitFileInfoDao dao = new SplitFileInfoDao();
#
# 		dao.clearData();
#
# 		// Get all files
# 		refreshFileList(oldPath, SplitFileInfoOld.class, dao, oldPath, oldFileMap);
# 		refreshFileList(newPath, SplitFileInfoNew.class, dao, newPath, newFileMap);
#
# 		// Put the comparison in the same table
# 		dao.intoSameRecord();
#
# 		// Processing missing files
# 		compareMoreNewFile(dao);
# 		compareMoreOldFile(dao);
#
# 		// Contrast
# 		int page = 0;
# 		int index = 0;
# 		long count = dao.countSameRecord();
# 		while (index <= count) {
# 			index = page * 500;
# 			List<SplitFileInfoVO> sameList = dao.querySameRecord(index);
# 			System.out.println("page:" + page + ":" + (count / 500));
# 			page++;
# 			for (SplitFileInfoVO vo : sameList) {
# 				compareFile(vo.getFullName(), vo.getNewFullName());
# 			}
#
# 		}
#
# 	}

#
#
# public class CompareDiff {
#
# 	public final static String fileseparator = File.separator;
#
# 	public static HashMap<String, String> oldFileMap = new LinkedHashMap<String, String>();
# 	public static HashMap<String, String> newFileMap = new LinkedHashMap<String, String>();
#
# 	//
# 	//
# 	private static String oldPath = "./old";
# 	private static String newPath = "./new";
#
# 	public static String airlineCode = "";
# 	public static String agentCode = "";
# 	public static String docNum = "";
# 	public static String recordNum = "";
#
# 	public final static String type = "hot";
# 	public final static String format = "default";
#
# 	public static String oldFileName = "";
# 	public static String curFileName = "";
#
# 	public static List<String> loglist = new ArrayList<String>();
#
# 	private final static String confPath = "config.txt";
#
# 	/**
# 	 *
# 	 *
# 	 * @param index
# 	 * @param record
# 	 * @param newRecord
# 	 */
# 	private static void compareElement(int index, CompareRecord record, CompareRecord newRecord) {
# 		Map<String, String> map = record.getElementMap();
# 		Map<String, String> newMap = newRecord.getElementMap();
# 		for (String key : map.keySet()) {
# 			String value = map.get(key);
# 			String newValue = newMap.get(key);
# 			if (record.isAirline(key)) {
# 				airlineCode = "air:" + value + "";
# 			}
#
# 			if (value != null && newValue != null) {
# 				if (!value.equals(newValue)) {
# 					if (value.contains(",")) {
# 						value = "\"" + value + "\"";
# 					}
#
# 					String error = "airlineCode" + "," + agentCode + "," + docNum + "," + record.getContent().getName()
# 							+ "," + key + "," + value + "," + newValue + ",";
# 					String comment = record.getElementComment(key);
#
# 					//
# 					String elementOldValue = record.getElementOldValue(key);
# 					String elementNewValue = record.getElementNewValue(key);
#
# 					if (elementNewValue != null && !newValue.equals(elementNewValue)) {
# 						comment = "";
# 					}
# 					if (elementOldValue != null && !value.equals(elementOldValue)) {
# 						comment = "";
# 					}
# 					//
# 					// checklength="Y" comment="hot_"
# 					if (record.isCheckLength(key) && !StringUtils.isBlank(value)) {
# 						comment = "";
# 					}
#
# 					if ("CARF".equals(key) && newValue.indexOf(value.trim()) > -1) {
# 						comment = "D_HOT_69";
# 					}
#
# 					error = error + comment + "," + oldFileName;
# 					loglist.add(error);
# 				}
# 			} else {
# 				LogCategory.BUSINESS.error(key + ":have no value");
# 			}
#
# 		}
#
# 	}
#
# 	public static void compareFile(String filename, String newFile) {
#
# 		processLog(filename);
#
# 		curFileName = filename;
#
# 		// agentCode,document number
# 		getFileds(filename);
#
# 		// read file
# 		List<String> oldList = FileUtils.readFile(filename);
# 		List<String> newList = FileUtils.readFile(newFile);
#
# 		CompareRoot root = CompareRoot.getCsiRoot(type);
# 		RecordFromat recordFromat = root.getElement(format);
# 		compareFileList(recordFromat, oldList, newList);
#
# 	}
#
# 	private static void compareFileList(RecordFromat recordFromat, List<String> oldList, List<String> newList) {
# 		int index = 0;
# 		int newindex = 0;
# 		// <content name="BFH01">
# 		// hot
# 		int namePosition = recordFromat.getRoot().getContentnameposition();
# 		int nameLength = recordFromat.getRoot().getLength();
#
# 		int lineLength = 3 + namePosition + nameLength;
#
# 		boolean iscanx = false;
# 		for (String line : oldList) {
# 			if (newindex >= newList.size()) {
# 				break;
# 			}
# 			if (line.length() > lineLength) {
# 				String element = getElement(line, namePosition, nameLength);
# 				String newLine = newList.get(newindex);
#
# 				String newelement = getElement(newLine, namePosition, nameLength);
#
# 				RecordContent recordContent = recordFromat.getElement(element);
# 				if (recordContent == null) {
# 					throw new RuntimeException("xml profile is not configured" + element);
# 				}
# 				CompareRecord record = new CompareRecord(line, recordContent);
# 				RecordContent newrecordContent = recordFromat.getElement(newelement);
# 				CompareRecord newRecord = new CompareRecord(newLine, newrecordContent);
#
# 				boolean isbreak = false;
# 				while (!(element.equals(newelement) && isSameTrnc(record, newRecord))
# 						|| isCanx(recordContent, record, newrecordContent, newRecord)) {
# 					// Find a correspondence in the old record, indicating that the current old
# 					// record is out, record and skip the old record
# 					if (findSameRecord(recordFromat, newelement, newRecord, oldList, namePosition, nameLength)
# 							&& !isCanx(recordContent, record, newrecordContent, newRecord)) {
# 						index++;
# 						String error = null;
# 						if ("BAR65".equals(element) || "BAR64".equals(element)) {
# 							error = "" + "airlineCode" + "," + agentCode + "," + docNum + "," + element + ",," + line
# 									+ ",N,D_HOT_3," + oldFileName;
# 						} else {
# 							error = "" + "airlineCode" + "," + agentCode + "," + docNum + "," + element + ",," + line
# 									+ ",N,," + oldFileName;
# 						}
# 						// LogCategory.BUSINESS.error(error);
# 						loglist.add(error);
# 						isbreak = true;
# 						break;
# 					} else {// Otherwise the new record is out of the record and skips the new record
#
# 						if (newrecordContent.isCheckcanx()) {
# 							iscanx = "CANX".equals(record.getElement(newrecordContent.getElementname()));
# 						}
# 						String comment = "";
# 						if (iscanx || "CANX".equals(newRecord.getElement(newrecordContent.getElementname()))) {
# 							comment = newrecordContent.getComment();
# 							if (comment == null) {
# 								comment = "";
# 							}
# 						}
# 						String error = "" + "airlineCode" + "," + agentCode + "," + docNum + "," + newelement + ",,N,"
# 								+ newLine + "," + comment + "," + oldFileName;
# //                         LogCategory.BUSINESS.error(error);
# 						loglist.add(error);
# 						newindex++;
# 						if (newindex >= newList.size()) {
# 							isbreak = true;
# 							break;
# 						}
# 						newLine = newList.get(newindex);
# 						newelement = getElement(newLine, namePosition, nameLength);
# 						newrecordContent = recordFromat.getElement(newelement);
# 						newRecord = new CompareRecord(newLine, newrecordContent);
# 					}
# 				}
#
# 				if (isbreak) {
# 					continue;
# 				}
#
# 				newrecordContent = recordFromat.getElement(newelement);
# 				newRecord = new CompareRecord(newLine, newrecordContent);
# 				record.parseRecord();
# 				newRecord.parseRecord();
# 				compareElement(index, record, newRecord);
#
# 			} else {
# 				LogCategory.BUSINESS.error("line No." + (index + 1) + " Wrong length");
# 			}
# 			index++;
# 			newindex++;
# 		}
#
# 		if (index != oldList.size()) {
# 			for (int i = index; i < (oldList.size()); i++) {
# 				String comment = getCString(recordFromat, oldList.get(i), namePosition, nameLength);
# 				String error = "" + "airlineCode" + "," + agentCode + "," + docNum + "" + "," + ""
# //                        + ",," + oldList.get(i) + ",N," + comment + "," + oldFileName;
# 						+ ",," + "Y,N," + comment + "," + oldFileName;
#
# 				if (!loglist.contains(error)) {
# 					loglist.add(error);
# 				}
# 			}
#
# 		}
#
# 		if (newindex != newList.size()) {
# 			for (int i = newindex; i < (newList.size()); i++) {
# 				String comment = getCString(recordFromat, newList.get(i), namePosition, nameLength);
# 				String error = "" + "airlineCode" + "," + agentCode + "," + docNum + ""
# //                    + ",,,N," + newList.get(i) + "," + comment + "," + oldFileName;
# 						+ ",,,N,Y" + "," + comment + "," + oldFileName;
#
# 				if (!loglist.contains(error)) {
# 					loglist.add(error);
# 				}
# 			}
#
# 		}
# 	}
#
# 	private static void compareMoreNewFile(SplitFileInfoDao dao) {
# 		// Less entire source files
# 		String filenames = "";
# 		for (String key : newFileMap.keySet()) {
# 			String value = oldFileMap.get(key);
# 			if (value == null) {
# 				System.out.println("Missing old file:" + key);
# 				String error = ",,,,,N,Y,," + key + "";
# 				LogCategory.BUSINESS.error(error);
# 				filenames += "'" + key + "',";
# 			}
# 		}
# 		filenames = filenames + "'temp'";
# 		// Less tickets
# 		int i = 0;
# 		int p = 0;
# 		List<SplitFileInfoNew> list = null;
# 		int size = 500;
# 		while (size >= 500) {
# 			i = p * 500;
# 			list = dao.queryMoreNew(filenames, i);
# 			size = list.size();
# 			for (SplitFileInfoNew newInfo : list) {
#
# 				String newValue = newInfo.getFullName();
# 				String agent = "";
# 				String key = newInfo.getPath();
# 				String[] newFileNames = FileUtils.splitFileName(newValue.substring(newPath.length()));
# 				String newFileName = newFileNames[1];
# 				if (newFileNames.length > 3) {
# 					agent = newFileNames[2];
# 				}
# 				String docnum = key.substring(key.lastIndexOf(fileseparator) + 1).replaceAll(".txt", "");
# 				String comment = getComment(newValue);
# 				String error = "," + agent + "," + docnum + ",,,N,Y," + comment + "," + newFileName + ",";
# 				LogCategory.BUSINESS.error(error);
# 			}
# 			p++;
# 		}
#
# 	}
#
# 	private static void compareMoreOldFile(SplitFileInfoDao dao) {
# 		String filenames = "";
# 		for (String key : oldFileMap.keySet()) {
# 			String newValue = newFileMap.get(key);
# 			if (newValue == null) {
# 				System.out.println("Missing old files:" + key);
# 				String error = ",,,,,Y,N,," + key + "";
# 				LogCategory.BUSINESS.error(error);
# 				filenames += "'" + key + "',";
# 			}
# 		}
# 		filenames = filenames + "'temp'";
# 		int i = 0;
# 		int p = 0;
# 		List<SplitFileInfoOld> oldList = null;
# 		int size = 500;
# 		while (size >= 500) {
# 			i = p * 500;
# 			oldList = dao.queryMoreOld(filenames, i);
# 			size = oldList.size();
# 			for (SplitFileInfoOld oldInfo : oldList) {
# 				String value = oldInfo.getFullName();
# 				String key = oldInfo.getPath();
# 				String agent = "";
# 				String[] fileNames = FileUtils.splitFileName(value.substring(newPath.length()));
# 				String fileName = fileNames[1];
#
# 				if (fileNames.length > 3) {
# 					agent = fileNames[2];
# 				}
# 				String comment = getComment(value);
# 				String docnum = key.substring(key.lastIndexOf(fileseparator) + 1).replaceAll(".txt", "");
# 				String error = "," + agent + "," + docnum + ",,,Y,N," + comment + "," + fileName + ",";
# 				LogCategory.BUSINESS.error(error);
#
# 			}
# 			p++;
# 		}
#
# 	}
#
# 	/**
# 	 * Read the configuration and set the result output file name
# 	 */
# 	private static void config(String path) {
#
# 		String resultPath = "./result";
# 		BufferedReader br;
# 		try {
#
# 			if (path == null) {
# 				br = new BufferedReader(new FileReader(confPath));
# 			} else {
# 				br = new BufferedReader(new FileReader(path));
# 			}
#
# 			String s = "";
# 			while ((s = br.readLine()) != null) {
# 				// delete the "bsp" and "date"
# 				if (s.startsWith("oldPath")) {
# 					oldPath = s.substring(s.lastIndexOf('=') + 1).trim();
# 				}
#
# 				if (s.startsWith("newPath")) {
# 					newPath = s.substring(s.lastIndexOf('=') + 1).trim();
# 				}
#
# 				if (s.startsWith("resultPath")) {
# 					resultPath = s.substring(s.lastIndexOf('=') + 1).trim();
# 				}
# 			}
# 		} catch (Exception e) {
# 			e.printStackTrace();
# 		}
#
# //		DOMConfigurator.configure(CompareDiff.class.getResource("/log4j.xml"));
# 		// Set the output log file name
# 		Appender appender = Logger.getRootLogger().getAppender("RootConsoleAppender");
#
# 		if (appender instanceof FileAppender) {
#
# 			FileAppender fappender = (FileAppender) appender;
#
# 			fappender.setFile(resultPath + ".csv");
#
# 			fappender.activateOptions();
#
# 		}
#
# 	}
#
# 	private static boolean findSameRecord(RecordFromat recordFromat, String newelement, CompareRecord newRecord,
# 			List<String> oldList, int namePosition, int nameLength) {
# 		for (String line : oldList) {
# 			String element = getElement(line, namePosition, nameLength);
# 			RecordContent recordContent = recordFromat.getElement(element);
# 			CompareRecord record = new CompareRecord(line, recordContent);
# 			if (newelement.equals(getElement(line, namePosition, nameLength))) {
# 				if (isSameTrnc(record, newRecord)) {
# 					return true;
# 				}
# 			}
# 		}
# 		return false;
# 	}
#
# 	/**
# 	 * Get the additional file with instructions for the canx ticket
# 	 *
# 	 * @param fileName
# 	 * @return
# 	 */
# 	private static String getComment(String fileName) {
# 		String result = "";
# 		CompareRoot root = CompareRoot.getCsiRoot(type);
# 		RecordFromat recordFromat = root.getElement(format);
# 		List<String> lineList = FileUtils.readFile(fileName);
# 		int namePosition = recordFromat.getRoot().getContentnameposition();
# 		int nameLength = recordFromat.getRoot().getLength();
# 		for (String line : lineList) {
# 			if (line.length() < 3) {
# 				continue;
# 			}
# 			String comment = getCString(recordFromat, line, namePosition, nameLength);
# 			if (!StringUtils.isBlank(comment)) {
# 				result = comment;
# 			}
# 		}
#
# 		return result;
# 	}
#
# 	/**
# 	 * Get instructions on whether a canx ticket is available in the event of
# 	 * multiple travel
# 	 *
# 	 * @param recordFromat
# 	 * @param line
# 	 * @param namePosition
# 	 * @param nameLength
# 	 * @return
# 	 */
# 	public static String getCString(RecordFromat recordFromat, String line, int namePosition, int nameLength) {
# 		String element = getElement(line, namePosition, nameLength);
# 		if ("BKS24".equals(element) || "BOT93".equals(element) || "BAR64".equals(element) || "BAR65".equals(element)) {
#
# 			RecordContent recordContent = recordFromat.getElement(element);
# 			CompareRecord record = new CompareRecord(line, recordContent);
# 			boolean iscanx = "CANX".equals(record.getElement(recordContent.getElementname()));
# 			if (iscanx) {
# 				return recordContent.getComment();
# 			}
# 		}
# 		return "";
# 	}
#
# 	private static String getElement(String line, int namePosition, int nameLength) {
# 		String element = line.substring(0, 3);
# 		if (namePosition > 0) {
# 			element = element + line.substring((namePosition - 1), (namePosition + nameLength - 1));
# 		}
# 		return element;
# 	}
#
# 	/**
# 	 * Get the source file name from the file path，agentCode,document number
# 	 *
# 	 * @param filename
# 	 */
# 	private static void getFileds(String filename) {
# 		String[] fileNames = FileUtils.splitFileName(filename.substring(oldPath.length()));
# 		if (fileNames == null || fileNames.length < 3) {
# 			LogCategory.BUSINESS.error(filename + "The file directory is incorrect!");
# 			return;
# 		}
#
# 		oldFileName = fileNames[1];
#
# 		docNum = filename.substring(filename.lastIndexOf(fileseparator) + 1).replaceAll(".txt", "");
# 		agentCode = "";
# 		if (fileNames.length > 3) {
# 			agentCode = fileNames[2];
# 		}
#
# 	}
#
# 	private static boolean isCanx(RecordContent recordContent, CompareRecord record, RecordContent newrcc,
# 			CompareRecord newRecord) {
# 		boolean iscanxold = "CANX".equals(record.getElement(recordContent.getElementname()));
# 		boolean iscanxnew = "CANX".equals(newRecord.getElement(newrcc.getElementname()));
# 		return !iscanxold && iscanxnew;
# 	}
#
# 	private static boolean isSameTrnc(CompareRecord oldRecord, CompareRecord newRecord) {
# 		String trncOld = oldRecord.getElement("TRNC");
# 		String trncNew = newRecord.getElement("TRNC");
# 		if (trncOld == null && trncNew == null) {
# 			return true;
# 		}
# 		if (trncOld != null && trncNew != null && trncOld.equals(trncNew)) {
# 			return true;
# 		}
# 		return false;
# 	}
#
# 	/**
# 	 * @param args
# 	 */
# 	public static void main(String[] args) {
# 		try {
#
# 			config(args[0]);
#
# 			processCompare();
#
# 			LogCategory.BUSINESS.error("--------Completed!-----------");
#
# 		} catch (Exception e) {
#
# 			System.out.println(e.getMessage());
# 			System.exit(2);
#
# 		}
#
# 	}
#
# 	private static boolean newPath(String curFileName, String filename) {
# 		if (StringUtils.isBlank(curFileName)) {
# 			return false;
# 		}
# 		String curpath = curFileName.substring(0, curFileName.lastIndexOf(fileseparator));
# 		String path = filename.substring(0, filename.lastIndexOf(fileseparator));
# 		return !curpath.equals(path);
# 	}
#
# 	public static void processCompare() {
# 		LogCategory.BUSINESS.error(
# 				"Airline code,Agent code,Document number,Record number,Field Name,ISIS value,ISIS2 value,Comments,ISIS File Name");
#
# 		SplitFileInfoDao dao = new SplitFileInfoDao();
#
# 		dao.clearData();
#
# 		// Get all files
# 		refreshFileList(oldPath, SplitFileInfoOld.class, dao, oldPath, oldFileMap);
# 		refreshFileList(newPath, SplitFileInfoNew.class, dao, newPath, newFileMap);
#
# 		// Put the comparison in the same table
# 		dao.intoSameRecord();
#
# 		// Processing missing files
# 		compareMoreNewFile(dao);
# 		compareMoreOldFile(dao);
#
# 		// Contrast
# 		int page = 0;
# 		int index = 0;
# 		long count = dao.countSameRecord();
# 		while (index <= count) {
# 			index = page * 500;
# 			List<SplitFileInfoVO> sameList = dao.querySameRecord(index);
# 			System.out.println("page:" + page + ":" + (count / 500));
# 			page++;
# 			for (SplitFileInfoVO vo : sameList) {
# 				compareFile(vo.getFullName(), vo.getNewFullName());
# 			}
#
# 		}
#
# 	}
#
# 	/**
# 	 * If the current file and the previous file directory are different, replace
# 	 * the airline code, output logs, empty the log list and airline code.
# 	 *
# 	 * @param filename
# 	 */
# 	private static void processLog(String filename) {
# 		// If the current file and the previous file directory are different, replace
# 		// the airline code, output logs, empty the log list and airline code.
# 		if (newPath(curFileName, filename)) {
# 			for (String log : loglist) {
# 				log = log.replaceAll("airlineCode", airlineCode);
# 				LogCategory.BUSINESS.error(log);
# 			}
# 			airlineCode = "";
# 			loglist = new ArrayList<String>();
# 		}
# 	}
#
# 	public static void refreshFileList(String strPath, Class type, SplitFileInfoDao dao, String rootpath,
# 			HashMap<String, String> fileMap) {
# 		System.out.println("" + strPath);
#
# 		File dir = new File(strPath);
# 		File[] files = dir.listFiles();
# 		if (files == null)
# 			return;
#
# 		for (int i = 0; i < files.length; i++) {
# 			if (files[i].isFile()) {
# 				String path = files[i].getPath();
# 				SplitFileInfo info = null;
# 				try {
# 					info = (SplitFileInfo) type.newInstance();
# 				} catch (InstantiationException e) {
# 					e.printStackTrace();
# 				} catch (IllegalAccessException e) {
# 					e.printStackTrace();
# 				}
#
# 				String key = path.substring(rootpath.length());
# 				String rootPath = path.substring(0, rootpath.length()) + key.substring(0, key.indexOf(fileseparator));
#
# 				info.setPath(key);
# 				info.setRootPath(rootPath);
# 				info.setFullName(path);
# //TODO
# 				String filename = key.substring(1, key.indexOf(fileseparator, 2));
#
# 				info.setFilename(filename);
#
# 				dao.add(info);
#
# 				if (fileMap.get(filename) == null) {
# 					fileMap.put(filename, key);
# 				}
#
# 			}
#
# 		}
# 		for (int i = 0; i < files.length; i++) {
#
# 			if (files[i].isDirectory()) {
# 				refreshFileList(files[i].getAbsolutePath(), type, dao, rootpath, fileMap);
# 			}
# 		}
#
# 	}
#
# }
