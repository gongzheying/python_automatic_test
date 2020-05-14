import os
from csv import DictWriter

from file_compare import filedao
from file_compare.filedao import SplitFileInfoOld, SplitFileInfoNew

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
        self.__refresh_file_list(self.old_split_dir, False)
        self.__refresh_file_list(self.new_split_dir, True)

        # Put the comparison in the same table
        filedao.into_same_record()

        # Processing missing files
        self.__compare_more_file(diff_writer, False)
        self.__compare_more_file(diff_writer, True)

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

                #  cap: split_root_dir/origin_csi_file/TBT{merchant_no}/{merchant_no}Detail.txt
                # dish: split_root_dir/origin_csi_file/CFH-{count_cfh}/CIH-{cco_air}/CBH-{cco_agent}/detail.txt

                is_detail = os.path.split(csi_file_full_name)[1].lower().endswith("detail.txt")
                if is_detail:
                    line_list = [line.strip() for line in open(csi_file_full_name, "rt")]
                    self.__more_file(line_list, diff_writer, new)

            p += 1

    def __more_file(self, lines, diff_writer, new):
        # type: (list,DictWriter,bool)->None
        pass

    #
    # private static void moreFile(List<String> lineList, String ny) {
    # 	int index = 0;
    # 	while (index < lineList.size()) {
    # 		String docNum = "";
    # 		List<String> groupList = getGroup(recordFromat, index, lineList);
    # 		for (String line : groupList) {
    # 			String doc = getDocNum(line, recordFromat);
    # 			if (doc != null) {
    # 				docNum = ":" + doc;
    # 			}
    # 		}
    # 		String error = oldFileName + "," + newFileName + ",csiDate," + pca
    # 				+ ",,csiFormat,D_CSI_pymt,lack of payment," + "" + "," + docNum + "," + ny + ",";
    # 		loglist.add(error);
    # 		index += groupList.size();
    # 	}
    #
    # }

    #
    # private static String getDocNum(String line, RecordFromat recordFromat) {
    # 	String element = getElement(line);
    # 	RecordContent recordContent = recordFromat.getElement(element);
    # 	CompareRecord record = new CompareRecord(line, recordContent);
    # 	record.parseRecord();
    # 	Map<String, String> map = record.getElementMap();
    # 	for (String key : map.keySet()) {
    # 		String value = map.get(key);
    # 		if (record.isDocnum(key)) {
    # 			return value;
    # 		}
    # 	}
    # 	return null;
    # }
    #
    # private static String getElement(String line) {
    # 	String element = line.substring(0, 3);
    # 	return element;
    # }
    #
    # private static List<String> getGroup(RecordFromat recordFromat, int index, List<String> list) {
    # 	List<String> group = new ArrayList<String>();
    # 	while (index < list.size()) {
    # 		String line = list.get(index);
    # 		String element = getElement(line);
    # 		RecordContent recordContent = recordFromat.getElement(element);
    # 		CompareRecord record = new CompareRecord(line, recordContent);
    #
    # 		if (record.isGroupEnd() && group.size() >= 1) {
    # 			break;
    # 		}
    #
    # 		if (record.isGroup()) {
    # 			group.add(line);
    # 		}
    #
    # 		index++;
    # 	}
    #
    # 	return group;
    # }

    def __refresh_file_list(self, split_root_dir, new):
        # type: (str,bool)->None
        entities = list()
        file_name = ""
        for root, dirs, files in os.walk(split_root_dir):
            for name in files:
                full_name = os.path.join(root, name)

                # split_root_dir/origin_csi_file/filename.txt

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
