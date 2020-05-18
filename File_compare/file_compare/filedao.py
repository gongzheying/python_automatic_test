import sqlite3

DATABASE_NAME = "ibsps_compare.db"


class SplitFileInfo:
    __path = None
    __root_path = None
    __full_name = None
    __original_file = None

    def __init__(self, path, root_path, full_name, original_file):
        # type: (str,str,str,str) -> SplitFileInfo
        self.__path = path
        self.__root_path = root_path
        self.__full_name = full_name
        self.__original_file = original_file

    @property
    def original_file(self):
        # type: ()->str
        return self.__original_file

    @original_file.setter
    def original_file(self, value):
        # type: (str) -> None
        self.__original_file = value

    @property
    def root_path(self):
        # type: ()->str
        return self.__root_path

    @root_path.setter
    def root_path(self, value):
        # type: (str) -> None
        self.__root_path = value

    @property
    def path(self):
        # type: ()->str
        return self.__path

    @path.setter
    def path(self, value):
        # type: (str) -> None
        self.__path = value

    @property
    def full_name(self):
        # type: ()->str
        return self.__full_name

    @full_name.setter
    def full_name(self, value):
        # type: (str) -> None
        self.__full_name = value


class SplitFileInfoNew(SplitFileInfo):
    def __init__(self, path, root_path, full_name, original_file):
        # type: (str,str,str,str) -> SplitFileInfoNew
        SplitFileInfo.__init__(self, path, root_path, full_name, original_file)


class SplitFileInfoOld(SplitFileInfo):
    def __init__(self, path, root_path, full_name, original_file):
        # type: (str,str,str,str) -> SplitFileInfoOld
        SplitFileInfo.__init__(self, path, root_path, full_name, original_file)


class SplitFileInfoVO(SplitFileInfo):
    __new_root_path = None
    __new_full_name = None

    def __init__(self, path, new_root_path, new_full_name, root_path, full_name, old_name):
        # type: (str,str,str,str,str,str) -> SplitFileInfoVO
        SplitFileInfo.__init__(self, path, root_path, full_name, old_name)
        self.__new_root_path = new_root_path
        self.__new_full_name = new_full_name

    @property
    def new_root_path(self):
        # type: ()->str
        return self.__new_root_path

    @new_root_path.setter
    def new_root_path(self, value):
        # type: (str) -> None
        self.__new_root_path = value

    @property
    def new_full_name(self):
        # type: ()->str
        return self.__new_full_name

    @new_full_name.setter
    def new_full_name(self, value):
        # type: (str) -> None
        self.__new_full_name = value


def add(entities, new):
    # type: (list, bool) -> None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        data = [(e.path, e.root_path, e.full_name, e.original_file) for e in entities]
        if new:
            cursor.executemany(
                "insert into FC_SPLIT_FILE_INF_NEW(path,rootpath,fullname,filename) values(?,?,?,?);", data)
        else:
            cursor.executemany(
                "insert into FC_SPLIT_FILE_INF_OLD(path,rootpath,fullname,filename) values(?,?,?,?);", data)
        conn.commit()

        cursor.close()

    except sqlite3.Error as error:
        print "Error while insert data - {}".format(error)
    finally:
        if conn:
            conn.close()


def clear_data():
    # type: () -> None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        cursor.executescript("""
        drop table if exists FC_SPLIT_FILE_INF_VO;
        drop table if exists FC_SPLIT_FILE_INF_NEW;
        drop table if exists FC_SPLIT_FILE_INF_OLD;

        create table FC_SPLIT_FILE_INF_VO((path TEXT PRIMARY KEY,newrootpath TEXT,newfullname TEXT,rootpath TEXT,fullname TEXT,filename TEXT);
        create table FC_SPLIT_FILE_INF_NEW(path TEXT PRIMARY KEY,rootpath TEXT,fullname TEXT,filename TEXT);
        create table FC_SPLIT_FILE_INF_OLD(path TEXT PRIMARY KEY,rootpath TEXT,fullname TEXT,filename TEXT);
        """)
        conn.commit()

        cursor.close()

    except sqlite3.Error as error:
        print "Error while clean data - {}".format(error)
    finally:
        if conn:
            conn.close()


def count_same_record():
    # type: ()->int
    conn = None
    total = 0
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        cursor.execute("select count(*) from FC_SPLIT_FILE_INF_VO; ")
        total = cursor.fetchone()[0]

        cursor.close()

    except sqlite3.Error as error:
        print "Error while fetch data - {}".format(error)
    finally:
        if conn:
            conn.close()
    return total


def into_same_record():
    # type: () -> None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        cursor.executescript("""
        insert into FC_SPLIT_FILE_INF_VO(path,newrootpath,newfullname,rootpath,fullname,filename) 
        select a.path,a.rootpath,a.fullname,b.rootpath,b.fullname,a.filename 
        from FC_SPLIT_FILE_INF_NEW as a join FC_SPLIT_FILE_INF_OLD as b 
          on a.path=b.path;
        """)
        conn.commit()

        cursor.close()

    except sqlite3.Error as error:
        print "Error while insert data - {}".format(error)
    finally:
        if conn:
            conn.close()


def query_more_files(more_files, index, new):
    # type: (str,int,bool) -> list
    conn = None
    res = list()
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        if new:
            cursor.execute("""
            select path,rootpath,fullname,filename from FC_SPLIT_FILE_INF_NEW 
            where filename not in ({})  
              and path not in (select path from FC_SPLIT_FILE_INF_OLD)
            limit 500 offset {}
            """.format(more_files, index))
            rows = cursor.fetchall()
            for row in rows:
                res.append(SplitFileInfoNew(row[0], row[1], row[2], row[3]))
        else:
            cursor.execute("""
            select path,rootpath,fullname,filename from FC_SPLIT_FILE_INF_NEW 
            where filename not in ({})  
              and path not in (select path from FC_SPLIT_FILE_INF_OLD)
            limit 500 offset {}
            """.format(more_files, index))
            rows = cursor.fetchall()
            for row in rows:
                res.append(SplitFileInfoOld(row[0], row[1], row[2], row[3]))

        cursor.close()

    except sqlite3.Error as error:
        print "Error while fetch data - {}".format(error)
    finally:
        if conn:
            conn.close()
    return res


def query_same_record(index):
    # type: (int) -> list
    conn = None
    res = list()
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        cursor.execute("""
        select path,newrootpath,newfullname,rootpath,fullname,filename from FC_SPLIT_FILE_INF_VO 
        limit 500 offset {}
        """.format(index))
        rows = cursor.fetchall()
        for row in rows:
            res.append(SplitFileInfoVO(row[0], row[1], row[2], row[3], row[4], row[5]))
        cursor.close()

    except sqlite3.Error as error:
        print "Error while fetch data - {}".format(error)
    finally:
        if conn:
            conn.close()
    return res
