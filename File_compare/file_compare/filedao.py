import sqlite3

DATABASE_NAME = "ibsps_compare.db"


class SplitFileInfo:
    __file_name = None
    __root_path = None
    __path = None
    __full_name = None

    def __init__(self, path, root_path, full_name, old_name):
        # type: (str,str,str,str) -> SplitFileInfo
        self.__path = path
        self.__root_path = root_path
        self.__full_name = full_name
        self.__file_name = old_name

    @property
    def file_name(self):
        # type: ()->str
        return self.__file_name

    @file_name.setter
    def file_name(self, value):
        # type: (str) -> None
        self.__file_name = value

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
    def __init__(self, path, root_path, full_name, old_name):
        # type: (str,str,str,str) -> SplitFileInfoNew
        SplitFileInfo.__init__(self, path, root_path, full_name, old_name)


class SplitFileInfoOld(SplitFileInfo):
    def __init__(self, path, root_path, full_name, old_name):
        # type: (str,str,str,str) -> SplitFileInfoOld
        SplitFileInfo.__init__(self, path, root_path, full_name, old_name)


class SplitFileInfoVO(SplitFileInfo):
    __new_root_path = None
    __new_full_name = None
    __new_old_name = None

    def __init__(self, path, new_root_path, new_full_name, new_old_name, root_path, full_name, old_name):
        # type: (str,str,str,str,str,str,str) -> SplitFileInfoVO
        SplitFileInfo.__init__(self, path, root_path, full_name, old_name)
        self.__new_root_path = new_root_path
        self.__new_full_name = new_full_name
        self.__new_old_name = new_old_name

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

    @property
    def new_old_name(self):
        # type: ()->str
        return self.__new_old_name

    @new_old_name.setter
    def new_old_name(self, value):
        # type: (str) -> None
        self.__new_old_name = value


def add(entities, new):
    # type: (list, bool) -> None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.cursor()

        data = [(e.path, e.root_path, e.full_name, e.file_name) for e in entities]
        if new:
            cursor.executemany(
                "insert into FC_SPLIT_FILE_INF_NEW(path,rootpath,fullname,oldname) values(?,?,?,?);", data)
        else:
            cursor.executemany(
                "insert into FC_SPLIT_FILE_INF_OLD(path,rootpath,fullname,oldname) values(?,?,?,?);", data)
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

        create table FC_SPLIT_FILE_INF_VO((path TEXT PRIMARY KEY,newrootpath TEXT,newfullname TEXT,newoldname TEXT,rootpath TEXT,fullname TEXT,oldname TEXT);
        create table FC_SPLIT_FILE_INF_NEW(path TEXT PRIMARY KEY,rootpath TEXT,fullname TEXT,oldname TEXT);
        create table FC_SPLIT_FILE_INF_OLD(path TEXT PRIMARY KEY,rootpath TEXT,fullname TEXT,oldname TEXT);
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
        insert into FC_SPLIT_FILE_INF_VO(path,newrootpath,newfullname,newoldname,rootpath,fullname,oldname) 
        select a.path,a.rootPath,a.fullname,a.oldname,b.rootpath,b.fullname,b.oldname 
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
            select path,rootpath,fullname,oldname from FC_SPLIT_FILE_INF_NEW 
            where oldname not in ({})  
              and path not in (select path from FC_SPLIT_FILE_INF_OLD)
            limit 500 offset {}
            """.format(more_files, index))
            rows = cursor.fetchall()
            for row in rows:
                res.append(SplitFileInfoNew(row[0], row[1], row[2], row[3]))
        else:
            cursor.execute("""
            select path,rootpath,fullname,oldname from FC_SPLIT_FILE_INF_NEW 
            where oldname not in ({})  
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
        select path,newrootpath,newfullname,newoldname,rootpath,fullname,oldname from FC_SPLIT_FILE_INF_VO 
        limit 500 offset {}
        """.format(index))
        rows = cursor.fetchall()
        for row in rows:
            res.append(SplitFileInfoVO(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        cursor.close()

    except sqlite3.Error as error:
        print "Error while fetch data - {}".format(error)
    finally:
        if conn:
            conn.close()
    return res
