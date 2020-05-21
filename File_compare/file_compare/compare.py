import ConfigParser
import io
import logging
import os
import pkgutil
import re
import shutil
import string

import cx_Oracle
from enum import Enum

from file_compare import csidiff, csisplit, hotdiff, hotsplit


class CompareType(Enum):
    HOT = 1
    CSI = 2


class Compare(object):

    @staticmethod
    def __get_conf():
        cf = ConfigParser.RawConfigParser()
        default_conf = os.path.join(os.getcwd(), "compare.ini")
        if os.path.exists(default_conf):
            cf.read(default_conf)
        else:
            default_conf_text = pkgutil.get_data(__package__, "compare.ini")
            cf.readfp(io.BytesIO(default_conf_text))
        return cf

    def __init__(self, compare_type, **kwargs):
        # type: (CompareType, dict) -> None

        cf = self.__get_conf()

        self.__type = compare_type.name
        self.__logger = logging.getLogger("compare.{}".format(self.__type))

        self.__conn_url = kwargs.get("connUrl", cf.get("database", "nbs_common_connstr"))
        self.__output_root = kwargs.get("outputRoot", cf.get("ibsp", "outbound_online_root"))
        self.__base_root = kwargs.get("baseRoot", cf.get("ibsp", "outbound_baseline_root"))

        home_dir = os.environ.get("HOME", ".")
        self.__result_root = "{}/.filecompare/{}/resultPath".format(home_dir, self.__type)
        self.__new_file_path = "{}/.filecompare/{}/new".format(home_dir, self.__type)
        self.__old_file_path = "{}/.filecompare/{}/old".format(home_dir, self.__type)
        self.__new_app_path = "{}/.filecompare/{}/newPath".format(home_dir, self.__type)
        self.__old_app_path = "{}/.filecompare/{}/oldPath".format(home_dir, self.__type)

    @property
    def logger(self):
        return self.__logger

    @property
    def type(self):
        return self.__type

    @property
    def conn_url(self):
        return self.__conn_url

    @property
    def output_root(self):
        return self.__output_root

    @property
    def base_root(self):
        return self.__base_root

    @property
    def result_root(self):
        return self.__result_root

    @property
    def new_file_path(self):
        return self.__new_file_path

    @property
    def old_file_path(self):
        return self.__old_file_path

    @property
    def new_app_path(self):
        return self.__new_app_path

    @property
    def old_app_path(self):
        return self.__old_app_path

    def get_output_sql(self, bsp_code, create_date):
        # type: (str,str)->str
        return None

    def __get_output_files(self, bsp_code, create_date):
        # type: (str,str)-> list

        sql = self.get_output_sql(bsp_code, create_date)
        data = []
        if sql is not None:
            try:
                with cx_Oracle.connect(self.conn_url, encoding="UTF-8") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql)
                        data = []
                        for path, in cursor:
                            data.append(path)

            except cx_Oracle.Error as ex:
                self.logger.error("Get file path failed: %s ", ex)

        return data

    def __clean_data_dirs(self, data_dirs):
        # type: (tuple) -> None

        """
        clean data dirs
        """
        for dataDir in data_dirs:
            if os.path.exists(dataDir):
                for child in os.listdir(dataDir):
                    child_path = os.path.join(dataDir, child)
                    if os.path.isdir(child_path):
                        shutil.rmtree(child_path)
                    else:
                        os.remove(child_path)
                self.logger.debug("Success: rm directory %s", dataDir)
            else:
                os.makedirs(dataDir, mode=0o755)
                self.logger.debug("Create directory %s", dataDir)

    def __replace_path(self, output_path):
        # type: (str) -> str

        """
        rename output path to base path
        """
        root = str(self.output_root)
        while len(root) > 0 and root.endswith("/"):
            root = root[0:len(root)]

        pattern = re.compile(self.output_root + "/+(AsiaPacific|Europe)")
        base_path = re.sub(pattern, self.base_root, output_path)
        return base_path

    def __copy_outputs(self, src, dst):
        # type: (str,str) -> None

        """
        copy output files
        """
        if os.path.exists(src):
            shutil.copy2(src, dst)
            self.logger.debug("Success: copy file %s", src)
        else:
            self.logger.debug("No such file %s", src)

    def __write_config(self, result_path):
        # type: (str) -> None

        """
        rewrite compare config 
        """
        fh = None
        try:
            fn = "%s/config.txt" % self.result_root

            fh = open(fn, "w")
            fh.write("new=%s%s" % (self.new_file_path, os.linesep))
            fh.write("old=%s%s" % (self.old_file_path, os.linesep))
            fh.write("newPath=%s%s" % (self.new_app_path, os.linesep))
            fh.write("oldPath=%s%s" % (self.old_app_path, os.linesep))
            fh.write("resultPath=%s%s" % (result_path, os.linesep))

            self.logger.debug("Success: modify config file %s", fn)

        except OSError as ex:
            self.logger.error("Modify config file failed: %s", ex)
        finally:
            if fh is not None:
                fh.close()

    def run_compare(self):
        # type: () -> bool

        """
        call compare.sh under scriptDir, if success return True
        """
        return True

    def run(self, bsp, date):
        # type: (str,str) -> None

        """
        run file compare tool, and return result path
        """
        self.logger.info("Start file comparision")
        result_path = "{}/{}_{}".format(self.result_root, bsp, date)

        data_dirs = (self.new_file_path, self.old_file_path, self.new_app_path, self.old_app_path, result_path)
        self.__clean_data_dirs(data_dirs)

        hot_files = self.__get_output_files(bsp, date)
        if hot_files is not None:
            for item in hot_files:
                path_new = str(item)
                path_old = self.__replace_path(str(item))
                self.__copy_outputs(path_new, self.new_file_path)
                self.__copy_outputs(path_old, self.old_file_path)

        self.__write_config(result_path)

        if self.run_compare():
            self.logger.info("File comparision is completed and the result is stored in %s", result_path)
        else:
            self.logger.warning("File comparision is failed")


class CsiCompare(Compare):

    def __init__(self, **kwargs):
        # type: (dict) -> None

        super(CsiCompare,self).__init__(CompareType.CSI, **kwargs)

    def get_output_sql(self, bsp_code, create_date):
        # type: (str,str) -> str

        sql_text = pkgutil.get_data(__package__, "resources/get_output_of_csi.sql")
        sql_template = string.Template(sql_text)

        params = dict(bsp=bsp_code, date=create_date)
        sql = sql_template.substitute(params)
        return sql

    def run_compare(self):
        conf = "{}/config.txt".format(self.result_root)
        return csisplit.run(conf) and csidiff.run(conf)


class HotCompare(Compare):

    def __init__(self, **kwargs):
        # type: (dict) -> None

        super(HotCompare, self).__init__(CompareType.HOT, **kwargs)

    def get_output_sql(self, bsp_code, create_date):
        # type: (str,str) -> str

        sql_text = pkgutil.get_data(__package__, "resources/get_output_of_hot.sql")
        sql_template = string.Template(sql_text)

        params = dict(bsp=bsp_code, date=create_date)
        sql = sql_template.substitute(params)
        return sql

    def run_compare(self):
        conf = "{}/config.txt".format(self.result_root)
        return hotsplit.run(conf) and hotdiff.run(conf)
