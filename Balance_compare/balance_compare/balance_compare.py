"""
Created on Jan 6, 2020

@author: wangyx
"""
import csv
import logging
import os
import pkgutil
import shutil

import cx_Oracle


class BalanceCompare(object):
    """
    classdocs
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = logging.getLogger("compare.balance")

    @staticmethod
    def __date_format(date):
        """
        "2017-12-02" to "171202"
        """
        new_date = date.replace("-", "")
        return new_date[2:]

    def run(self, bsp, date, conn_url):
        # type: (str,str,str) -> None

        date = self.__date_format(date)
        
        with cx_Oracle.connect(conn_url) as connection:
            with connection.cursor() as cursor:
                
                try:
                    package_sql = pkgutil.get_data("balance_compare", "resources/PCK_AUTOTEST_BALANCE.sql")
                    body_sql = pkgutil.get_data("balance_compare", "resources/PCK_AUTOTEST_BALANCE_BODY.sql")

                    cursor.execute(package_sql)
                    self.__logger.info("init PCK_AUTOTEST_BALANCE.sql completed")
                    
                    cursor.execute(body_sql)
                    self.__logger.info("init PCK_AUTOTEST_BALANCE_BODY.sql completed")
                except cx_Oracle.Error as ex:
                    self.__logger.error("init PCK_AUTOTEST_BALANCE.sql failed : %s", ex)
                    
                cursor.callproc('PCK_AUTOTEST_BALANCE.p_create_balance', [bsp, date])  
                cursor.callproc('PCK_AUTOTEST_BALANCE.p_insert_balance', [bsp, date])
                
                sql = "select * from RET_BALANCE_" + bsp + "_" + date
                d = "%s/.datacompare/balance/%s" % (os.environ["HOME"], bsp + "_" + date + "_balance")
                
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.makedirs(d)
                self.__logger.info("result folder created: %s", d)
                
                with open(d + os.sep + bsp + "_" + date + '_balance.csv', 'w') as f:
                    f_csv = csv.writer(f)
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    f_csv.writerows(result)
                    
                self.__logger.info("result file created: %s_%s_balance.csv", bsp, date)
                self.__logger.info("-----balance compare completed-----")

