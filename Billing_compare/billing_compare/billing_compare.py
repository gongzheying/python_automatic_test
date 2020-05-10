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


class BillingCompare(object):
    """
    classdocs
    """

    __sql = ("STA_AIR_STATIS.sql",
             "STA_SETTLE_SUMMARY.sql",
             "STA_PARTI_SUMMARY_AGT.sql",
             "STA_PARTI_SUMMARY_AIR.sql",
             "STA_REMIT_SUMMARY.sql",
             "STA_SETTLE_DERAIL.sql")

    def __init__(self):
        """
        Constructor
        """
        self.__logger = logging.getLogger("compare.billing")

    @staticmethod
    def __sql_to_str(name, bsp, period):
        sql_tmpl = pkgutil.get_data("billing_compare", "resources/" + name).decode(encoding="utf-8")
        sql = sql_tmpl.replace("&period", period).replace("&bsp", bsp).replace("'&nbs'", "nbs")
        return sql

    def run(self, bsp, date, conn_url):
        # type: (str,str,str) -> None

        with cx_Oracle.connect(conn_url) as connection:
            with connection.cursor() as cursor:

                sql = "select bill_period from ref_billperiod  where bsp_code=:sqlbsp and to_date(:sqldate," \
                      "'yyyy-mm-dd') between start_date and end_date "

                cursor.execute(sql, sqlbsp=bsp, sqldate=date)
                row = cursor.fetchone()
                period = row[0]

                d = "%s/.datacompare/billing/%s" % (os.environ["HOME"], bsp + "_" + date + "_billing")
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.makedirs(d)
                logging.debug("result folder created: %s", d)
                for s in self.__sql:
                    with open(d + os.sep + s + '.csv', 'w') as f:
                        f_csv = csv.writer(f)
                        sql = self.__sql_to_str(s, bsp, period)
                        cursor.execute(sql)
                        result = cursor.fetchall()
                        f_csv.writerows(result)
                    logging.info("result file created: %s.csv", s)
        logging.info("-----billing compare completed-----")
