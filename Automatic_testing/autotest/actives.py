from datetime import datetime, timedelta
import logging
import os
import pkgutil
import shutil
import string
import subprocess
import time

import cx_Oracle
import luigi

from autotest import context
from balance_compare.balance_compare import BalanceCompare
from billing_compare.billing_compare import BillingCompare
from file_compare.compare import CsiCompare, HotCompare 


class Activity(luigi.Task):

    def __init__(self, *args, **kwargs):
        self.__done = False
        self.__ctx = context.Instance()
        self.__logger = logging.getLogger(self.familyName())
        super().__init__(*args, **kwargs)

    def get_done(self):
        return self.__done

    def set_done(self, value):
        self.__done = value

    def get_ctx(self):
        return self.__ctx
    
    def get_logger(self):
        return self.__logger    
    
    done = property(get_done, set_done, None, None)
    ctx = property(get_ctx, None, None, None)
    logger = property(get_logger, None, None, None)
        
    def complete(self):
        return self.done

    def run(self):
        try:
            self.done = False
            self.runIt()
            self.done = True
        except (subprocess.SubprocessError, RuntimeError) as ex:
            self.logger.error("an error occurred :\n\t%s", ex)
            self.done = False

    def runIt(self):
        pass
    
    @classmethod
    def familyName(cls):
        return cls.__name__

    def runCommand(self, commandLine):
        proc = subprocess.run(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8")
        if proc.returncode != 0:
            self.logger.warning("the following command may not exist correctly : \n%s\n%s\n%s", commandLine, "stdout/stderr".center(80, "*"), proc.stdout)


class DailyActivity(Activity):
    date = luigi.DateParameter()

        
class Activity1stStopServices(Activity):

    @classmethod
    def familyName(cls):
        return "autotest.stopservices"

    def runIt(self):
        self.logger.info("stopping jboss: webapp")
        for jb in self.ctx.webapps:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping jboss: executor")
        for jb in self.ctx.executors:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping jboss: scheduler")
        for jb in self.ctx.schedulers:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping Bo")
        bo = self.ctx.bo
        self.runCommand(bo.stopCommand())

        self.logger.info("stop service is completed")


class Activity2ndClearTempfiles(Activity):

    @classmethod
    def familyName(cls):
        return "autotest.cleartempfiles"

    def requires(self):
        return Activity1stStopServices()
        
    def runIt(self):
        self.logger.info("deleting the files in the input, input backup and jboss logs directories")
        for rmdir in (self.ctx.task.inputDir , self.ctx.task.inputBackupDir, self.ctx.task.logDir):
            if os.path.exists(rmdir):
                for root, dirs, files in os.walk(rmdir, topdown=False):  # @UnusedVariable
                    for file in files:
                        os.remove(os.path.join(root, file))
        
        self.logger.info("temp files have been deleted")


class Activity3rdRollbackDatabase(Activity):
    
    @classmethod
    def familyName(cls):
        return "autotest.rollbackdatabase"

    def requires(self):
        return Activity2ndClearTempfiles()
        
    def runIt(self):
        
        # TODO:need to add one step of "drop user and init user"
        # self.logger.info("drop user")

        self.logger.info("rollbacking nbs_common schema")
        self.impdp(self.ctx.task.nbsCommonConnStr, self.ctx.task.nbsCommonDumpfile)
        
        self.logger.info("rollbacking nbs schema")
        self.impdp(self.ctx.task.nbsConnStr, self.ctx.task.nbsDumpfile)
        
        self.logger.info("patching nbs_common sql")
        self.patch(self.ctx.task.nbsCommonConnStr, self.ctx.task.nbsCommonPatch)
        
        self.logger.info("patching nbs sql")
        self.patch(self.ctx.task.nbsConnStr, self.ctx.task.nbsPatch)
        
        self.logger.info("nbs and nbs_common have been rollback")
    
    def impdp(self, connect:str, dumpfilepath:str):
        
        dumpdir = os.path.dirname(dumpfilepath)
        dumpfile = os.path.basename(dumpfilepath)
        dumpdirname = None  # @UnusedVariable
        with cx_Oracle.connect(connect, encoding="UTF-8") as connection:
            with connection.cursor() as cursor:
                cursor.execute("select directory_name from dba_directories where directory_path=:dumpdir", dumpdir=dumpdir)
                dumpdirname, = cursor.fetchone()

        if dumpdirname is None:
            raise RuntimeError("the oracle directory '%s' is undefined" % (dumpdir)) 
        
        impdpCommand = "impdp {0} directory={1} dumpfile={2} logfile=imp_{2}.log table_exists_action=REPLACE ".format(connect, dumpdirname, dumpfile)
        self.runCommand(impdpCommand)
        
    # def dropUser(self):
        # print()
        
    def patch(self, connect:str, filepath:str):
        sqltext = pkgutil.get_data(__package__, "resources/{0}".format(filepath)).decode(encoding="utf-8")
        sqllist = [ sql.strip() for sql in sqltext.split(";") if bool(sql.strip())]
        
        if len(sqllist) > 0:
            with cx_Oracle.connect(connect, encoding="UTF-8") as connection:
                with connection.cursor() as cursor:
                    for sql in sqllist:
                        cursor.execute(sql)
                connection.commit()


class DailyActivity1stChangeSysdate(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.changesysdate"

    def runIt(self):
        self.logger.info("stopping jboss: webapp")
        for jb in self.ctx.webapps:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping jboss: executor")
        for jb in self.ctx.executors:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping jboss: scheduler")
        for jb in self.ctx.schedulers:
            self.runCommand(jb.stopCommand())
        
        self.logger.info("stopping Bo")
        bo = self.ctx.bo
        self.runCommand(bo.stopCommand())

        self.logger.info("stop service is completed")
        
        self.logger.info("changing system date to %s: webapp", self.date)
        for jb in self.ctx.webapps:
            self.runCommand(jb.changeSysdateCommand(self.date))
        
        self.logger.info("changing system date to %s: executor", self.date)
        for jb in self.ctx.executors:
            self.runCommand(jb.changeSysdateCommand(self.date))
        
        self.logger.info("changing system date to %s: scheduler", self.date)
        for jb in self.ctx.schedulers:
            self.runCommand(jb.changeSysdateCommand(self.date))
        
        self.logger.info("changing system date to %s: Bo", self.date)
        bo = self.ctx.bo
        self.runCommand(bo.changeSysdateCommand(self.date))

        self.logger.info("change system date is completed")


class DailyActivity2ndResetQuartz(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.resetquartz"

    def requires(self):
        return DailyActivity1stChangeSysdate(date=self.date)
    
    def runIt(self):
        self.logger.info("deleting the quartz data")
        sqltext = pkgutil.get_data(__package__, "resources/del_qrtz.sql").decode(encoding="utf-8")
        sqllist = [ sql.strip() for sql in sqltext.split(";") if bool(sql.strip())]
        
        if len(sqllist) > 0:
            with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
                with connection.cursor() as cursor:
                    for sql in sqllist:
                        cursor.execute(sql)
                connection.commit()

        self.logger.info("the quartz data have been deleted")


class DailyActivity3rdStartServices(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.startservices"

    def requires(self):
        return DailyActivity2ndResetQuartz(date=self.date)
    
    def runIt(self):

        self.logger.info("starting jboss: scheduler")
        for jb in self.ctx.schedulers:
            self.runCommand(jb.startCommand())
        
        self.logger.info("starting jboss: webapp")
        for jb in self.ctx.webapps:
            self.runCommand(jb.startCommand())
        
        self.logger.info("starting jboss: executor")
        for jb in self.ctx.executors:
            self.runCommand(jb.startCommand())
        
        self.logger.info("starting Bo")
        bo = self.ctx.bo
        self.runCommand(bo.startCommand())

        sleepSeconds = 5 * 60
        self.logger.info("waiting %s seconds for all servers to be ready", sleepSeconds)
        time.sleep(sleepSeconds)
        self.logger.info("start service is completed")


class DailyActivity4thCopyInputfiles(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.copyinputfiles"

    def requires(self):
        return DailyActivity3rdStartServices(date=self.date)      
   
    def runIt(self):
        sleepSeconds = 3 * 60
        for seq in (1, 2, 3, 4):
            self.logger.info("copying seq.%s input files in %s", seq, self.date)

            if seq == 1:
                srcs = [ "%s/%s/%s/ACLI" % (self.ctx.task.inputSrcDir, self.date.strftime("%Y%m%d"), seq) ]  # pylint: disable=no-member
            else:
                srcs = [ "%s/%s/%s/%s" % (self.ctx.task.inputSrcDir, self.date.strftime("%Y%m%d"), seq, bsp) for bsp in self.ctx.task.bspCodes ]  # pylint: disable=no-member

            for src in srcs:
                for root, dirs, files in os.walk(src):  # @UnusedVariable
                    for file in files:
                        destfile = file if file.endswith(".COMP") else "%s.COMP" % file
                        shutil.copy2(os.path.join(root, file), os.path.join(self.ctx.task.inputDir, destfile))
             
            self.logger.info("waiting %s seconds for seq.%s input files in %s to be ready", sleepSeconds, seq, self.date)
            time.sleep(sleepSeconds)

        self.logger.info("copy input files in %s is completed", self.date)
        

class DailyActivity5thCheckInputfiles(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autoetst.checkinputfiles"

    def requires(self):
        return DailyActivity4thCopyInputfiles(date=self.date)
    
    def runIt(self):
        self.logger.info("checking the process status of input files in %s ", self.date)
        sqlText = pkgutil.get_data(__package__, "resources/check_inputfile.sql").decode(encoding="utf-8")
        sqlTemplate = string.Template(sqlText)
        
        params = dict(bsp="','".join(self.ctx.task.bspCodes), date=self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        sql = sqlTemplate.substitute(params)
        
        pollingSeconds = 2 * 60 * 60
        intervalSeconds = 30
        expectedEnd = datetime.today() + timedelta(seconds=pollingSeconds)
        
        while datetime.today() < expectedEnd:
            
            with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
                with connection.cursor() as cursor:
                    allCompleted = True
                    for bsp, completed, rejected, failed, numbers, unloaded, diff in cursor.execute(sql):
                        self.logger.info("%s input files count:%s, completed:%s, rejectd:%s, failed:%s ", bsp, numbers, completed, rejected, int(failed) + int(unloaded))
                        if int(diff) != 0:
                            allCompleted = False
                            
                    if allCompleted:
                        self.logger.info("all of input files in %s have been done", self.date)
                        break
            
            time.sleep(intervalSeconds)
                   
        else:
            raise RuntimeError("all of input files in %s have not been loaded in iBSPs in time" % self.date)   

        
class DailyActivity6thRunJobs(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.runjobs"

    def requires(self):
        return DailyActivity5thCheckInputfiles(date=self.date)
    
    def runIt(self):
        self.startNetfare()
        self.checkNetfare()        
        self.startJobs()
        self.checkJobs()
        self.logger.info("all of jobs in %s have been done" % self.date)
        return True

    def startNetfare(self):

        self.logger.info("starting netfare in %s", self.date)
        sqlText = pkgutil.get_data(__package__, "resources/start_netfare.sql").decode(encoding="utf-8")
        sqlTemplate = string.Template(sqlText)
        
        params = dict(bsp="','".join(self.ctx.task.bspCodes), date=self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        sql = sqlTemplate.substitute(params)
        
        sleepSeconds = 3 * 60
            
        with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()                
        
        self.logger.info("waiting %s seconds for netfare in %s to be ready", sleepSeconds, self.date)
        time.sleep(sleepSeconds)
        self.logger.info("netfare in %s has been started", self.date)
    
    def checkNetfare(self):

        self.logger.info("checking the process status of netfare in %s", self.date)
        sqlText = pkgutil.get_data(__package__, "resources/check_netfare.sql").decode(encoding="utf-8")
        sqlTemplate = string.Template(sqlText)
        
        params = dict(bsp="','".join(self.ctx.task.bspCodes), date=self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        sql = sqlTemplate.substitute(params)
        
        pollingSeconds = 60 * 60
        intervalSeconds = 30
        expectedEnd = datetime.today() + timedelta(seconds=pollingSeconds)
        
        while datetime.today() < expectedEnd:
            
            with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
                with connection.cursor() as cursor:
                    allCompleted = True
                    for bsp, completed, failed, numbers, diff in cursor.execute(sql):
                        self.logger.info("%s netfare count:%s, completed:%s, failed:%s ", bsp, numbers, completed, failed)
                        if int(diff) != 0:
                            allCompleted = False
                            
                    if allCompleted:
                        self.logger.info("netfare in %s has been done", self.date)
                        break
            
            time.sleep(intervalSeconds)
                   
        else:
            raise RuntimeError("netfare in %s has not been loaded in iBSPs in time" % self.date)    
        
    def startJobs(self):
        
        self.logger.info("starting jobs in %s", self.date)
        sqlText = pkgutil.get_data(__package__, "resources/start_jobs.sql").decode(encoding="utf-8")
        sqlTemplate = string.Template(sqlText)
        
        params = dict(bsp="','".join(self.ctx.task.bspCodes), date=self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        sql = sqlTemplate.substitute(params)
        
        sleepSeconds = 3 * 60
            
        with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()                
        
        self.logger.info("waiting %s seconds for jobs in %s to be ready", sleepSeconds, self.date)
        time.sleep(sleepSeconds)
        self.logger.info("jobs in %s have been started", self.date)   
    
    def checkJobs(self):

        self.logger.info("checking the process status of jobs in %s", self.date)
        sqlText = pkgutil.get_data(__package__, "resources/check_jobs.sql").decode(encoding="utf-8")
        sqlTemplate = string.Template(sqlText)
        
        params = dict(bsp="','".join(self.ctx.task.bspCodes), date=self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        sql = sqlTemplate.substitute(params)
        
        pollingSeconds = 4 * 60 * 60
        intervalSeconds = 30
        expectedEnd = datetime.today() + timedelta(seconds=pollingSeconds)
        
        while datetime.today() < expectedEnd:
            
            with cx_Oracle.connect(self.ctx.task.nbsCommonConnStr, encoding="UTF-8") as connection:
                with connection.cursor() as cursor:
                    allCompleted = True
                    for bsp, completed, failed, numbers, diff in cursor.execute(sql):
                        self.logger.info("%s jobs count:%s, completed:%s, failed:%s ", bsp, numbers, completed, failed)
                        if int(diff) != 0:
                            allCompleted = False
                            
                    if allCompleted:
                        self.logger.info("jobs in %s have been done", self.date)
                        break
            
            time.sleep(intervalSeconds)
                   
        else:
            raise RuntimeError("jobs in %s have not been loaded in iBSPs in time" % self.date)    
    

class DailyActivity7thCompareBalance(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.comparebalance"

    def runIt(self):
        self.logger.info("init balance data in %s", self.date)
        self.logger.info("compare balance data in %s", self.date)
        
        bala = BalanceCompare()
        
        for bsp in self.ctx.task.bspCodes:
            bala.run(bsp, self.date.strftime("%Y-%m-%d"), self.ctx.task.nbsConnStr)  # pylint: disable=no-member
        
        self.logger.info("compare balance data in %s has been done", self.date)


class DailyActivity8thCompareBilling(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.comparebilling"
   
    def runIt(self):
        self.logger.info("compare billing data in %s", self.date)
        
        bill = BillingCompare()
        
        for bsp in self.ctx.task.bspCodes:
            bill.run(bsp, self.date.strftime("%Y-%m-%d"), self.ctx.task.nbsCommonConnStr)  # pylint: disable=no-member
        
        self.logger.info("compare billing data in %s has been done", self.date)


class DailyActivity9thCompareCsi(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.comparecsi"
    
    def runIt(self):
        self.logger.info("comparing csi data in %s", self.date)
        cmp = CsiCompare(connUrl=self.ctx.task.nbsCommonConnStr,
                         outputRoot=self.ctx.task.outputDir,
                         baseRoot=self.ctx.task.outputBaseDir,
                         splitJar=self.ctx.task.csiSplitJar,
                         diffJar=self.ctx.task.csiDiffJar)
        for bsp in self.ctx.task.bspCodes:
            cmp.run(bsp, self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        self.logger.info("compare csi file in %s has been done", self.date)


class DailyActivity10thCompareHot(DailyActivity):

    @classmethod
    def familyName(cls):
        return "autotest.comparehot"
    
    def runIt(self):
        self.logger.info("comparing hot data in %s", self.date)
        cmp = HotCompare(connUrl=self.ctx.task.nbsCommonConnStr,
                         outputRoot=self.ctx.task.outputDir,
                         baseRoot=self.ctx.task.outputBaseDir,
                         splitJar=self.ctx.task.hotSplitJar,
                         diffJar=self.ctx.task.hotDiffJar)
        for bsp in self.ctx.task.bspCodes:
            cmp.run(bsp, self.date.strftime("%Y-%m-%d"))  # pylint: disable=no-member
        self.logger.info("compare hot file in %s has been done", self.date)
        
