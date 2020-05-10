import configparser
import datetime
import enum
import os
import pkgutil
import string
import threading

IfNull = lambda  x, y: y if x is None else x


class InstanceMeta(type):

    def __init__(self, *args, **kwargs):
        self.__instance = None
        self.__lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        with self.__lock:
            if self.__instance is None:
                self.__instance = super().__call__(*args, **kwargs)
        return self.__instance

        
class Instance(metaclass=InstanceMeta):
    
    def __getConf(self):
        cf = configparser.ConfigParser()
        defaultConf = os.path.join(os.getcwd(), "autotest.ini")
        if os.path.exists(defaultConf):
            cf.read(defaultConf, "utf-8")
        else:    
            defaultConfText = pkgutil.get_data(__package__, "autotest.ini").decode(encoding="utf-8")
            cf.read_string(defaultConfText)
        return cf
    
    def __init__(self):
        cf = self.__getConf()

        taskArgs = dict(cf.items("task"))
        self.__task = Task(**taskArgs)

        authArgs = dict(cf.items("server.auth"))
        
        appArgs = dict(cf.items("server.webapp"))
        appArgs.update(authArgs)
        self.__webapps = Webapp.create(**appArgs)
        
        exeArgs = dict(cf.items("server.executor"))
        exeArgs.update(authArgs)
        self.__executors = Executor.create(**exeArgs)
        
        schArgs = dict(cf.items("server.scheduler"))
        schArgs.update(authArgs)
        self.__schedulers = Scheduler.create(**schArgs)
        
        boArgs = dict(cf.items("server.bo"))
        boArgs.update(authArgs)
        self.__bo = Bo(**boArgs)

    def get_task(self):
        return self.__task

    def get_webapps(self):
        return self.__webapps

    def get_executors(self):
        return self.__executors

    def get_schedulers(self):
        return self.__schedulers

    def get_bo(self):
        return self.__bo
   
    task = property(get_task, None, None, None)
    webapps = property(get_webapps, None, None, None)
    executors = property(get_executors, None, None, None)
    schedulers = property(get_schedulers, None, None, None)
    bo = property(get_bo, None, None, None)
    
    
class Task():
    
    def __init__(self, **kwargs):
        self.__startDate = datetime.datetime.strptime(IfNull(kwargs.get("start_date"), "2017-12-02"), "%Y-%m-%d")   
        self.__endDate = datetime.datetime.strptime(IfNull(kwargs.get("end_date"), "2017-12-05"), "%Y-%m-%d")
        self.__bspCodes = IfNull(kwargs.get("bsp_codes"), "SE,LV,LT,TP").split(",")
        self.__logDir = IfNull(kwargs.get("log_dir"), "/ibspdata/LOG")

        self.__inputDir = IfNull(kwargs.get("input_dir"), "/ibspdata/SFTP/Channel/Inbound/Online/Inputs")
        self.__inputBackupDir = IfNull(kwargs.get("input_backup_dir"), "/ibspdata/SFTP/Channel/Inbound/Backup")
        self.__inputSrcDir = IfNull(kwargs.get("input_src_dir"), "/ibspdata/Autotest/input_src")

        self.__outputDir = IfNull(kwargs.get("output_dir"), "/ibspdata/APP/Online/Outbound/")
        self.__outputBaseDir = IfNull(kwargs.get("output_base_dir"), "/ibspdata/Autotest/output_files/")

        self.__csiSplitJar = IfNull(kwargs.get("csi_split_jar"), "/ibspdata/Autotest/csi/jar/fc-split-csi-1-jar-with-dependencies.jar")
        self.__csiDiffJar = IfNull(kwargs.get("csi_diff_jar"), "/ibspdata/Autotest/csi/jar/fc-diff-csi-1-jar-with-dependencies.jar")
        self.__hotSplitJar = IfNull(kwargs.get("hot_split_jar"), "/ibspdata/Autotest/hot/jar/fc-split-hot-1-jar-with-dependencies.jar")
        self.__hotDiffJar = IfNull(kwargs.get("hot_diff_jar"), "/ibspdata/Autotest/hot/jar/fc-diff-hot-1-jar-with-dependencies.jar")
        
        self.__nbsConnStr = IfNull(kwargs.get("nbs_connstr"), "nbs/nbs@isis2db")
        self.__nbsDumpfile = IfNull(kwargs.get("nbs_dumpfile"), "nbs_test.dmp")
        self.__nbsCommonConnStr = IfNull(kwargs.get("nbs_common_connstr"), "nbs_common/nbs_common@isis2db")
        self.__nbsCommonDumpfile = IfNull(kwargs.get("nbs_common_dumpfile"), "nbs_common_test.dmp")
        
        self.__nbsPatch = IfNull(kwargs.get("nbs_patch"), "nbs_patch.sql")
        self.__nbsCommonPatch = IfNull(kwargs.get("nbs_common_patch"), "nbs_common_patch.sql")

    def get_start_date(self):
        return self.__startDate

    def get_end_date(self):
        return self.__endDate

    def get_bsp_codes(self):
        return self.__bspCodes

    def get_log_dir(self):
        return self.__logDir

    def get_input_dir(self):
        return self.__inputDir

    def get_input_backup_dir(self):
        return self.__inputBackupDir

    def get_input_src_dir(self):
        return self.__inputSrcDir

    def get_output_dir(self):
        return self.__outputDir

    def get_output_base_dir(self):
        return self.__outputBaseDir

    def get_csi_split_jar(self):
        return self.__csiSplitJar

    def get_csi_diff_jar(self):
        return self.__csiDiffJar

    def get_hot_split_jar(self):
        return self.__hotSplitJar

    def get_hot_diff_jar(self):
        return self.__hotDiffJar

    def get_nbs_conn_str(self):
        return self.__nbsConnStr

    def get_nbs_dumpfile(self):
        return self.__nbsDumpfile

    def get_nbs_common_conn_str(self):
        return self.__nbsCommonConnStr

    def get_nbs_common_dumpfile(self):
        return self.__nbsCommonDumpfile
    
    def get_nbs_patch(self):
        return self.__nbsPatch
    
    def get_nbs_common_patch(self):
        return self.__nbsCommonPatch

    startDate = property(get_start_date, None, None, None)
    endDate = property(get_end_date, None, None, None)
    bspCodes = property(get_bsp_codes, None, None, None)
    logDir = property(get_log_dir, None, None, None)
    inputDir = property(get_input_dir, None, None, None)
    inputBackupDir = property(get_input_backup_dir, None, None, None)
    inputSrcDir = property(get_input_src_dir, None, None, None)
    outputDir = property(get_output_dir, None, None, None)
    outputBaseDir = property(get_output_base_dir, None, None, None)
    csiSplitJar = property(get_csi_split_jar, None, None, None)
    csiDiffJar = property(get_csi_diff_jar, None, None, None)
    hotSplitJar = property(get_hot_split_jar, None, None, None)
    hotDiffJar = property(get_hot_diff_jar, None, None, None)
    nbsConnStr = property(get_nbs_conn_str, None, None, None)
    nbsDumpfile = property(get_nbs_dumpfile, None, None, None)
    nbsCommonConnStr = property(get_nbs_common_conn_str, None, None, None)
    nbsCommonDumpfile = property(get_nbs_common_dumpfile, None, None, None)
    nbsPatch = property(get_nbs_patch, None, None, None)
    nbsCommonPatch = property(get_nbs_common_patch, None, None, None)


class ServerType(enum.Enum):
    SCH = 1
    APP = 2
    EXE = 3
    

class Server():
    
    def __init__(self, **kwargs):
        self.__host = kwargs.get("host")
        self.__user = kwargs.get("user")
        self.__identityFile = kwargs.get("identity_file")

    def get_host(self):
        return self.__host

    def get_user(self):
        return self.__user

    def get_identity_file(self):
        return self.__identityFile

    def get_is_root_user(self):
        return "root" == self.__user

    host = property(get_host, None, None, None)
    user = property(get_user, None, None, None)
    identityFile = property(get_identity_file, None, None, None)
    isRootUser = property(get_is_root_user, None, None, None)

    def submitCommand(self, command:str):
        """
        return ssh command 
        """
        sudoCommand = "sudo su -" if not self.isRootUser else ""
        submitCommand = ("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i '%s' %s@%s  <<'EOF'\n" + 
            "%s\n" + 
            "%s\n" + 
            "EOF\n") % (self.identityFile, self.user, self.host, sudoCommand, command)
        return submitCommand

    def changeSysdateCommand(self, sysdate:datetime.date):
        """
        return change sysdate command
        """
        commandLine = "date -s %s" % sysdate.strftime("%Y-%m-%d")
        return self.submitCommand(commandLine)
        
        
class Jboss(Server):

    @classmethod
    def create(cls, **kwargs):
        hosts = kwargs.get("host").split(",")
        servers = []
        for host in hosts:
            initargs = kwargs.copy()
            initargs.update(dict(host=host))
            servers.append(cls(**initargs))
        return servers
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__runAsUser = IfNull(kwargs.get("run_as_user"), "jboss")
        self.__javaHome = IfNull(kwargs.get("java_home"), "/usr/lib/jvm/java-8-openjdk-amd64/")
        self.__jbossHome = IfNull(kwargs.get("jboss_home"), "/opt/jboss-eap-7.1")
        self.__serverConfig = IfNull(kwargs.get("server_config"), "standalone-full.xml" if ServerType.SCH == self.serverType else "standalone.xml")
        self.__commandTemplate = string.Template(pkgutil.get_data(__package__, "resources/jboss.sh.tmpl").decode(encoding="utf-8"))

    def get_run_as_user(self):
        return self.__runAsUser

    def get_java_home(self):
        return self.__javaHome

    def get_jboss_home(self):
        return self.__jbossHome

    def get_server_config(self):
        return self.__serverConfig

    def stopCommand(self):
        """
        return stop jboss command 
        """
        params = dict(java_home=self.javaHome, jboss_home=self.jbossHome, server_config=self.serverConfig, run_as_user=self.runAsUser, command="stop")
        return self.submitCommand(self.__commandTemplate.substitute(params))
    
    def startCommand(self):
        """
        return start jboss command
        """
        params = dict(java_home=self.javaHome, jboss_home=self.jbossHome, server_config=self.serverConfig, run_as_user=self.runAsUser, command="start")
        return self.submitCommand(self.__commandTemplate.substitute(params))

    serverType = None
    runAsUser = property(get_run_as_user, None, None, None)
    javaHome = property(get_java_home, None, None, None)
    jbossHome = property(get_jboss_home, None, None, None)
    serverConfig = property(get_server_config, None, None, None)

        
class Webapp(Jboss):
    serverType = ServerType.APP

        
class Executor(Jboss):
    serverType = ServerType.EXE

    
class Scheduler(Jboss):
    serverType = ServerType.SCH
    
    
class Bo(Server):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__runAsUser = IfNull(kwargs.get("run_as_user"), "bonew")
        self.__boHome = IfNull(kwargs.get("bo_home"), "/opt/bo4/sap_bobj")

    def get_run_as_user(self):
        return self.__runAsUser

    def get_bo_home(self):
        return self.__boHome
    
    def stopCommand(self):
        """
        return bo stop command
        """
        commandLine = "su - %s -c \"%s/stopservers \" " % (self.runAsUser, self.boHome)
        return self.submitCommand(commandLine)
        
    def startCommand(self):
        """
        return bo start command
        """
        commandLine = "su - %s -c \"%s/startservers \" " % (self.runAsUser, self.boHome)
        return self.submitCommand(commandLine)
    
    runAsUser = property(get_run_as_user, None, None, None)
    boHome = property(get_bo_home, None, None, None)
    
