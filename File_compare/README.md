# Pre Configuration

## Install python3
	sudo yum install python3

## Install pip3
	sudo yum install python3-pip

## Install Oracle python lib
	pip3 install cx_Oracle --upgrade
    
To configure pip to use this new mirror, edit /etc/pip.conf as follows:

    [global]
    timeout = 6000
    index-url = https://mirrors.aliyun.com/pypi/simple/
    trusted-host = mirrors.aliyun.com

    
## Install Oracle Instant Client
1.Download an Oracle 19, 18, 12, or 11.2 “Basic” or “Basic Light” zip file: 64-bit or 32-bit, matching your Python architecture.

2.Unzip the package into a single directory that is accessible to your application. For example:

	mkdir -p /opt/oracle
	cd /opt/oracle
	unzip instantclient-basic-linux.x64-19.3.0.0.0dbru.zip

3.Install the libaio package with sudo or as the root user. For example:

	sudo yum install libaio

On some Linux distributions this package is called _libaio1_ instead.

4.Add Instant Client to the runtime link path. For example, with sudo or as the root user:

	sudo sh -c "echo /opt/oracle/instantclient_19_3 > /etc/ld.so.conf.d/oracle-instantclient.conf"
	sudo ldconfig

5.Set enviroment variable 'ORACLE\_HOME'

Many Oracle database applications look for Oracle software in the location specified in the environment variable 'ORACLE\_HOME'.
Typical workstations will only have one Oracle install, and will want to define this variable in a system-wide location. 

	sudo sudo vi /etc/profile.d/oracle.sh && sudo chmod o+r /etc/profile.d/oracle.sh 
	
For example, add the following:

	export ORACLE_HOME=/opt/oracle/instantclient_19_3	

Alternatively, each user can define this in their ~/.bash_profile

## Add Oracle Net Service Name
Then _tnsnames.ora_ file is a configuration file that contains network service names mapped to connect descriptors for the local naming method, or net service names mapped to listener protocol addresses. By default, the _tnsnames.ora_ file is located in the ORACLE\_HOME/network/admin directory. Oracle Net will check this directory for the configuration file. For example, given a _tnsnames.ora_ file with the following conents:

	isis2db =
	  (DESCRIPTION =
	    (ADDRESS = (PROTOCOL = TCP)(HOST = dbhost.example.com)(PORT = 1521))
	    (CONNECT_DATA =
	    (SERVER = DEDICATED)
	    (SERVICE_NAME = isis2db)
	  )
	)


# Adjusting configuration file 'compare.ini'
## section 'database' 
### options

*   user   
`the nbs_common schema name`
*   password   
`the password of nbs_common schema`
*   tnsname   
`the Oracle Net Service Name in tnsnames.ora`

## section 'ibsp'
### options

*   outbound_online_root    
`the root directory of the IBSPs output files`
*   outbound_baseline_root   
`the root directory of the IBSPs baseline output files`
   

## section 'CSI'
### options

*   split_jar    
`the file path of the CSI splitter`
*   diff_jar   
`the file path of the CSI different checker`

## section 'HOT'
### options

*   split_jar    
`the file path of the HOT splitter`
*   diff_jar   
`the file path of the HOT different checker`


For example, given a _compare.ini_ file with the following contents:


	[database]
	user=nbs_common
	password=nbs_common
	tnsname=isis2db
	
	[ibsp]
	outbound_online_root=/ibspdata/APP/Online/Outbound
	outbound_baseline_root=/ibspdata/Autotest/output_files/V573OutputFile
	
	[CSI]
	split_jar=/ibspdata/Autotest/csi/jar/fc-split-csi-1-jar-with-dependencies.jar
	diff_jar=/ibspdata/Autotest/csi/jar/fc-diff-csi-1-jar-with-dependencies.jar
	
	[HOT]
	split-jar=/ibspdata/Autotest/hot/jar/fc-split-hot-1-jar-with-dependencies.jar
	diff_jar=/ibspdata/Autotest/hot/jar/fc-diff-hot-1-jar-with-dependencies.jar
	
	
# Run compare tool
## Usage

	pythone3 main.py [-h] -t TYPE -b BSP -d DATE

To run the command, you need to provide the following parameters 
	
*   TYPE   
`the file type of the output files, it can be 'HOT' or 'CSI'`
*   BSP   
`the code of Pseudo BSP`
*	DATE   
`the create date of the output files, it needs to be in yyyy-mm-dd format`


For example

	python3 main.py -t HOT -b SE -d 2017-12-03
	
After running, the log file 'compare.log' can be found in the current directory

# Install compare tool into python library

    rm -rf build
    rm -rf dist
    python3 setup.py install --user --record install.txt

