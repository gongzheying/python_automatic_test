# configuration file 'autotest.ini'
## section 'task' 
### options

* start_date  
`the begin date of testcase : [fmt] yyyy-mm-dd `
* end_date  
`the end date of testcase : [fmt] yyyy-mm-dd `
* bsp_codes  
`the bsp codes of testcase, multi bsps are separated by commas`
* input_src_dir  
`the input file directory of testcase`
* log_dir  
`the log directory of IBSPs`
* input_dir  
`the input channel directory of IBSPs`
* input_backup_dir  
`the input backup directory of IBSPs`
* output_dir  
`the output root directory of IBSPs`
* output_base_dir  
`the output baseline diectory of IBSPs`
* csi_split_jar  
`the csi split jar of filecompare tool`
* csi_diff_jar  
`the csi diff jar of filecompare tool`
* hot_split_jar  
`the hot split jar of filecompare tool`
* hot_diff_jar  
`the hot diff jar of filecompare tool`
* nbs_connstr  
`the nbs schema connect url of IBSPs : [fmt] user/pwd@tnsname `
* nbs_dumpfile  
`the nbs schema init dumpfile of IBSPs`
* nbs_common_connstr  
`the nbs_common schema connect url of IBSPs : [fmt] user/pwd@tnsname `
* nbs_ommon_dumpfile  
`the nbs_common schema init dumpfile of IBSPs`



## section 'server.auth'
### options

* user  
`the os account of jboss and bo`
* identity_file  
`the authentication identity file used to ssh connect jboss and bo`


   

## section 'server.webapp', 'server.executor', 'server.scheduler'
### options

* host  
`the ip or hostname of jboss, multi hosts are separated by commas`
* java_home  
`the java home directory`
* jboss_home  
`the jboss home directory`
* server_config
`the jboss config file`
* run_as_user
`the os account used to run jboss`




## section 'server.bo'
### options

* host  
`the ip or hostname of bo`
* bo_home  
`the bo home directory`
* run_as_user
`the os account used to run bo`



For example, given a _autotest.ini_ file with the following contents:


	[task]
	start_date=2017-12-02
	end_date=2017-12-05
	bsp_codes=SE,LV,LT,TP
	log_dir=/ibspdata/LOG
	input_dir=/ibspdata/SFTP/Channel/Inbound/Online/Inputs
	input_backup_dir=/ibspdata/SFTP/Channel/Inbound/Backup
	input_src_dir=/ibspdata/Autotest/input_src/v542Inputfile
	output_dir=/ibspdata/APP/Online/Outbound/
	output_base_dir=/ibspdata/Autotest/output_files/V573OutputFile
	csi_split_jar=/ibspdata/Autotest/csi/jar/fc-split-csi-1-jar-with-dependencies.jar
	csi_diff_jar=/ibspdata/Autotest/csi/jar/fc-diff-csi-1-jar-with-dependencies.jar
	hot_split_jar=/ibspdata/Autotest/hot/jar/fc-split-hot-1-jar-with-dependencies.jar
	hot_diff_jar=/ibspdata/Autotest/hot/jar/fc-diff-hot-1-jar-with-dependencies.jar
	nbs_connstr=nbs/nbs@isis2db
	nbs_dumpfile=/opt/ora/dump/nbs_test.dmp
	nbs_common_connstr=nbs_common/nbs_common@isis2db
	nbs_ommon_dumpfile=/opt/ora/dump/nbs_common_test.dmp
	
	
	
	[server.auth]
	user=ec2-user
	identity_file=~/.ssh/nonprod-nfe-key-pair.pem
	
	
	[server.webapp]
	host=10.188.21.142
	java_home=/opt/jdk1.8.0_191
	jboss_home=/opt/jboss-eap-7.1
	server_config=standalone.xml
	run_as_user=jboss
	
	
	[server.executor]
	host=10.188.21.144,10.188.21.145
	java_home=/opt/jdk1.8.0_191
	jboss_home=/opt/jboss-eap-7.1
	server_config=standalone.xml
	run_as_user=jboss
	
	[server.scheduler]
	host=10.188.21.143
	java_home=/opt/jdk1.8.0_191
	jboss_home=/opt/jboss-eap-7.1
	server_config=standalone-full.xml
	run_as_user=jboss
	
	
	[server.bo]
	host=10.188.21.146
	bo_home=/opt/bo4/sap_bobj
	run_as_user=bonew
	
	
	
	
