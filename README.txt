Qserv Quick start guide :
-------------------------

Pre-requisites :
----------------

By default, Qserv install script rebuild MySQL. But MySQL configuration file (/etc/my.cnf on RedHat-like distributions or /etc/mysql/my.cnf on Debian-like distributions) can produce conflict with MySQL provided with Qserv package. So, you should backup and then remove this file.

Dependencies :
--------------

Depending on your distribution, you can install dependencies, as root, with next commands :

  # admin/qserv-install-deps-sl6.sh
  # admin/qserv-install-deps-ubuntu.sh

Installing Qserv :
------------------

  Configure the build :
  ---------------------

First do :
 
  $ cd /path_to_qserv_src/ 
  $ cp qserv-build.example.sh qserv-build.conf

and then set your install parameters in : 
- qserv-build.conf (restrictive rights recommended as it contains MySQL
  password)

You could add next line to your ~/.bashrc :
  source /path_to_qserv_base_dir/qserv-env.sh 
it will provide you usefull aliases and update your PATH with qserv binaries path.
  
  Create main directories ,
  -------------------------
  Download source main package and dependencies ,
  -----------------------------------------------
  Run the full install :
  ----------------------

  $ scons install 
It may take a while ...
  
  Partition the PT1.1 data :
  --------------------------
TODO
#Assuming PT1.1 data are in ${QSERV_DATA}/pt11/, next command will partition PT1.1 Object data :
#  $ ./admin/qserv-partition-data-pt11.sh

  Load the pt1.1 data and meta:
  -----------------------------

Assuming you've sourced qserv-env.sh, next command will launch Qserv :
  $ qserv-start

  $ ./admin/bin/qservdatamanager.py --config-dir /path-to-dir-containing-qserv-build.conf/ 
will load PT1.1 Object data.

and
  $ ./admin/bin/qservdatamanager.py --config-dir /path-to-dir-containing-qserv-build.conf/ -m fill-table-meta -n 4

  Launch Qserv and run a small test :
  -----------------------------------

If not already done :
  $ qserv-start
Then connect to MySQL proxy (assuming mysql-proxy-port was setted to 4040):
  $  mysql --host 127.0.0.1 --port 4040 --user 'qsmaster' LSST 
and launch next queries :
  > select count(*) from Object;
  > select * from Object where ObjectId=402395485975435;
It should success.

Official documentation : 
------------------------
It is located in ${QSERV_SRC}/admin/Install.txt

