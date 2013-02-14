Qserv Quick start guide :
-------------------------

Pre-requisites :
----------------

By default, Qserv install script rebuild MySQL. But MySQL configuration file (/etc/my.cnf on RedHat-like distributions or /etc/mysql/my.cnf on Debian-like distributions) can produce conflict with MySQL provided with Qserv package. So, you should backup and then remove this file.

WARNING : Qserv must be installed and executed with a non-root account, so
create this account this  next command first :

  # su -c "useradd qserv"

Dependencies :
--------------

Depending on your distribution, you can install dependencies, as root, with next commands :

  # su -c "./admin/bootstrap/qserv-install-deps-sl6.sh"
  # su -c "./admin/bootstrap/qserv-install-deps-ubuntu.sh"

Installing Qserv :
------------------

  Configure the build :
  ---------------------

WARNING : next commands have to be executed with qserv account.

Assuming you've downloaded qserv in /home/qserv/src/qserv/

First do :
 
  $ cd /home/qserv/src/qserv/ 
  $ cp qserv-build.default.conf qserv-build.conf

and then set your install parameters in qserv-build.conf (restrictive access rights recommended as it contains MySQL password)
This file is well-commented.

then :
  $ mkdir ~/.lsst/
  $ ln -s /home/qserv/src/qserv/qserv-build.conf ~/.lsst/qserv.conf  
  $ ln -s /home/qserv/src/qserv/qserv-build.default.conf ~/.lsst/qserv.default.conf  

Then add next line to your ~/.bashrc (assuming you've setted base_dir to
/opt/qserv in qserv-build.conf):
  source /opt/qserv/qserv-env.sh 
it will provide you usefull aliases and update your PATH with qserv binaries path.

Then source your ~/.bashrc :
  $ source ~/.bashrc  

  Create main directories , download source dependencies , and run the full install :
  -----------------------------------------------------------------------------------

  $ scons install 
It may take a while ...
  
  Partition the PT1.1 data :
  --------------------------

Assuming PT1.1 data are in ${QSERV_DATA}/pt11/, next command will partition PT1.1 Object data :
  $ qserv-datamanager.py --config-dir=/home/qserv/src/qserv/ --mode=partition 

  Load the PT1.1 data and meta:
  -----------------------------

Assuming you've sourced qserv-env.sh, next command will launch Qserv :
  $ qserv-start

Load PT1.1 Object data :
  $ qserv-datamanager.py --config-dir /home/qserv/src/qserv/

Generate and load PT1.1 Object meta :
  $ qserv-datamanager.py --config-dir /home/qserv/src/qserv/ -m fill-table-meta -n 4

  Launch Qserv and run a small test :
  -----------------------------------

If not already done :
  $ qserv-start

Then connect to MySQL proxy (assuming mysql-proxy-port was setted to 4040 in
qserv-build.conf):
  $  mysql --host 127.0.0.1 --port 4040 --user 'qsmaster' LSST 

and launch next queries :
  > select count(*) from Object;
  > select * from Object where ObjectId=402395485975435;

It should success.

Official documentation : 
------------------------
It is located in ./admin/Install.txt

