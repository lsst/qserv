Qserv Quick start guide :
-------------------------

WARNING : 
    By default, Qserv install script rebuild MySQL. But MySQL configuration file 
    (/etc/my.cnf on RedHat-like distributions or /etc/mysql/my.cnf on Debian-like distributions) 
    can produce conflict with MySQL provided with Qserv package. So, you should backup and then remove this file.

WARNING : 
    Qserv must be installed and executed with a non-root account, so
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

WARNING : 
    Next commands have to be executed with qserv account.

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

Launch Qserv :
--------------

If not already done :
  $ qserv-start

Launch functionnal tests :
--------------------------

  $ qserv-testdata.sh

This command will load tree small dataset on a mono-node installation, and on a
mysql server and compare results of some meaningfull queries.
More documentation can be found in tests/README.txt

Launch unit tests :
-------------------

  $ qserv-testunit.sh

Code coverage of unit tests still need to be improved.

Cleaning and uninstalling Qserv :
---------------------------------

  $ qserv-stop
  $ scons -c
  $ scons perl-clean-all

Official documentation : 
------------------------
It is located in ./admin/Install.txt

