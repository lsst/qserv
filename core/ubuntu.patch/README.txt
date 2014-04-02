We found light patching of scons files is sufficient to make
qserv build and run on Ubuntu 12.04. To apply the patches, 
run once the script ubuntu.patch/applyIt.sh from the core 
directory.

If you run into problems with antlr, try using runantlr:
cd /usr/bin; sudo ln -s runantlr antlr
(version 2.7.7 is required)
also, install libantlr-dev
