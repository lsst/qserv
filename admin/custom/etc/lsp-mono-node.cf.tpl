# Unified configuration for xrootd/cmsd for both manager and server instances
# "if"-block separates manager-only and server-only configuration.

# if <regexp> block.  Test whether the hostname matches the <regexp>, 
# Example: tuson121 is the hostname of our manager node within our 
# allocation at LLNL.

###################################################################################
# WARNING : mono-node installation, uncomment manager section to enable multi-node.
###################################################################################

############
# if manager
############
#if <XROOTD_MANAGER_HOST>*
# Use manager mode
#all.role manager

# Use standard filesystem plugin
# Newer xrootd uses embedded plugin by default, uncomment for older xrootd
# xrootd.fslib libXrdOfs.so

##########################
# else: non-manager nodes
##########################
#else

# Use server mode
all.role server

# Use qserv worker filesystem plugin
xrootd.fslib libqserv_worker.so

# Set <pathname> for file location resolution.  
# i.e., <pathname>/somefile will be exported in the xroot URL: 
# xroot://manager:0000/somefile
oss.localroot /data/lsst/lspexport

# Do not change.  This directive tries to lower the minimum free disk space
# for "file" writes (which are subquery writes in our case).
cms.space linger 0 recalc 15 min 10m 11m

# Use asyncronous filesystem API when interfacing with filesystem plugin 
# (qserv-worker plugin is tested using this configuration)
xrootd.async force
#fi

########################################
# Shared directives (manager and server)
########################################

# Writable paths for administration
# cmsd and xrootd paths for pid
all.pidpath <XROOTD_PID_DIR> 
# path to write logging and other information
all.adminpath <XROOTD_ADMIN_DIR>

# host:port of manager instance (2131 is default)
# all.manager <XROOTD_MANAGER_HOST>:<CMSD_MANAGER_PORT>

# Do not change. This specifies valid virtual paths that can be accessed.  
# "nolock" directive prevents write-locking and is important for qserv
# qserv is hardcoded for these paths. 
all.export /q/ nolock
all.export /result/ nolock

xrd.port <XROOTD_PORT>

# Optional: Prevent dns resolution in logs.  
# This may speed up request processing.
xrd.network nodnr
 
