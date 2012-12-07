import os, sys, io
import logging
import time
from datetime import datetime
import ConfigParser
import urllib2


def read_config(build_parser_file, default_parser):
    logger = logging.getLogger('scons-qserv')
    logger.debug("Reading build config file : %s" % build_parser_file)
    parser = ConfigParser.SafeConfigParser()
    parser.readfp(io.BytesIO(default_parser))
    parser.read(build_parser_file)

    logger.debug("Build configuration : ")
    for section in parser.sections():
       logger.debug("[%s]" % section)
       for option in parser.options(section):
        logger.debug("'%s' = '%s'" % (option, parser.get(section,option)))

    config = dict()
    config['base_dir']          = parser.get("qserv","base_dir")
    config['log_dir']           = parser.get("qserv","log_dir")
    config['geometry_src_dir']  = parser.get("qserv","geometry_src_dir")
    config['node_type']         = parser.get("qserv","node_type")
    config['mysqld_port']       = parser.get("mysqld","port")
    config['mysqld_pass']       = parser.get("mysqld","pass",raw=True)
    config['mysqld_data_dir']   = parser.get("mysqld","data_dir")
    config['mysqld_proxy_port'] = parser.get("mysql-proxy","port") 
    config['lsst_data_dir']     = parser.get("lsst","data_dir")
    config['cmsd_manager_port'] = parser.get("xrootd","cmsd_manager_port")
    config['xrootd_port']       = parser.get("xrootd","xrootd_port")

    config['dependencies']=dict()
    for option in parser.options("dependencies"):
        config['dependencies'][option] = parser.get("dependencies",option)

    return config 

def is_readable_dir(dir):
    """
    Test is a directory is readable.
    Return a couple (success,message), where success is a boolean and message a string
    """
    try:
        os.listdir(dir)
    except BaseException as e:
        return (False,"No read access : %s" % (e));
    return (True,"")

def is_writeable_dir(dir):
    """
    Test is a directory exists, if no try to create it, if yes test if it is writeable.
    Return a couple (success,message), where success is a boolean and message a string
    """
    try:
	if (os.path.exists(dir)):
            filename="%s/test.check" % dir
            f = open(filename,'w')
            f.close()
            os.remove(filename)
        else:
            Execute(Mkdir(dir))
    except IOError as e:
        if (e.errno==errno.ENOENT) :
            return (False,"No write access to directory : %s" % (dir));
    except BaseException as e:
        return (False,"No write access : %s" % (e));
    return (True,"")



def download_action(target, source, env):
    
    logger = logging.getLogger('scons-qserv')

    logger.debug("Target %s :" % target[0])
    logger.debug("Source %s :" % source[0])

    url = str(source[0])
    file_name = str(target[0])
    logger.debug("Opening %s :" % url)
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    logger.info("Downloading: %s Bytes: %s" % (file_name, file_size))

    file_size_dl = 0
    block_sz = 64 * 256 
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print(status)

    f.close()


def build_cmd_with_opts(config):

    install_opts="--install-dir=\"%s\"" % config['base_dir']
    install_opts="%s --log-dir=\"%s\"" % (install_opts, config['log_dir'])
    install_opts="%s --mysql-data-dir=\"%s\"" % (install_opts, config['mysqld_data_dir'])
    install_opts="%s --mysql-port=%s" % (install_opts, config['mysqld_port'])
    install_opts="%s --mysql-proxy-port=%s" % (install_opts,config['mysqld_proxy_port'])
    install_opts="%s --mysql-pass=\"%s\"" % (install_opts,config['mysqld_pass'])
    install_opts="%s --cmsd-manager-port=%s" % (install_opts,config['cmsd_manager_port'])
    install_opts="%s --xrootd-port=%s" % (install_opts,config['xrootd_port'])
    
    if len(config['geometry_src_dir'])!=0 :
        install_opts="%s --geometry-dir=\"%s\"" % (install_opts,config['geometry_src_dir'])
    
    if config['node-type']=='mono' :
        install_opts="%s --mono-node" % install_opts
    elif config['node-type']=='master' :
        None
    elif config['node-type']=='worker' :
        None

    log_file_prefix = config['log_dir']
    if config['qserv-only']==True :
        install_opts="%s --qserv" % install_opts
        log_file_prefix += "/QSERV-ONLY"
    elif config['qserv-clean']==True :
        install_opts="%s --clean-all" % install_opts
        log_file_prefix = "~/QSERV-CLEAN"
    elif config['init-mysql-db']==True :
        install_opts="%s --init-mysql-db" % install_opts
        log_file_prefix += "/QSERV-INIT-MYSQL-DB"
    else :
        log_file_prefix += "/INSTALL"
    log_file_name = log_file_prefix + "-" + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + ".log" 

    command_str = config['src_dir' ] + "/admin/qserv-install " + install_opts + " > " + log_file_name + " 2&>1"
    
    return command_str

