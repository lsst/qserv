import logging
from datetime import datetime
import urllib2

def download(target, source, env):
    
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


def build_cmd_with_opts(config, target='install'):

    logger = logging.getLogger('scons-qserv')

    install_opts="--install-dir=\"%s\"" % config['base_dir']
    install_opts="%s --log-dir=\"%s\"" % (install_opts, config['log_dir'])
    install_opts="%s --mysql-data-dir=\"%s\"" % (install_opts, config['mysqld_data_dir'])
    install_opts="%s --mysql-port=%s" % (install_opts, config['mysqld_port'])
    install_opts="%s --mysql-proxy-port=%s" % (install_opts,config['mysqld_proxy_port'])
    install_opts="%s --mysql-pass=\"%s\"" % (install_opts,config['mysqld_pass'])
    install_opts="%s --cmsd-manager-port=%s" % (install_opts,config['cmsd_manager_port'])
    install_opts="%s --xrootd-port=%s" % (install_opts,config['xrootd_port'])
    
    if config.has_key('geometry_src_dir') : 
        if commons.is_readable(config['geometry_src_dir']) :
            install_opts="%s --geometry-dir=\"%s\"" % (install_opts,config['geometry_src_dir'])
        else :
            logger.fatal("Error while accessing geometry src dir : '%s' for reading." % config['geometry_src_dir'])
            exit(1)
    
    if config['node_type']=='mono' :
        install_opts="%s --mono-node" % install_opts
    elif config['node_type']=='master' :
        None
    elif config['node_type']=='worker' :
        None

    log_file_prefix = config['log_dir']
    if target=='qserv-only' :
        install_opts="%s --qserv" % install_opts
        log_file_prefix += "/QSERV-ONLY"
    elif target == 'clean-all' :
        install_opts="%s --clean-all" % install_opts
        log_file_prefix = "~/QSERV-CLEAN"
    elif target == 'init-mysql-db' :
        install_opts="%s --init-mysql-db" % install_opts
        log_file_prefix += "/QSERV-INIT-MYSQL-DB"
    else :
        log_file_prefix += "/INSTALL"
    log_file_name = log_file_prefix + "-" + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + ".log" 

    command_str = config['src_dir' ] + "/admin/qserv-install " 
    command_str += install_opts + " &> " + log_file_name
    
    #logger.debug("Launching perl install script with next command : %s" % command_str)
    return command_str

