import io
import os
import logging
import subprocess 
import ConfigParser

def read_config(config_file, default_config_file):
    logger = logging.getLogger('scons-qserv')
    logger.debug("Reading build config file : %s" % config_file)
    parser = ConfigParser.SafeConfigParser()
    parser.read(default_config_file)
    parser.read(config_file)

    logger.debug("Build configuration : ")
    for section in parser.sections():
       logger.debug("[%s]" % section)
       for option in parser.options(section):
        logger.debug("'%s' = '%s'" % (option, parser.get(section,option)))

    config = dict()
    config['base_dir']          = parser.get("qserv","base_dir")
    config['bin_dir']           = "{base_dir}{sep}bin".format(base_dir=config['base_dir'], sep=os.sep)
    config['log_dir']           = parser.get("qserv","log_dir")
    if parser.has_option("qserv","geometry_src_dir"):
        config['geometry_src_dir']  = parser.get("qserv","geometry_src_dir")
    config['node_type']         = parser.get("qserv","node_type")
    config['mysqld_port']       = parser.get("mysqld","port")
    # TODO : manage special characters (see config file comments for additional information)
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

def is_readable(dir):
    """
    Test is a dir is readable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')

    logger.debug("Checking read access for : %s", dir)
    logger = logging.getLogger('scons-qserv')
    try:
        os.listdir(dir)
        return True 
    except Exception as e:
        logger.debug("No read access to dir %s : %s" % (dir,e))
        return False

def is_writable(dir):
    """
    Test if a dir is writeable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')
    try:
        tmp_prefix = "write_tester";
        count = 0
        filename = os.path.join(dir, tmp_prefix)
        while(os.path.exists(filename)):
            filename = "{}.{}".format(os.path.join(dir, tmp_prefix),count)
            count = count + 1
        f = open(filename,"w")
        f.close()
        os.remove(filename)
        return True
    except Exception as e:
        logger.info("No write access to dir %s : %s" % (dir,e))
        return False


def init_default_logger(logger_name, level=logging.DEBUG, log_path=".") :

    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # this level can be reduce for each handler
    logger.setLevel(level)
 
    file_handler = logging.FileHandler(log_path+os.sep+logger_name+'.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler) 
 
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def run_command(cmd_args, logger_name=None) :
    """ Run a shell command

    Keyword arguments
    cmd_args -- a list of arguments
    logger_name -- the name of a logger, if not specified, will log to stdout 

    Return a string containing stdout and stderr
    """
    log_str="Running next command from python : '%s' " % cmd_args
    if logger_name != None :
    	logger = logging.getLogger(logger_name)
        logger.info(log_str)
    else :
        print(log_str)
    process = subprocess.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    (stdoutdata, stderrdata) = process.communicate()
    return stdoutdata


