import io
import os
import logging
import subprocess 
import sys 
import ConfigParser

def read_config(config_file, default_config_file):
    logger = logging.getLogger()
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
    section='qserv'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)
    config['qserv']['bin_dir']    = os.path.join(config['qserv']['base_dir'], "bin")
    
    section='mysqld'
    config[section] = dict()
    options = [option for option in parser.options(section) if option != 'pass']
    for option in options:
        config[section][option] = parser.get(section,option)
    # TODO : manage special characters (see config file comments for additional information)
    config['mysqld']['pass']       = parser.get("mysqld","pass",raw=True)

    section='mysql_proxy'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)
    
    section='lsst'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)
    
    section='xrootd'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)
    
    section='dependencies'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)

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


def init_default_logger(log_file_prefix, logger_name = '', level=logging.DEBUG, log_path=".") :

    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # this level can be reduce for each handler
    logger.setLevel(level)
 
    file_handler = logging.FileHandler(os.path.join(log_path+os.sep,log_file_prefix+'.log'))
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

    cmd_str= " ".join(cmd_args)

    log_str="Running next command from python : '%s'" % cmd_args
    if logger_name != None :
    	logger = logging.getLogger(logger_name)
        logger.info(log_str)
    else :
        print(log_str)

# TODO : use this with python 2.7 :
#  try :
#        out = subprocess.check_output(
#                cmd_args, 
#                stderr=subprocess.STDOUT
#              )
#
#    except subprocess.CalledProcessError as e:
#        logger.fatal("Error : '%s' %s  while running command : '%s'" %
#            (e,out,cmd_str)
#        )
#        sys.exit(1)


    try :
        process = subprocess.Popen(
            cmd_args, stdin=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        (stdoutdata, stderrdata) = process.communicate()

    except OSError as e:
        logger.fatal("Error : '%s' while running command : '%s'" %
            (e,cmd_str))
        sys.exit(1)
    except ValueError as e:
        logger.fatal("Invalid parameter : '%s' for command : %s " % (e,cmd_str))
        sys.exit(1)

    return stdoutdata


