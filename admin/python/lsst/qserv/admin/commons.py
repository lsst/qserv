import io
import os
import logging
import re
import subprocess
import sys
import ConfigParser

def read_user_config():
    config_file=os.path.join(os.getenv("HOME"),".lsst","qserv.conf")
    default_config_file=os.path.join(os.getenv("HOME"),".lsst","qserv.default.conf")
    config = read_config(config_file, default_config_file)
    return config

def read_config(config_file, default_config_file):

    logger = logging.getLogger()
    logger.debug("Reading build config file : %s" % config_file)

    if not os.path.isfile(config_file):
        logger.fatal("qserv configuration file not found : %s" % config_file)
        exit(1)
    elif not os.path.isfile(default_config_file):
        logger.fatal("qserv configuration file with default values not found : %s" % default_config_file)
        exit(1)

    parser = ConfigParser.SafeConfigParser()
    parser.read(default_config_file)
    parser.read(config_file)

    logger.debug("Build configuration : ")
    for section in parser.sections():
       logger.debug("===")
       logger.debug("[%s]" % section)
       logger.debug("===")
       for option in parser.options(section):
        logger.debug("'%s' = '%s'" % (option, parser.get(section,option)))

    config = dict()
    section='qserv'
    config[section] = dict()
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)
    # computable configuration parameters
    for dir in ['base_dir', 'tmp_dir', 'log_dir']:
        config['qserv'][dir] = os.path.normpath(config['qserv'][dir])
    config['qserv']['bin_dir'] = os.path.join(config['qserv']['base_dir'], "bin")

    section='mysqld'
    config[section] = dict()
    options = [option for option in parser.options(section) if option not in ['pass','port'] ]
    for option in options:
        config[section][option] = parser.get(section,option)

    # TODO : manage special characters for pass (see config file comments for additional information)
    config['mysqld']['pass']    = parser.get("mysqld","pass",raw=True)
    config['mysqld']['port'] = parser.getint('mysqld','port')
    # computable configuration parameter
    config['mysqld']['sock']    = os.path.join(config['qserv']['base_dir'], "var","lib","mysql","mysql.sock")

    section='mysql_proxy'
    config[section] = dict()
    options = [option for option in parser.options(section) if option != 'port']
    for option in parser.options(section):
        config[section][option] = parser.get(section,option)

    config['mysql_proxy']['port'] = parser.getint('mysql_proxy','port')

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

    # normalize directories names
    # TODO duplicate normpath() call
    for section in config.keys():
        for option in config[section].keys():
            if re.match(".*_dir",option):
                config[section][option] = os.path.normpath(config[section][option])

    config['bin'] = dict()
    config['bin']['mysql'] = os.path.join(config['qserv']['bin_dir'],'mysql')
    config['bin']['python'] = os.path.join(config['qserv']['bin_dir'],'python')

    return config

def is_readable(dir):
    """
    Test is a dir is readable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')

    logger.debug("Checking read access for : %s", dir)
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

def init_default_logger(log_file_prefix, level=logging.DEBUG, log_path="."):
    console_logger(level)
    logger = file_logger(log_file_prefix, level, log_path)
    return logger

def console_logger(level=logging.DEBUG):
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def file_logger(log_file_prefix, level=logging.DEBUG, log_path="."):

    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # this level can be reduce for each handler
    logger.setLevel(level)

    file_handler = logging.FileHandler(os.path.join(log_path+os.sep,log_file_prefix+'.log'))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def run_command(cmd_args, stdin_file=None, stdout_file=None, stderr_file=None) :
    """ Run a shell command

    Keyword arguments
    cmd_args -- a list of arguments
    logger_name -- the name of a logger, if not specified, will log to stdout

    Return a string containing stdout and stderr
    """
    logger = logging.getLogger()

    cmd_str= " ".join(cmd_args)
    logger.info("Running :\n---\n\t%s\n---" % cmd_str)

    sin = None
    if stdin_file != None:
        logger.debug("stdin file : %s" % stdout_file)
        sin=open(stdin_file,"r")

    sout = None
    if stdout_file != None:
        logger.debug("stdout file : %s" % stdout_file)
        sout=open(stdout_file,"w")
    else:
        sout=subprocess.PIPE

    serr = None
    if stderr_file != None:
        logger.debug("stderr file : %s" % stderr_file)
        serr=open(stderr_file,"w")
    else:
        serr=subprocess.PIPE

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
            cmd_args, stdin=sin, stdout=sout, stderr=serr
        )

        (stdoutdata, stderrdata) = process.communicate()

        if stdoutdata != None and len(stdoutdata)>0:
            logger.info("Stdout : %s " % stdoutdata)
        if stderrdata != None and len(stderrdata)>0:
            logger.info("Stderr : %s " % stderrdata)

        if process.returncode!=0 :
            logger.fatal("Error code returned by command : %s " % cmd_str)
            sys.exit(1)

    except OSError as e:
        logger.fatal("Error : %s while running command : %s" %
                     (e,cmd_str))
        sys.exit(1)
    except ValueError as e:
        logger.fatal("Invalid parameter : '%s' for command : %s " % (e,cmd_str))
        sys.exit(1)



def run_backgroundCommand(cmd_args, stdin_file=None, stdout_file=None, stderr_file=None, logger_name=None):

    logger = logging.getLogger(logger_name)

    cmd_str= " ".join(cmd_args)
    logger.info("Running :\n---\n\t%s\n---" % cmd_str)

    sin = None
    if stdin_file != None:
        logger.debug("stdin file : %s" % stdout_file)
        sin=open(stdin_file,"r")

    sout = None
    if stdout_file != None:
        logger.debug("stdout file : %s" % stdout_file)
        sout=open(stdout_file,"w")
    else:
        sout=subprocess.PIPE

    serr = None
    if stderr_file != None:
        logger.debug("stderr file : %s" % stderr_file)
        serr=open(stderr_file,"w")
    else:
        serr=subprocess.PIPE

    try :
        pid = subprocess.Popen( cmd_args, stdin=sin, stdout=sout, stderr=serr ).pid

    except OSError as e:
        logger.fatal("Error : %s while running command : %s" %
                     (e,cmd_str))
        sys.exit(1)
    except ValueError as e:
        logger.fatal("Invalid parameter : '%s' for command : %s " % (e,cmd_str))
        sys.exit(1)

