import io
import os
import hashlib
import getpass 
import logging
import re
import subprocess
import sys
import ConfigParser
import const

config = dict()

def read_user_config():
    config_file=os.path.join(os.getenv("HOME"),".lsst","qserv.conf")
    config = read_config(config_file)
    return config

def read_config(config_file):

    global config
    logger = logging.getLogger()
    logger.debug("Reading config file : %s" % config_file)

    if not os.path.isfile(config_file):
        logger.fatal("qserv configuration file not found : %s" % config_file)
        exit(1)

    parser = ConfigParser.SafeConfigParser()
    #parser.readfp(io.BytesIO(const.DEFAULT_CONFIG))
    parser.read(config_file)

    logger.debug("Build configuration : ")
    for section in parser.sections():
       logger.debug("===")
       logger.debug("[%s]" % section)
       logger.debug("===")
       config[section] = dict() 
       for option in parser.options(section):
        logger.debug("'%s' = '%s'" % (option, parser.get(section,option)))
        config[section][option] = parser.get(section,option)

    # normalize directories names
    for section in config.keys():
        for option in config[section].keys():
            if re.match(".*_dir",option):
                config[section][option] = os.path.normpath(config[section][option])

    # computable configuration parameters
    config['qserv']['scratch_dir'] = os.path.join( "/dev", "shm", "qserv-%s-%s" %
                                        (getpass.getuser(),
                                        hashlib.sha224(config['qserv']['run_base_dir']).hexdigest())
                                    )

    # TODO : manage special characters for pass (see config file comments for additional information)
    config['mysqld']['pass'] = parser.get("mysqld","pass",raw=True)
    if parser.has_option('mysqld','port'):
        config['mysqld']['port'] = parser.getint('mysqld','port')

    config['mysql_proxy']['port'] = parser.getint('mysql_proxy','port')

    return config

def getConfig():
    return config

def restart(service_name):

        config = getConfig()
        if len(config)==0 :
            raise RuntimeError("Qserv configuration is empty")
        initd_path = os.path.join(config['qserv']['run_base_dir'],'etc','init.d')
        daemon_script = os.path.join(initd_path,service_name)
        out = os.system("%s stop" % daemon_script)
        out = os.system("%s start" % daemon_script)


def run_command(cmd_args, stdin_file=None, stdout_file=None, stderr_file=None, loglevel=logging.INFO) :
    """
    Run a shell command

    Return a string containing stdout and stderr
    """
    logger = logging.getLogger()

    cmd_str= ' '.join(cmd_args)
    logger.log(loglevel, "Running : {0}".format(cmd_str))

    sin = None
    if stdin_file != None:
        logger.debug("stdin file : %s" % stdin_file)
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
        process = subprocess.Popen(
            cmd_args, stdin=sin, stdout=sout, stderr=serr
        )

        (stdoutdata, stderrdata) = process.communicate()

        if stdoutdata != None and len(stdoutdata)>0:
            logger.info("\tstdout :\n--\n%s--" % stdoutdata)
        if stderrdata != None and len(stderrdata)>0:
            logger.info("\tstderr :\n--\n%s--" % stderrdata)

        if process.returncode!=0 :
            logger.fatal("Error code returned by command : {0} ".format(cmd_str))
            sys.exit(1)

    except OSError as e:
        logger.fatal("Error : %s while running command : %s" %
                     (e,cmd_str))
        sys.exit(1)
    except ValueError as e:
        logger.fatal("Invalid parameter : '%s' for command : %s " % (e,cmd_str))
        sys.exit(1)

