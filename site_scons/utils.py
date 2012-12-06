import os, sys, io
import logging
import time
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
    config['base_dir']        = parser.get("qserv","base_dir")
    config['log_dir']         = parser.get("qserv","log_dir")
    config['mysqld_data_dir'] = parser.get("mysqld","data_dir")
    config['lsst_data_dir']   = parser.get("lsst","data_dir")

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
