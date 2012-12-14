import os
import logging
import time
import commons 

from SCons.Script import Execute, Mkdir   # for Execute and Mkdir

def exists_and_is_writable(dir) :
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')
    logger.debug("Checking existence and write access for : %s", dir)
    if not os.path.exists(dir):
	try:
            Execute(Mkdir(dir))
        except Exception as e:
            logger.info("Unable to create dir : %s : %s" % (dir,e))
            return False 
    elif not commons.is_writable(dir):
        return False
    
    return True	 

