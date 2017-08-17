
from __future__ import absolute_import, division, print_function

import logging
import os


def is_readable(dir):
    """
    Test is a dir is readable.
    Return a boolean
    """
    logger = logging.getLogger()

    logger.debug("Checking read access for : %r", dir)
    try:
        os.listdir(dir)
        return True
    except Exception as e:
        logger.debug("No read access to dir %r : %r" % (dir, e))
        return False


def is_writable(dir):
    """
    Test if a dir is writeable.
    Return a boolean
    """
    logger = logging.getLogger()
    try:
        tmp_prefix = "write_tester"
        count = 0
        filename = os.path.join(dir, tmp_prefix)
        while(os.path.exists(filename)):
            filename = "{}.{}".format(os.path.join(dir, tmp_prefix), count)
            count = count + 1
        f = open(filename, "w")
        f.close()
        os.remove(filename)
        return True
    except Exception as e:
        logger.info("No write access to dir %r : %r" % (dir, e))
        return False
