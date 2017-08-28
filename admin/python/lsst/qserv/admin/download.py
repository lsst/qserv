
from __future__ import absolute_import, division, print_function

from future import standard_library
standard_library.install_aliases()
from builtins import chr
import os
import logging
from datetime import datetime
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def download(file_name, url_str):

    logger = logging.getLogger()

    logger.debug("Target %r :" % file_name)
    logger.debug("Source %r :" % url_str)

    url = Request(url_str)

    file_size_dl = -2
    file_size = -1

    success = True

    try:

        logger.debug("Opening %r :" % url_str)
        u = urlopen(url_str)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        logger.info("Downloading: %r Bytes: %r" % (file_name, file_size))

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
            sys.stdout.write(status)
            sys.stdout.flush()
            sys.stdout.write('\r')
            sys.stdout.flush()

        f.close()

    # handle errors
    except HTTPError as e:
        logger.fatal("HTTP Error: %r %r" % (e, url_str))
        success = False
    except URLError as e:
        logger.fatal("URL Error: %r %r" % (e, url_str))
        success = False

    if file_size_dl != file_size:
        logger.fatal("Download of file %r failed" % url_str)
        success = False

    if not success:
        sys.exit(1)
