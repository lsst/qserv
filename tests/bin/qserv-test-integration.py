#!/usr/bin/env python
from lsst.qserv.admin import commons
from lsst.qserv.admin import logger 
from lsst.qserv.tests.testdataset import TestDataSet, suite

import argparse 
import logging
import os
import unittest

def parseArgs():


    parser = argparse.ArgumentParser(
            description='''Qserv integration tests tool. Please specify an input data directory using, by precedence, 
-r option, QSERV_TESTDATA_DIR environment variable or by setting testdata_dir value in ~/.lsst/qserv.conf''',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )
 
    # Logging management
    verbose_dict = {
        'DEBUG'     : logging.DEBUG,
        'INFO'      : logging.INFO,
        'WARNING'   : logging.WARNING,
        'ERROR'     : logging.ERROR,
        'FATAL'   : logging.FATAL,
    }
    verbose_arg_values = verbose_dict.keys() 
    parser.add_argument("-v", "--verbose-level", dest="verbose_str", choices=verbose_arg_values,
        default='INFO',
        help="verbosity level"
        )

    # run dir, all mutable data related to a qserv running instance are
    # located here
    parser.add_argument("-t", "--testdata-dir", dest="testdata_dir",
            default=os.environ.get('QSERV_TESTDATA_DIR'),
            help="full path to directory containing test datasets default to $QSERV_TESTDATA_DIR"
            )

    args = parser.parse_args()

    args.verbose_level = verbose_dict[args.verbose_str]

    return args

if __name__ == '__main__':
    args = parseArgs()
    config = commons.read_user_config()
    logger.init_default_logger("qserv-test-integration", args.verbose_level, config['qserv']['log_dir'])
    log = logging.getLogger()
    if args.testdata_dir is not None:
	log.debug("Overriding ~/.lsst/qserv.conf testdata_dir value with {0}".format(args.testdata_dir))
        config['qserv']['testdata_dir'] = args.testdata_dir
    unittest.TextTestRunner(verbosity=2).run(suite())
