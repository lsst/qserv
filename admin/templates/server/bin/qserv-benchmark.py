#!/usr/bin/env python

from lsst.qserv.admin import commons, logger
import logging
from lsst.qserv.tests.benchmark import Benchmark, parseOptions

def main():
    
    options = parseOptions()
    
    config = commons.read_user_config()

    logger.init_default_logger(
            "qserv-test-dataset{0}".format(options.case_no),
            logging.INFO,
            log_path=config['qserv']['log_dir']
            )
    
    bench = Benchmark(options.case_no, options.out_dirname)

    bench.run(options.mode_list, options.load_data, options.stop_at_query)

    failed_queries = bench.analyzeQueryResults()

    if len(failed_queries) == 0:
        print "Test case%s succeed" % options.case_no
    else:
        print "Test case%s failed" % options.case_no
    
    if options.load_data == False:
            print ("Please check that corresponding data are loaded, otherwise run {0} with -l option.".format(os.path.basename(__file__)))
    
if __name__ == '__main__':
    main()
