#!/usr/bin/env python

from lsst.qserv.admin import commons, logger, download
import logging
from lsst.qserv.tests.benchmark import Benchmark, parseOptions
import os
import tarfile

def download_testdata(test_dir):
    log = logging.getLogger()
    testdata_dir = os.path.join(test_dir,"testdata")
    testdata_archive="testdata.tar.gz"
    testdata_url="http://datasky.in2p3.fr/qserv/distserver/tarballs/"+testdata_archive
    testdata_file=os.path.join(test_dir,testdata_archive)

    if not os.path.isdir(testdata_dir):
        log.debug("downloading test data")
        if not os.path.exists(test_dir):
            os.makedirs(test_dir)
        download.download(testdata_file,testdata_url)
        tar = tarfile.open(testdata_file)
        tar.extractall(test_dir)
        tar.close()

def main():
    
    options = parseOptions()
    config = commons.read_user_config()
    
    logger.init_default_logger(
            "qserv-test-dataset{0}".format(options.case_no),
            logging.DEBUG,
            log_path=config['qserv']['log_dir']
            )

    download_testdata(os.path.join(config['qserv']['base_dir'],"tests"))

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
