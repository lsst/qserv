#!/usr/bin/env python

from lsst.qserv.tests.benchmark import Benchmark, parseOptions

def main():
    options = parseOptions()
    bench = Benchmark(options.case_no, options.out_dirname,
            options.config_dir)

    bench.run(options.mode_list, options.load_data, options.stop_at_query)

    if bench.areQueryResultsEquals():
        print "Test case%s succeed" % options.case_no
    else:
        print "Test case%s failed" % options.case_no
        if options.load_data==False:
            print ("Please check that corresponding data are loaded, "
                   % "otherwise run test with -l option."
                   % options.case_no)
    
if __name__ == '__main__':
    main()
