#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# makeChunk.py creates partitioned chunks from an input data
# set. The input data set is conceptually duplicated over the whole
# sky, packing linearly across stripes, but roughly taking into
# account the convergence of meridians at the poles.  partition.py is
# used to compute chunk boundaries, the input data set is duplicated
# as necessary to pass to the partitioner, which should only output
# the chunks specified.
# 
# 

import csv
import optparse
import partition
from textwrap import dedent
import time

class ChunkBounds:
    def __init__(self, chunkId, bounds):
        self.chunkId = chunkId
        self.bounds = bounds[:]
        pass

        
    def __str__(self):
        return "%d : %s" % (self.chunkId, str(self.bounds))

class App:
    def __init__(self):
        usage = "usage: %prog [options] input_1 input_2 ..."
        parser = optparse.OptionParser(usage)
        def explainArgs(option,opt,value,parser):
            conf = parser.values
            print "Fixed spatial chunking:"
            c = partition.Chunker(conf)
            c.printConfig()
            print "Overlap:", conf.overlap, "deg (%d min)" %(conf.overlap * 60)
            pass
        general = optparse.OptionGroup(parser, "General options")
        general.add_option(
            "-o", "--overlap", type="float", dest="overlap", default=0.01667,
            help="Chunk/sub-chunk overlap radius (deg); defaults to %default.")
        general.add_option(
            "-O", "--output-dir", dest="outputDir", default=".",
            help=dedent("""\
            Output directory. If omitted the current working
            directory is used."""))
        general.add_option(
            "-t", "--theta-column", type="int", dest="thetaColumn", default=0,
            help=dedent("""\
            0-based index of the longitude angle (e.g. right ascension) column
            in the input CSV files; defaults to %default."""))
        general.add_option(
            "-p", "--phi-column", type="int", dest="phiColumn", default=1,
            help=dedent("""\
            0-based index of the latitude angle (e.g. declination)
            column in the input CSV files; defaults to %default."""))
        general.add_option(
            "-P", "--chunk-prefix", dest="chunkPrefix",  default="Object",
            help=dedent("""\
            Prefix for output chunk file names, defaults to %default. An
            underscore followed by the chunkId is appended to this prefix.
            Full and self overlap files are named similarly, but contain the
            'FullOverlap' / 'SelfOverlap' strings between the prefix and
            chunkId."""))
        general.add_option(
            "-l", "--skip-lines", type="int", dest="skipLines", default=0,
            help=dedent("""\
            Number of initial lines to skip in the input files.
            By default no lines are skipped."""))
        general.add_option(
            "-j", "--num-workers", type="int", dest="numWorkers",
            help=dedent("""\
            (Python 2.6+) Number of worker processes to use. Omitting this
            option or specifying a value less than 1 will launch as many
            workers as there are CPUs in the system."""))
        general.add_option(
            "-v", "--verbose", dest="verbose", action="store_true",
            help="Wordy progress reports.")
        general.add_option(
            "-d", "--debug", dest="debug", action="store_true",
            help="Print debug messages")
        general.add_option(
            "--explain", action="callback", callback=explainArgs,
            help="Print current understanding of options and parameters")
        parser.add_option_group(general)

        # Standard chunking options
        chunking = optparse.OptionGroup(parser, "Standard chunking options")
        chunking.add_option(
            "-S", "--num-stripes", type="int", dest="numStripes", default=18,
            help=dedent("""\
            Number of declination stripes to create chunks from. The default is
            %default."""))
        chunking.add_option(
            "-s", "--num-sub-stripes", type="int", dest="numSubStripes",
            default=100, help=dedent("""\
            Number of sub-stripes to divide each stripe into. The default is
            %default."""))
        parser.add_option_group(chunking)

        # CSV format options
        fmt = optparse.OptionGroup(
            parser, "CSV format options", dedent("""\
            See http://docs.python.org/library/csv.html#csv-fmt-params for
             details."""))
        fmt.add_option(
            "-D", "--delimiter", dest="delimiter", default=",",
            help=dedent("""\
            One character string used to separate fields in the
            input CSV files. The default is %default."""))
        fmt.add_option(
            "-n", "--no-doublequote", dest="doublequote", action="store_false",
            help=dedent("""\
            Turn off double quoting of quote characters inside a CSV field."""))
        fmt.add_option(
            "-e", "--escapechar", dest="escapechar", default=None,
            help="Delimiter escape character.")
        quoteHelp = dedent("""\
            CSV quoting style. May be one of %d  (quote all fields), %d (quote
            fields containing special characters), %d (quote non-numeric fields)
            or %d (never quote fields). The default is %%default.""" %
                           (csv.QUOTE_ALL, csv.QUOTE_MINIMAL,
                            csv.QUOTE_NONNUMERIC, csv.QUOTE_NONE))
        fmt.add_option(
            "-Q", "--quoting", type="int", dest="quoting",
            default=csv.QUOTE_MINIMAL, help=quoteHelp)
        fmt.add_option(
            "-q", "--quotechar", dest="quotechar", default='"',
            help=dedent("""\
            One character string to quote fields with; default is %default."""))
        fmt.add_option(
            "-I", "--skipinitialspace", dest="skipinitialspace",
            action="store_true", help=dedent("""\
            Ignore whitespace immediately following delimiters."""))
        parser.add_option_group(fmt)

        # Tuning options
        tuning = optparse.OptionGroup(parser, "Tuning options")
        tuning.add_option(
            "-b", "--output-buffer-size", type="float", default=1.0,
            dest="outputBufferSize", help=dedent("""\
            Size in MiB of in-memory output buffers. This is approximately the
            granularity at which file writes occur; fractional values are 
            allowed. The default is %default."""))
        tuning.add_option(
            "-m", "--max-open-writers", type="int", default=32,
            dest="maxOpenWriters", help=dedent("""\
            Maximum number of open chunk file writers per process; defaults to
            %default. Up to 3 file handles are opened per writer."""))
        tuning.add_option(
            "-i", "--input-split-size", type="float", default=64.0,
            dest="inputSplitSize", help=dedent("""\
            Approximate size in MiB of input file splits; defaults to %default.
            Fractional values are allowed."""))
        parser.add_option_group(tuning)
        self.parser = parser
        pass

    def printChunkMap(self, writers):
        for declNum in range(len(writers)):
            wl = writers[declNum]
            for raNum in range(len(wl)):
                w = wl[raNum]
                print "%5s" % (w.chunkId),
            print
        pass

    def printChunkCounts(self, chunker):
        for c in chunker.numChunks:
            print c
        print len(chunker.numSubChunks)

    def printBounds(self, writers, chunker):
        coords = [0,0,0,0] # [stripenum, substripenum, chunkoff, subcoff]
        bounds = [0,0,0,0] # [thetamin, thetamax, phimin, phimax]
        stripes = []
        thetaLim = 360.0
        phiLim = 90.0
        for stripeNum in range(len(writers)):
            curStripes = []
            wl = writers[stripeNum]
            for w in wl:
                #coords[3] = chunker.numSubStripes - 1
                #coords[3] = chun
                chunker.setCoordsFromIds(w.chunkId, 0, coords)
                chunker.setBounds(coords, bounds)
                c = ChunkBounds(w.chunkId, bounds)
                print w.chunkId, bounds
                phiMin = bounds[2]
                if stripes and not curStripes: # back-push phi mins
                    for sc in stripes[-1]:
                        sc.bounds[3] = phiMin
                        pass
                curStripes.append(c)
            if len(curStripes) > 1:
                # back-push theta mins 
                curStripes[-1].bounds[1] = thetaLim
                for i in range(len(curStripes) - 1):
                    curStripes[i].bounds[1] = curStripes[i+1].bounds[0]
                    pass
            stripes.append(curStripes[:])
        # patch last chunk
        stripes[-1][0].bounds[1] = thetaLim
        stripes[-1][0].bounds[3] = phiLim
        a = 0
        for s in stripes:
            print "Stripe", a
            print "\n".join(map(str, s))
            a += 1
            #if a > 2: break

    def run(self):
        (conf, inputs) = self.parser.parse_args()
        s = partition.SpatialChunkMapper(conf,int(time.time()))
        print sum(map(len,s.writers))
        self.printChunkMap(s.writers)
        #self.printChunkCounts(s.chunker)
        self.printBounds(s.writers, s.chunker)
        
            

def main():
    a = App()
    a.run()
    pass

if __name__ == "__main__":
    main()
