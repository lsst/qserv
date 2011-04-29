#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
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
## PT1.1 Objects have the following box:
## ra min: 357.977817138 = -2.0221828620000224
## ra max: = 5.21559213586
## decl min: -6.80690075667
## decl max: 7.11656414672
## thus 49.7 copies are possible in ra (49)
## and 12.9 copies are possible in decl 
## Somewhat less copies are made because of the distance expansion
# near the poles.

# try -S60 -s18 ( 60 stripes, 18 substripes). used for pt1 testing
import csv
from collections import defaultdict
import itertools
from itertools import chain, imap, izip
import math
import optparse
import partition
import random
from textwrap import dedent
import time

import duplicator
        
class DuplicatingIter:
    def __init__(self, iterable, args):
        self.iterable = iterable
        self.gen = self._generateDuplicates(args)
        self.next = self.gen.next # Replace my next() with the generator's

    def __iter__(self):
        return self

    def next(self):
        return self.gen.next()
    
    def _generateDuplicates(self, args):
        copyList = args.copyList
        transformer = duplicator.Transformer(args)
        #header = transformer.selectHeader()
        clip = args.clipChunkCols
        if clip:
            print "Clipping columns for chunk/subchunk"
        for r in self.iterable:
            if clip: rw = r[:-2]
            else: rw = r
            for c in copyList:
                rnew = transformer.transform(rw,c)
                if rnew: yield rnew
            pass
        pass

class Alloc:
    def __init__(self, totalNodes, items, numGetter, conf):
        self.total = totalNodes
        self.numGetter = numGetter
        self.items = items
        self.conf = conf
        self.mapping = self._compute(self.total, self.items)
        self.invalid = self._computeInvalid(items)
        self.sanity()
        pass
    
    def getForNode(self, chosen):
        items = self.items
        # round robin selection
        cLen = len(items)
        i = 0
        c = chosen
        cList = []
        stride = self.total
        if stride == 1: return items[:]
        #FIXME: perhaps range(cLen/total)?
        for i in range(cLen/(stride-1)):
            if c >= cLen: break
            cList.append(items[c])
            c += stride
            
        return cList

    def sanity(self):
        for (n,cl) in self.mapping.iteritems():
            cl = map(self.numGetter, cl)
            cl2 = map(self.numGetter, self.getForNode(n))
            
            if set(cl) != set(cl2):
                print "Alloc mismatch for node", n
                print "Map",cl
                print "naive",cl2
                return False
        return True

    def _compute(self, total, items):
        d = defaultdict(list)
        for (n,c) in izip(itertools.cycle(range(total)), items):
            d[n].append(c)
        return d

    def _computeInvalid(self, items):
        assert items
        s = map(self.numGetter, items)
        s.sort()
        # Round up to 1000s
        r = 1000.0
        cmax = int(math.ceil(s[-1] / r) * r)
        s = set(s)
        
        return [e for e in range(cmax) if e not in s]

class App:
    def __init__(self):
        self.parser = self._makeParser()
        self.shouldDuplicate = False
        self.conf = None
        self.inputs = None
        self.chunks = set()
        pass

    def run(self):
        self._ingestArgs()
        if self.conf.explainArgs:
            self._explainArgs(self.conf)
            return

        if self.conf.dryRun:
            print "Dry run requested. Exiting now."
            return
        if self.shouldDuplicate:
            partition.chunk(self.conf, self.inputs)
        else:
            print "No action specified.  Did you want to duplicate? (--dupe)"
        pass

    def _chunkAcceptor(self, cid):
        return cid in self.chunks

    def _explainArgs(self, option, opt, value, parser):
        pass

    def _explainArgs(self, conf):
        print "Fixed spatial chunking:"
        c = partition.Chunker(conf)
        c.printConfig()
        print "Overlap:", conf.overlap, "deg (%d min)" %(conf.overlap * 60)
        pass

    def _enableDuplication(self, option, opt, value, parser):
        self.shouldDuplicate = True

    def _ingestArgs(self):
        (conf, inputs) = self.parser.parse_args()
        
        # Got inputs?
        if len(inputs) == 0 and self.shouldDuplicate:
            self.parser.error("At least one input file must be specified")

        # Validate and adjust sizes
        if conf.outputBufferSize < 1.0 / 1024 or conf.outputBufferSize > 64.0:
            self.parser.error(dedent("""\
            Output buffer size must be at least 1KiB and no more than
            64MiB."""))
        conf.outputBufferSize = int(conf.outputBufferSize * 1048576.0)
        if conf.inputSplitSize > 256.0:
            self.parser.error("Input split size must not exceed 256 MiB.")
        conf.inputSplitSize = int(conf.inputSplitSize * 1048576.0)

        self._setupChunking(conf)
        if self.shouldDuplicate:
            self._setupDuplication(conf)
        if conf.printDupeLayout:
            pass
        self.conf = conf
        self.inputs = inputs
        pass
    
    def _padEdges(self, bounds, overlap):
        newBounds = bounds[:]
        # Stretch by max stretch.
        factor = max(map(lambda p:math.fabs(1 / math.cos(p)), bounds[2:4]))
        newBounds[0] -= overlap * factor
        newBounds[1] += overlap * factor
        #phi gets padded without stretch
        newBounds[2] -= overlap
        newBounds[3] += overlap
        return newBounds 

    def _setupChunking(self, conf): 
        assert type(conf.node) == int
        assert type(conf.nodeCount) == int
        if not conf.nodeCount: 
            self.parser.error("Node count not specified (--node-count)")

        pd = duplicator.PartitionDef(conf)
        allChunkBounds = [cb for cList in pd.partitionStripes 
                          for cb in cList]
        self._alloc = Alloc(conf.nodeCount, allChunkBounds, 
                            lambda c:c.chunkId, self.conf)
        
        if conf.printAlloc:
            m = self._alloc.mapping
            keys = m.keys()
            keys.sort()
            s = "\n".join(map(lambda k: "%d : %s," 
                              % (k, map(lambda c:c.chunkId,m[k])),
                              keys))
            print "[%s]" % s
            pass

        if conf.writeInvalid:
            inv = self._alloc.invalid
            open(conf.writeInvalid, "w").write(
                "\n".join(imap(str,inv)))

        chunkBounds = self._alloc.mapping[conf.node]
        print "Choosing chunks for %d (%d total) from %s chunks" % (
            conf.node, conf.nodeCount, len(allChunkBounds))

        if conf.emptyFixed:
            print "Empty (clipped) chunks:", pd.emptyChunks
            emptySet = set(pd.emptyChunks)
            chunkBounds = filter(lambda c:c not in emptySet, chunkBounds)
            
        boundsList = map(lambda cb: cb.bounds, chunkBounds)
        # Add padding for overlap
        self.paddedBounds = map(lambda b:self._padEdges(b, conf.overlap), 
                                boundsList)
        self.chunkBounds = chunkBounds
        pass

    def _setupDuplication(self):
        paddedBounds = self.paddedBounds
        chunkBounds = self.chunkBounds
        dd = duplicator.DuplicationDef(conf)        
        if conf.printPlottable:
            pd.printPartBounds(plottable=True)
            dd.printCopyInfo(plottable=True)
        else:
            if conf.printDupeLayout:
                dd.printCopyInfo()
            if conf.printChunkLayout:
                pd.printPartBounds()            

        print "Expanding allocated region to allow overlap=",conf.overlap
        copyList = dd.computeAllCopies(paddedBounds)
            
        print len(copyList), "copies needed of", dd.dupeCount, "available"
        print "Building", len(paddedBounds), "chunks"
        copyInfoList = map(dd.getDupeInfo, copyList)
#        for c in copyList:
#            dd.checkDupeSanityCoord(c)
#        dd.checkDupeSanityThetaLim()
        scma = duplicator.CsvSchema(conf)
        if scma.thetaColumn:
            # Override thetaColumn and phiColumn specs from schema
            setattr(conf, "thetaColumn", scma.thetaColumn)
            setattr(conf, "phiColumn", scma.phiColumn)
        setattr(conf, "headerColumns", scma.headerColumns)
        if scma.clipChunkCols: setattr(conf, "clipChunkCols", True)
        else: setattr(conf, "clipChunkCols", False)
        pw = partition.addPickleWorkaround(
            lambda rows: DuplicatingIter(rows, conf))
        setattr(conf, "rowFilter", pw)
        self.chunks = set(map(lambda cb: cb.chunkId, chunkBounds))
        ca = partition.addPickleWorkaround(self._chunkAcceptor)
        setattr(conf, "chunkAcceptor", ca)
        setattr(conf, "copyList", copyInfoList)
        print "Chunk nums:", sorted(self.chunks)
        print "Dupe nums:", sorted(map(lambda c: c[1], copyInfoList))

        pass

    def _makeParser(self):
        usage = "usage: %prog [options] input_1 input_2 ..."
        parser = optparse.OptionParser(usage)
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
            in the input CSV files; defaults to %default.
            DEPRECATED: Use the schema options instead."""))
        general.add_option(
            "-p", "--phi-column", type="int", dest="phiColumn", default=1,
            help=dedent("""\
            0-based index of the latitude angle (e.g. declination)
            column in the input CSV files; defaults to %default.
            DEPRECATED: Use the schema options instead."""))
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
            "--explain", action="callback", dest="explainArgs",
            action="store_true",
            help="Print current understanding of options and parameters")
        general.add_option(
            "--dry-run", dest="dryRun", action="store_true",
            help="Perform dry run (don't duplicate or partition)")
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
        chunking.add_option(
            "--print-chunk-layout", action="store_true", 
            dest="printChunkLayout",
            help="Print the chunk layout with chunk numbers and bounding boxes")

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

        # Duplication options
        duplication = optparse.OptionGroup(parser, "Duplication options")
        duplication.add_option(
            "--dupe", action="callback", callback=self._enableDuplication,
            help="Turn on duplication.")
        duplication.add_option(
            "--node", type="int", default=0,
            dest="node", help=dedent("""\
            This node's number out of all nodes (0 - (total-1);
            defaults to %default."""))
        duplication.add_option(
            "--node-count", type="int",
            dest="nodeCount", help=dedent("""\
            The total number of nodes.."""))
        duplication.add_option(
            "--empty-stripes", type="int", default=0,
            dest="emptyStripes", help=dedent("""\
            The number of stripes to leave empty.
            Instead of duplicating to fill the sky, chunk stripes can be left
            empty.  This can be used to limit the final data set size.  
            Specify a number that indicates how many stripes will be 
            subtracted from the full set of (partitioned) chunk stripes.
            The most polar stripes will be emptied.  e.g., if 3 stripes 
            are requested empty, the two southernmost (most negative phi)
            stripes and one northernmost (most postive phi) stripe will 
            be left empty. Chunk allocation applies to the new set.
            (see --empty-fixed)"""))
        duplication.add_option(
            "--empty-fixed", action="store_true", 
            dest="emptyFixed", help=dedent("""\
            Set empty stripes after a fixed allocation.
            This option requests chunk allocation to occur before removing
            stripes from the final set.  Thus chunks will be allocated
            assuming a non-empty final set, and then chunks will be removed
            after allocation.  This allows allocations to be as compatible
            as possible even with empty-stripe values.  (only applies when
            --empty-stripes is chosen)."""))
        duplication.add_option(
            "--chunk-list", type="str",
            dest="chunkList", help=dedent("""\
            A comma-separated list of chunk numbers to generate.  
            Cannot be used in conjunction with --node and --nodeCount.
            << NOT IMPLEMENTED >>"""))
        duplication.add_option(
            "--bounds", dest="bounds",
            default=
            "-2.0221828620000224,5.21559213586,-6.80690075667,7.11656414672",
            help=dedent("""\
            Bounding box of input source to be replicated.  The form is
            ra0,ra1,decl0,decl1 , where ra/decl are specified in degrees.  
            Negative and/or floating point numbers are acceptable.  The 
            default is from PT1.1 %default."""))
        duplication.add_option(
            "--print-dupe-layout", dest="printDupeLayout", 
            action="store_true",
            help="Print the layout of the duplicated copies")
        duplication.add_option(
            "--print-plottable", dest="printPlottable",
            action="store_true",
            help="Print something plottable by the hack tcl script")
        duplication.add_option(
            "--print-alloc", dest="printAlloc",
            action="store_true",
            help="Print a mapping from chunks to nodes")
        duplication.add_option(
            "--write-invalid", dest="writeInvalid",
            help="Write out the invalid chunk list to a file.")

        parser.add_option_group(duplication)
        
        schema = optparse.OptionGroup(parser, "Schema options")
        schema.add_option(
            "--schema", type="str", metavar="FILE",
            dest="schemaFile", help=dedent("""\
            Derive column names from FILE, a file containing a MySQL 
            CREATE TABLE statement of the form produced by an invocation 
            of 'mysqldump -T ...' """))
        schema.add_option("--columns", dest="columnNames", default=None,
                          metavar="NAMES",
                          help="Use NAMES as column names(e.g., " + 
                          "'id,ra,dec,flux') for the input.")
        schema.add_option(
            "--theta-name", type="str", dest="thetaName", default="ra_PS",
            help=dedent("""\
            Name of column to use for longitude angle (e.g., right ascension)
            in the input CSV files;
            defaults to %default. Use 'raObject' for the Source table."""))
        schema.add_option(
            "--phi-name", type="str", dest="phiName", default="decl_PS",
            help=dedent("""\
            Name of column to use for latitude angle (e.g., declination) 
            in the input CSV files;
            defaults to %default. Use 'declObject' for the Source table."""))
        parser.add_option_group(schema)
        return parser

def main():
    a = App()
    a.run()
    pass

if __name__ == "__main__":
    main()
