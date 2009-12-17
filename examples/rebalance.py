#!/usr/bin/env python
# rebalance.py
#
# Rebalance Object*_nnn.csv files among K directories.
# Motivation:
# Serge's partitioning script puts csv files in stripe_xxx
# directories.  We want to balance them among the nodes we are
# using. So, we redistribute files among node_xx directories.

# This reuses some (large?) portions from Serge's loader.py
from itertools import chain, imap, izip, repeat
import optparse
import os
from textwrap import dedent

import loader # Assume loader.py is co-located. :)

class RebalanceProgram:
    usage = dedent("""\
    usage: %prog [options] <path> [<path> [<path> ...]]

    Program that takes a bunch of Object*.csv files and rebalances
    them among a number of directories.

    <path>: A path where the program can find Object*.csv files. 
    """)
    parser = optparse.OptionParser(usage)
    parser.add_option(
        "-d", "--dirs", dest="dirs", type="int", default=22, help=dedent("""\
        Number of directories among which to balance (default=%default)."""))
    parser.add_option(
        "-p", "--prefix", dest="prefix", default="Object", help=dedent("""\
        Prefix of files to rebalance (default=%default)."""))
    parser.add_option(
        "-t", "--target", dest="target", default="node", help=dedent("""\
        Target path prefix to which the files will be moved. 
        (default=%default)."""))

    def __init__(self):
        self._setup()

    def _setup(self):
        parser = RebalanceProgram.parser
        (opts, args) = parser.parse_args()
        if len(args) < 1:
            parser.error(dedent("""Must specify at least one path. 
            (How about . ?)"""))
        self._pathList = args
        self._opts = opts
        pass

    def _gatherChunkFiles(self):
        chunkFiles = chain(*imap(loader.findChunkFiles, self._pathList, 
                                 repeat(self._opts.prefix)))
        self._chunkFiles = [x for x in chunkFiles]

    def _checkGathering(self):
        checkDupe = set()
        for l in self._chunkFiles:
            for f in l:
                path, name = os.path.split(f)
                if name in checkDupe:
                    raise RuntimeError(dedent("""\
                                Found 2 identically named chunk files:
                                %s and %s""" % (p, c)))
                checkDupe.add(name)
        pass

    def _move(self):
        chunkCount = len(self._chunkFiles)
        nominal, extra = divmod(chunkCount, self._opts.dirs)
        print chunkCount, "in dirs with at least", nominal
        start = 0

        for i in range(self._opts.dirs):
            length = i * nominal
            if extra: 
                length += 1
                extra -= 1
            stop = start + length
            if stop > chunkCount:
                stop = chunkCount
            targetdir = self._opts.target + str(i)
            try:
                os.listdir(targetdir)
            except OSError:
                os.makedirs(targetdir)
                pass


            for j in range(start, stop):
                for f in self._chunkFiles[j]:
                    path, name = os.path.split(f)
                    print "rename ", f, os.path.join(targetdir, name)
                    os.rename(f, os.path.join(targetdir, name))
            start = stop
        pass

    def run(self):
        self._gatherChunkFiles()
        self._checkGathering()
        self._move()

if __name__ == "__main__":
    p = RebalanceProgram()
    p.run()
