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

#
# An attempt to avoid constructs not supported by python 2.4.x has been
# made, but this script remains untested on python 2.4.x.
import cStringIO as sio
import csv
from copy import copy
import errno
import fcntl
from glob import glob
from itertools import chain, count, groupby, izip, repeat
import math
import mmap
import numpy as np
import operator
import optparse
import os, os.path
import pdb
import re
from textwrap import dedent
import time

try:
    # requires python 2.6.x
    import multiprocessing as mp
    _have_mp = True
except ImportError:
    # process in serial fashion
    _have_mp = False

try:
    # requires python 2.7.x
    from collections import OrderedDict
except ImportError:
    from UserDict import DictMixin

    # Python 2.4 lacks all()
    try:
        all
    except NameError:
        def all(seq):
            for elem in seq:
                if not elem:
                    return False
            return True

    class OrderedDict(dict, DictMixin):
        """Drop-in replacement for collections.OrderedDict by
        Raymond Hettinger, see http://code.activestate.com/recipes/576693/
        """
        def __init__(self, *args, **kwds):
            if len(args) > 1:
                raise TypeError('expected at most 1 arguments, got %d' %
                                len(args))
            try:
                self.__end
            except AttributeError:
                self.clear()
            self.update(*args, **kwds)

        def clear(self):
            self.__end = end = []
            end += [None, end, end] # sentinel node for doubly linked list
            self.__map = {}         # key --> [key, prev, next]
            dict.clear(self)

        def __setitem__(self, key, value):
            if key not in self:
                end = self.__end
                curr = end[1]
                curr[2] = end[1] = self.__map[key] = [key, curr, end]
            dict.__setitem__(self, key, value)

        def __delitem__(self, key):
            dict.__delitem__(self, key)
            key, prev, next = self.__map.pop(key)
            prev[2] = next
            next[1] = prev

        def __iter__(self):
            end = self.__end
            curr = end[2]
            while curr is not end:
                yield curr[0]
                curr = curr[2]

        def __reversed__(self):
            end = self.__end
            curr = end[1]
            while curr is not end:
                yield curr[0]
                curr = curr[1]

        def popitem(self, last=True):
            if not self:
                raise KeyError('dictionary is empty')
            if last:
                key = reversed(self).next()
            else:
                key = iter(self).next()
            value = self.pop(key)
            return key, value

        def __reduce__(self):
            items = [[k, self[k]] for k in self]
            tmp = self.__map, self.__end
            del self.__map, self.__end
            inst_dict = vars(self).copy()
            self.__map, self.__end = tmp
            if inst_dict:
                return (self.__class__, (items,), inst_dict)
            return self.__class__, (items,)

        def keys(self):
            return list(self)

        setdefault = DictMixin.setdefault
        update = DictMixin.update
        pop = DictMixin.pop
        values = DictMixin.values
        items = DictMixin.items
        iterkeys = DictMixin.iterkeys
        itervalues = DictMixin.itervalues
        iteritems = DictMixin.iteritems

        def __repr__(self):
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, self.items())

        def copy(self):
            return self.__class__(self)

        @classmethod
        def fromkeys(cls, iterable, value=None):
            d = cls()
            for key in iterable:
                d[key] = value
            return d

        def __eq__(self, other):
            if isinstance(other, OrderedDict):
                return len(self)==len(other) and \
                       all(p==q for p, q in  zip(self.items(), other.items()))
            return dict.__eq__(self, other)

        def __ne__(self, other):
            return not self == other

# -- Working around pickle limitations --------
pickleWorkaround = dict()
pickleWorkaroundCounter = 0

def addPickleWorkaround(obj):
    global pickleWorkaround, pickleWorkaroundCounter
    n = pickleWorkaroundCounter
    pickleWorkaroundCounter += 1
    pickleWorkaround[n] = obj
    return n

# -- Iterating over CSV records in file subsets --------

def _csvArgs(conf, mode="r"):
    """Extract and return csv formatting arguments from conf.
    """
    if mode=='w':
        delimiter = conf.delimiter_out
    else:
        delimiter = conf.delimiter

    return { 'delimiter': delimiter,
             'doublequote': conf.doublequote,
             'quoting': conf.quoting,
             'quotechar': conf.quotechar,
             'skipinitialspace': conf.skipinitialspace,
             'lineterminator': '\n'
           }

class InputSplit(object):
    """A contiguous sequence of bytes in a file.
    """
    def __init__(self, path, offset, length, skip=0):
        assert isinstance(offset, (int, long)) and offset >= 0
        assert isinstance(length, (int, long)) and length > 0
        self.path = path
        self.offset = offset
        self.length = length
        self.skip = skip

    def __str__(self):
        return "InputSplit (%s, [%d: %d], skip=%d)" % (
            self.path, self.offset, self.offset + self.length, self.skip)

class MMapIter(object):
    """Iterator over lines in a mmap.mmap object; the mmap.mmap
    object is NOT closed by this class.
    """
    def __init__(self, mem):
        self.mem = mem
    def __iter__(self):
        return self
    def next(self):
        line = self.mem.readline()
        if len(line) == 0:
            raise StopIteration()
        return line

class FileIter(object):
    """Iterator over lines in a subset of a file object; the file
    object is closed once the last line has been read.
    """
    def __init__(self, inputSplit):
        self.file = open(inputSplit.path, 'rb')
        self.file.seek(inputSplit.offset, os.SEEK_SET)
        self.end = inputSplit.offset + inputSplit.length
        for i in xrange(inputSplit.skip):
            self.file.readline()
    def __iter__(self):
        return self
    def next(self):
        if self.file.tell() >= self.end:
            self.file.close()
            raise StopIteration()
        return self.file.readline()

class InputSplitIter(object):
    """Iterator over CSV records in an InputSplit. Uses regular file IO,
    since Python 2.5.x and earlier are incapable of memory mapping subsets
    of a file starting at a non-zero offset.
    """
    def __init__(self, inputSplit, **kwargs):
        self.fileIter = FileIter(inputSplit)

        self.reader = csv.reader(self.fileIter, **kwargs)
        # self.reader = csv.reader(self.fileIter, delimiter="\t")
    def __iter__(self):
        return self
    def next(self):
        csvline = self.reader.next()
        return csvline

# Size of blocks to read when searching backwards for line terminators
LT_BLOCK_SIZE = mmap.PAGESIZE

def computeInputSplits(inputPaths, splitSize, skip):
    """Return a list containing one or more input splits of roughly splitSize
    bytes for each file in inputPaths. If splitSize is <= 0, one input split
    will be generated for each file. Otherwise, splits are arranged such that
    they begin on a line and end with a line-terminator.

    Note that what we really want is to find the beginnings of CSV records;
    in the worst case this requires searching backwards from an offset to the
    very beginning of the file. The logic here simply searches backwards for
    the first line terminator, disregarding issues that can arise from
    records containing e.g. quoted strings with embedded new-lines. For such
    CSV files, a splitSize of 0 should be used.
    """
    splits = []
    for j, path in enumerate(inputPaths):
        size = os.stat(path).st_size
        if size <= splitSize or splitSize <= 0:
            splits.append(InputSplit(path, 0, size, skip))
        else:
            # Multiple splits per file - start with evenenly spaced splits,
            # then adjust each starting offset backwards until a line
            # terminator ('\n') is hit.
            numSplits = size/splitSize
            if size % splitSize != 0:
                numSplits += 1
            off = [i*splitSize for i in xrange(numSplits)]
            inFile = open(path, 'rb')
            try:
                i = 1
                while i < len(off):
                    start = off[i]
                    j = -1
                    while j == -1 and start > 0:
                        dest = max(0, start - LT_BLOCK_SIZE)
                        inFile.seek(dest, os.SEEK_SET)
                        buf = inFile.read(start - dest)
                        start = dest
                        j = buf.rfind('\n')
                    start += j + 1
                    if start == off[i - 1]:
                        # Line spanned an entire split
                        del off[i]
                    else:
                        off[i] = start
                        i += 1
                off.append(size)
                for i in xrange(len(off) - 1):
                    if i == 0: s = skip
                    else:      s = 0
                    splits.append(InputSplit(path, off[i],
                                             off[i + 1] - off[i], s))
            finally:
                inFile.close()
    return splits


# -- Map/Reduce ----------------

def _emit(x, output):
    if x != None:
        if isinstance(x, list):
            output += x
        elif isinstance(x, tuple):
            output.append(x)
        else:
            raise TypeError("map() must produce a 2-tuple or 2-tuple list")

def _dispatchMapper(args):
    split, mapperType, conf, i = args
    results = []
    mapper = mapperType(conf, i)

    rows = InputSplitIter(split, **_csvArgs(conf))
    if hasattr(conf, "rowFilter"): # Allow optional filter (e.g., duplicator)
        rows = pickleWorkaround[conf.rowFilter](rows)
    for row in rows:
        _emit(mapper.map(row), results)
    if hasattr(mapperType, 'finish'):
        _emit(mapper.finish(conf), results)
    return results

# Maintain one reducer instance per process
_reducer = None

def _dispatchReducer(args):
    global _reducer
    keyValues, reducerType, conf = args
    if _reducer == None:
        _reducer = reducerType(conf)
    _reducer.reduce(*keyValues)

class SerialPool(object):
    """Simple drop-in replacement for a subset of the multiprocessing.Pool
    class; all tasks are run in the same process as the caller."""
    def __init__(self, numWorkers):
        self._pool = [None]
    def map(self, fun, seq, chunkSize=None):
        return map(fun, seq)
    def close(self):
        pass

def mapReduce(mapper, reducer, conf, inputSplits, numWorkers):
    """Primitive map/reduce implementation. Map and reduce are
    run in parallel if multiprocessing is available; the sorting of
    intermediate results is always performed by the master process.

    mapper is a type that
     * must provide a constructor accepting an object and an integer
       as an argument.
     * must provide a map() method accepting a single argument
       corresponding to a CSV record. The return value must be None,
       a 2 element tuple, or a list of 2 element tuples. It must be
       possible to pickle the tuple contents.
     * may provide a finish() method accepting an object as an argument. The
       method is called immediately after the last map() call. The return
       value may be None, a 2 element tuple or a list of 2 element tuples
       and counts towards map output. This allows implementation of
       combiner-like functionality within a mapper.

    reducer is a type that
     * must provide a constructor accepting an object as an argument.
     * must provide a reduce() method accepting a key and a list-of-values
       as arguments. The function is called once for each key output during
       the reduce-phase; values is a list of all map output with that key.
    """
    if len(inputSplits) == 0:
        return []
    if (mapper == None or not isinstance(mapper, type) or
        not hasattr(mapper, 'map')):
        raise TypeError("Mapper must be a type with a 'map' attribute, got " +
                        repr(mapper))
    if reducer != None and not (isinstance(reducer, type) and
       hasattr(reducer, 'reduce')):
       raise TypeError("Reducer must be a type with a 'reduce' attribute, got " +
                       repr(reducer))

    keyfun = lambda x: x[0]
    inSeq = izip(inputSplits, repeat(mapper), repeat(conf), count())
    if _have_mp and numWorkers != 1 and len(inputSplits) != 1:
        poolType = mp.Pool
    else:
        poolType = SerialPool
    pool = poolType(numWorkers)
    try:
        t = time.time()
        results = pool.map(_dispatchMapper, inSeq)
        if conf.verbose:
            print "Map phase finished in %f sec" % (time.time() - t)
        t = time.time()
        results = sorted(reduce(operator.iadd, results), key=keyfun)
        results = [(k, [e[1] for e in g]) for k, g in groupby(results, keyfun)]
        if conf.verbose:
            print "Sort phase finished in %f sec" % (time.time() - t)
        if reducer != None:
            n = len(pool._pool)
            outSeq = izip(results, repeat(reducer), repeat(conf))
            t = time.time()
            pool.map(_dispatchReducer, outSeq, 1 + len(results) / n)
            if conf.verbose:
                print "Reduce phase finished in %f sec" % (time.time() - t)
        else:
            return results
    finally:
        pool.close()


# -- Geometry and associatied utilities ----------------

DEG_PER_ARCSEC = 1.0/3600.0
EPSILON = 0.001 * DEG_PER_ARCSEC

def maxAlpha(r, centerPhi):
    """Computes the extent in longitude [-alpha,alpha] of the circle
    with radius r and center (0, centerPhi) on the unit sphere.
    Both r and centerPhi are assumed to be in units of degrees;
    centerPhi is clamped to lie in the range [-90,90] and r must
    lie in the range [0, 90].
    """
    assert r >= 0.0 and r <= 90.0
    if r == 0.0:
        return 0.0
    centerPhi = clampPhi(centerPhi)
    if abs(centerPhi) + r > 90.0 - 60.0 * DEG_PER_ARCSEC:
        return 180.0
    r = math.radians(r)
    c = math.radians(centerPhi)
    y = math.sin(r)
    x = math.sqrt(abs(math.cos(c - r) * math.cos(c + r)))
    return math.degrees(abs(math.atan(y / x)))

def segments(phiMin, phiMax, width):
    """Computes the number of segments to divide the given declination range
    (stripe) into. Two points in the declination range separated by at least
    one segment are guaranteed to be separated by an angular distance of at
    least width.
    """
    p = max(abs(phiMin), abs(phiMax))
    if p > 90.0 - 1 * DEG_PER_ARCSEC:
        return 1
    if width >= 180.0:
        return 1
    elif width < 1 * DEG_PER_ARCSEC:
        width = 1 * DEG_PER_ARCSEC
    p = math.radians(p)
    cw = math.cos(math.radians(width));
    sp = math.sin(p);
    cp = math.cos(p);
    x = cw - sp * sp
    u = cp * cp
    y = math.sqrt(abs(u * u - x * x))
    return int(math.floor((2.0 * math.pi) / abs(math.atan2(y, x))))

def clampTheta(theta):
    """For use in computing partition bounds: clamps an input longitude angle
    to 360.0 deg. Any input angle >= 360.0 - EPSILON is mapped to 360.0.
    This is because partition bounds are computed by multiplying a sub-chunk
    width by a sub-chunk number; the last sub-chunk in a sub-stripe can
    therefore have a maximum longitude angle very slightly less than 360.0.
    """
    if theta >= 360.0 or (360.0 - theta < EPSILON):
        return 360.0
    return theta

def clampPhi(phi):
    """For use in computing partition bounds: clamps an input latitude angle
    to [-90.0, 90.0 + EPSILON] deg. Any input angle >= 90.0 is mapped to
    90.0 + EPSILON. This is because the upper latitude angle bound of a
    partition is non-inclusive; clamping to 90.0 would necessitate a zero
    area partition containing only the north pole.
    """
    if phi < -90.0:
        return -90.0
    elif phi >= 90.0:
        return 90.0 + EPSILON
    return phi

def minDeltaTheta(theta1, theta2):
    """Return minimum delta between two longitude angles.
    """
    delta = abs(theta1 - theta2)
    return min(delta, 360.0 - delta)

def segmentWidth(phiMin, phiMax, numSegments):
    """Return the angular width of a single segment obtained by
    chopping the latitude angle stripe [phiMin, phiMax] into
    numSegments equal width (in longitude angle) segments.
    """
    p = math.radians(max(abs(phiMin), abs(phiMax)))
    cw = math.cos(math.radians(360.0 / numSegments))
    sp = math.sin(p);
    cp = math.cos(p);
    return math.degrees(math.acos(cw * cp * cp + sp * sp))


# -- File IO --------

class CsvFileWriter(object):
    def __init__(self, path, writerId, conf):
        if not os.path.exists(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except OSError, e:
                # Two or more processes can simultaneously detect that a
                # directory does not exist and attempt to create it; only
                # one will succeed without this check.
                if e.errno != errno.EEXIST:
                    raise e
        self.path = path
        self.writerId = writerId
        self.file = None
        self.buffer = None
        self.writer = None
        self.numRows = 0
        self.csvArgs = _csvArgs(conf, 'w')
        self.bufferSize = conf.outputBufferSize
        self.debug = conf.debug

    def getId(self):
        return self.writerId

    def _open(self):
        if self.debug:
            print "Opening " + self.path
        self.file = open(self.path, 'ab')
        self.buffer = sio.StringIO()
        self.writer = csv.writer(self.buffer, **(self.csvArgs))
    def _flush(self):
        if self.file != None and self.buffer.tell() > 0:
            if self.debug:
                print "Flushing " + self.path
            # Concurrent writes to files opened in append mode are supposed
            # to be serialized (i.e no interleaved data); be paranoid for now.
            fcntl.lockf(self.file.fileno(), fcntl.LOCK_EX)
            try:
                self.file.write(self.buffer.getvalue())
                self.file.flush()
            finally:
                fcntl.lockf(self.file.fileno(), fcntl.LOCK_UN)
                self.buffer.truncate(0)

    def writerow(self, row):
        """Write row to the file, allocating resources as necessary.
        """
        if self.file == None:
            self._open()
        self.writer.writerow(row)
        self.numRows += 1
        if self.buffer.tell() > self.bufferSize:
            self._flush()

    def close(self):
        """Release any system resources held by this CsvFileWriter.
        """
        if self.debug:
            print "Closing %s (%d rows written)" % (self.path, self.numRows)
        self.numRows = 0
        self._flush()
        if self.file != None:
            self.file.close()
            self.buffer.close()
            self.file = None
            self.buffer = None
            self.writer = None

class ChunkWriter(object):
    """CSV chunk file writer; handles writing records to a
    chunk file and the associated full and self overlap files.
    """
    CHUNK = 0
    FULL_OVERLAP = 1
    SELF_OVERLAP = 2

    def __init__(self, paths, chunkId, conf):
        assert len(paths) == 3
        self.writers = [CsvFileWriter(p, chunkId, conf) for p in paths]
        self.chunkId = chunkId
        self.not_written = True
        self.paths = paths


    def getId(self):
        return self.chunkId

    def writerow(self, row, which):
        """Write row to the chunk file or its full-overlap or
        self-overlap file, allocating resources as necessary.
        """
        # if chunk file isn't empty
        # empty full-overlap or self-overlap will be created (even if empty)
        if self.not_written :
            [open(p, 'w').close() for p in self.paths]
            self.not_written = False

        self.writers[which].writerow(row)

    def close(self):
        """Release system resources held by this ChunkWriter.
        """
        for w in self.writers:
            w.close()

class LRUCache(object):
    """An LRU cache of objects; any object with getId() and close() methods
    can be cached.
    """
    def __init__(self, maxObjects):
        self.maxObjects = maxObjects
        self.objects = OrderedDict()

    def update(self, obj):
        """Mark obj as the most recently used object. If obj is
        not in the cache and the cache is full, then the least
        recently used object is closed and evicted.
        """
        objId = obj.getId()
        if objId not in self.objects:
            if len(self.objects) >= self.maxObjects:
                o = self.objects.popitem(False)
                o[1].close()
            self.objects[objId] = obj
        else:
            # Move writer to the end of the OrderedDict
            del self.objects[objId]
            self.objects[objId] = obj
        return obj

    def close(self):
        """Close and remove all objects from the cache.
        """
        for obj in self.objects.itervalues():
            obj.close()
        self.objects.clear()


# -- Non-adaptive spatial chunking --------

class Chunker(object):
    """Class for mapping positions to sub-chunks in a purely spatial manner.
    """
    def __init__(self, conf):
        overlap = conf.overlap
        ns = conf.numStripes
        nsub = conf.numSubStripes
        h = 180.0 / ns
        hs = 180.0 / (ns * nsub)
        self.overlap = overlap
        self.stripeHeight = h
        self.subStripeHeight = hs
        self.numSubStripes = nsub
        self.numChunks = [segments(i*h - 90.0, (i+1)*h - 90.0, h)
                          for i in xrange(ns)]
        self.numSubChunks = []
        self.subChunkWidth = []
        self.alpha = []
        self._coords = np.array([0, 0, 0, 0], dtype=np.int32)
        for i in xrange(ns * nsub):
            nc = self.numChunks[i / nsub]
            n = segments(i*hs - 90.0, (i+1)*hs - 90.0, hs) / nc
            self.numSubChunks.append(n)
            w = 360.0 / (n * nc)
            self.subChunkWidth.append(w)
            a = maxAlpha(overlap, max(abs(i*hs - 90.0), abs((i+1)*hs - 90.0)))
            assert a <= w
            self.alpha.append(a)
        self.maxSubChunks = max(self.numSubChunks)

    def getNumStripes(self):
        return len(self.numChunks)

    def getNumChunks(self, stripe):
        return self.numChunks[stripe]

    def setCoords(self, theta, phi, coords):
        """Set the chunk and sub-chunk coordinates to those of the given
        point.
        """
        coords[1] = int(math.floor((phi + 90.0) / self.subStripeHeight))
        if coords[1] >= self.numSubStripes * len(self.numChunks):
            coords[1] = self.numSubStripes * len(self.numChunks) - 1
        coords[0] = coords[1] / self.numSubStripes
        # chunk and sub-chunk within stripe
        coords[3] = int(math.floor(theta / self.subChunkWidth[coords[1]]))
        nsc = self.numSubChunks[coords[1]]
        if coords[3] >= nsc * self.numChunks[coords[0]]:
            coords[3] = nsc * self.numChunks[coords[0]] - 1
        coords[2] = coords[3] / nsc

    def setCoordsFromIds(self, chunkId, subChunkId, coords):
        """Set the chunk and sub-chunk coordinates to those of the given ids.
        """
        coords[0] = chunkId / (len(self.numChunks) * 2)
        coords[2] = chunkId - coords[0] * len(self.numChunks) * 2
        ssb = coords[0] * self.numSubStripes
        coords[1] = subChunkId / self.maxSubChunks + ssb
        n = self.numSubChunks[coords[1]]
        sco = (coords[1] - ssb) * self.maxSubChunks
        coords[3] = subChunkId - sco + coords[2] * n

    def getIds(self, coords):
        """Given a coordinate array containing a stripe number,
        sub-stripe number, chunk number, and sub-chunk number,
        return the corresponding [chunkId, subChunkId] list."""
        chunkId = coords[0] * len(self.numChunks) * 2 + coords[2]
        rss = (coords[1] - coords[0] * self.numSubStripes) * self.maxSubChunks
        rsc = (coords[3] - coords[2] * self.numSubChunks[coords[1]])
        subChunkId = rss + rsc
        return [chunkId, subChunkId]

    def setBounds(self, coords, bounds):
        """Given a coordinate array containing a stripe number,
        sub-stripe number, chunk number, and sub-chunk number, set the
        longitude/latitude angle bounds (thetaMin, thetaMax, phiMin, phiMax)
        for the corresponding sub-chunk."""
        bounds[0] = coords[3] * self.subChunkWidth[coords[1]]
        bounds[1] = clampTheta((coords[3] + 1) * self.subChunkWidth[coords[1]])
        bounds[2] = clampPhi(coords[1] * self.subStripeHeight - 90.0)
        bounds[3] = clampPhi((coords[1] + 1) * self.subStripeHeight - 90.0)

    def _upDownOverlap(self, results, theta, thetaMin, thetaMax, both):
        nc = self.numChunks[self._coords[0]]
        nsc = self.numSubChunks[self._coords[1]]
        tc = nc * nsc
        w = self.subChunkWidth[self._coords[1]]
        a = self.alpha[self._coords[1]]
        minsc = int(math.floor((theta - a) / w))
        if minsc < 0:
            minsc += tc
        maxsc = int(math.floor((theta + a) / w))
        if maxsc >= tc:
            maxsc -= tc
        if minsc > maxsc:
            # longitude wrap-around
            itr = chain(xrange(minsc, tc), xrange(0, maxsc + 1))
        else:
            itr = xrange(minsc, maxsc + 1)
        for sc in itr:
            self._coords[3] = sc
            self._coords[2] = sc / nsc
            results.append((self.getIds(self._coords),
                            (self._coords[0], self._coords[2]), both))

    def getOverlap(self, theta, phi, coords, bounds):
        """Given a position P = (theta, phi) and corresponding sub-chunk
        coordinates/bounds, return a list of
        ((chunkId, subChunkId), (stripe, chunk), both) tuples corresponding
        to the ids and stripe/chunk numbers of the sub-chunks whose overlap
        regions contain P. both is set to True if P lies in the full
        and self overlap regions of that sub-chunk. Otherwise, P lies only
        in the full-overlap region.
        """
        results = []
        alpha = self.alpha[coords[1]]
        if bounds[2] > -90.0 and phi < bounds[2] + self.overlap:
            # P is in full-overlap region of sub-chunks 1 sub-stripe down
            self._coords[1] = coords[1] - 1
            self._coords[0] = self._coords[1] / self.numSubStripes
            self._upDownOverlap(results, theta, bounds[0], bounds[1], False)
        if bounds[3] < 90.0 and phi >= bounds[3] - self.overlap:
            # P is in full/self-overlap regions of sub-chunks 1 sub-stripe up
            self._coords[1] = coords[1] + 1
            self._coords[0] = self._coords[1] / self.numSubStripes
            self._upDownOverlap(results, theta, bounds[0], bounds[1], True)
        nsc = self.numSubChunks[coords[1]]
        if nsc > 1:
            if theta < bounds[0] + alpha:
                # P is in full/self-overlap region of sub-chunk to the left
                self._coords[:] = coords
                self._coords[3] -= 1
                if self._coords[3] == -1:
                    nc = self.numChunks[coords[0]]
                    self._coords[2] = nc - 1
                    self._coords[3] = nc * nsc - 1
                else:
                    self._coords[2] = self._coords[3] / nsc
                results.append((self.getIds(self._coords),
                                (self._coords[0], self._coords[2]),
                                True))
            if theta >= bounds[1] - alpha:
                # P is in full-overlap region of sub-chunk to the right
                self._coords[:] = coords
                self._coords[3] += 1
                nc = self.numChunks[coords[0]]
                if self._coords[3] == nc * nsc:
                    self._coords[2:4] = 0
                else:
                    self._coords[2] = self._coords[3] / nsc
                results.append((self.getIds(self._coords),
                                (self._coords[0], self._coords[2]),
                                False))
        return results

    def printConfig(self):
        print "Number of stripes: %d" % self.getNumStripes()
        print "Number of sub-stripes per stripe: %d" % self.numSubStripes
        print "Stripe height: %.9g deg" % self.stripeHeight
        print "Sub-stripe height: %.9g deg" % self.subStripeHeight,
        print "(%d min)" % (self.subStripeHeight * 60)


class SpatialChunkMapper(object):
    """Mapper which bucket sorts CSV records by chunk; map output is
    [(chunkId, {subChunkId: numRows})].
    """
    def __init__(self, conf, i):
        self.thetaColumn = conf.thetaColumn
        self.phiColumn = conf.phiColumn
        self.overlap = conf.overlap
        self.cache = LRUCache(conf.maxOpenWriters)
        self.chunker = Chunker(conf)
        self.coords = np.array([0, 0, 0, 0], dtype=np.int32)
        self.bounds = np.array([0.0, 0.0, 0.0, 0.0], dtype=np.float64)
        self.chunks = {}
        self._initRowCombiner(conf) # Init flexible row writer.
        if hasattr(conf, "chunkAcceptor"): # allow client to select chunks
            self.chunkAcceptor = pickleWorkaround[conf.chunkAcceptor]
        else:
            self.chunkAcceptor = None

        # Build chunk writer arrays
        ns = self.chunker.getNumStripes()
        self.writers = []
        prefixes = (conf.chunkPrefix,
                    conf.chunkPrefix + 'FullOverlap',
                    conf.chunkPrefix + 'SelfOverlap')
        for i in xrange(ns):
            self.writers.append([])
            baseDir = os.path.join(conf.outputDir, "stripe_%d" % i)
            for j in xrange(self.chunker.getNumChunks(i)):
                # chunks are at least as wide as stripes are high, so j < ns*2
                chunkId = i * ns * 2 + j
                if self.chunkAcceptor and not self.chunkAcceptor(chunkId):
                    self.writers[i].append(None)
                else:
                    paths = [os.path.join(baseDir, p + ("_%d.csv" % chunkId))
                             for p in prefixes]
                    # creatin chunk overlap file, even if empty
                    self.writers[i].append(ChunkWriter(paths, chunkId, conf))

    def map(self, row):

        if (self.thetaColumn >= len(row)) :
            print "WRONG ROW: theta=%s row=%s" % (self.thetaColumn, row)
        elif (self.phiColumn >= len(row)):
            print "WRONG ROW: phi=%s row=%s" % (self.phiColumn, row)
        else:
            # extract position from row
            theta = float(row[self.thetaColumn])
            phi = float(row[self.phiColumn])
            # compute coordinates, ids, and sub-chunk bounds
            self.chunker.setCoords(theta, phi, self.coords)
            self.chunker.setBounds(self.coords, self.bounds)
            chunkId, subChunkId = self.chunker.getIds(self.coords)
            # Reject the unaccepted
            if self.chunkAcceptor and not self.chunkAcceptor(chunkId): return
            # write row plus the chunkId and subChunkId to the appropriate file
            writer = self.writers[self.coords[0]][self.coords[2]]
            self.cache.update(writer)
            r = self._combineChunkWithRow(row, [chunkId, subChunkId])
            writer.writerow(r, ChunkWriter.CHUNK)

            # write to overlap files
            overlap = self.chunker.getOverlap(theta, phi, self.coords,
                                              self.bounds)

            for ids, coords, both in overlap:
                w = self.writers[coords[0]][coords[1]]
                if not w: continue
                self.cache.update(w)
                r = self._combineChunkWithRow(row, ids)
                if both:
                    w.writerow(r, ChunkWriter.SELF_OVERLAP)
                w.writerow(r, ChunkWriter.FULL_OVERLAP)
            # Record sub-chunks containing at least one row
            if chunkId not in self.chunks:
                self.chunks[chunkId] = {subChunkId: 1}
            else:
                if subChunkId in self.chunks[chunkId]:
                    self.chunks[chunkId][subChunkId] += 1
                else:
                    self.chunks[chunkId][subChunkId] = 1

    def finish(self, conf):
        """Flush and close all open chunk files; return a list
        [(chunkId, {subChunkId, numRows})].
        """
        self.cache.close()
        return self.chunks.items()

    def _initRowCombiner(self, conf):
        if conf.chunkColumn and conf.chunkColumn >= 0:
            self.cIdx = conf.chunkColumn
            self._combineChunkWithRow = self._insertChunkInRow
        else:
            self._combineChunkWithRow = self._concatenateChunkToRow

    def _combineChunkWithRow(self, row, chunkAndSubChunkId):
        """Add chunkId and subChunkId to a row. Defaults to
        _concatChunkToRow(self, row, chunkAndSubChunkId)"""
        return self._concatenateChunkToRow(row, chunkAndSubChunkId)

    def _concatenateChunkToRow(self, row, chunkAndSubChunkId):
        """Add chunkId and subChunkId to a row by concatenating."""
        return row + chunkAndSubChunkId

    def _insertChunkInRow(self, row, chunkAndSubChunkId):
        """Add chunkId and subChunkId to a row by replacing existing
        columns in the row."""
        return row[:self.cIdx] + chunkAndSubChunkId + row[self.cIdx+2:]

def _dictMerge(d1, d2):
    for k in d2:
        if k in d1:
            d1[k] += d2[k]
        else:
            d1[k] = d2[k]
    d2.clear()
    return d1

class PartitionReducer(object):
    """Reducer that writes out partition metadata.
    """
    def __init__(self, conf):
        name = conf.chunkPrefix + "Partitions.csv"
        self.path = os.path.join(conf.outputDir, name)
        self.file = open(self.path, 'ab')
        self.buffer = sio.StringIO()

        self.writer = csv.writer(self.buffer, **_csvArgs(conf, mode='w'))
        self.chunker = Chunker(conf)
        self.coords = np.array([0, 0, 0, 0], dtype=np.int32)
        self.bounds = np.array([0.0, 0.0, 0.0, 0.0], dtype=np.float64)

    def reduce(self, key, values):
        histogram = reduce(_dictMerge, values)
        for subChunkId in histogram:
            self.chunker.setCoordsFromIds(key, subChunkId, self.coords)
            self.chunker.setBounds(self.coords, self.bounds)
            row = (key, subChunkId, histogram[subChunkId], self.bounds[0],
                   self.bounds[1], self.bounds[2], self.bounds[3],
                   self.chunker.overlap, self.chunker.alpha[self.coords[1]])
            self.writer.writerow(row)
        fcntl.lockf(self.file.fileno(), fcntl.LOCK_EX)
        try:
            self.file.write(self.buffer.getvalue())
            self.file.flush()
        finally:
            fcntl.lockf(self.file.fileno(), fcntl.LOCK_UN)
            self.buffer.truncate(0)

def chunk(conf, inputFiles):
    """Driver routine for standard spatial chunking.
    """
    splits = computeInputSplits(inputFiles, conf.inputSplitSize,
                                conf.skipLines)
    if conf.verbose:
        chunker = Chunker(conf)
        chunker.printConfig()
        print "Input splits:"
        for split in splits:
            print "\t%s" % split
    numWorkers = conf.numWorkers
    if numWorkers <= 0:
        numWorkers = None
    mapReduce(SpatialChunkMapper, PartitionReducer, conf, splits, numWorkers)


# -- Adaptive spatial chunking --------
#
# Motivation:
#
# Standard chunking operates in a purely spatial manner; direct control
# over the number of points in each sub-chunk is not available. Given the
# use of an O(N^2) spatial join algorithm on sub-chunks, relatively small
# differences in sub-chunk row count can result in significant per sub-chunk
# run time differences. Depending on the scale of density variation in the
# input and on the chunk to worker mapping, this may result in non-trivial
# execution time skew. To address this, the adaptive scheme attempts to
# create chunks and sub-chunks with uniform row counts.
#
# Naive strategy:
# 1. Compute histogram of point distribution over very fine spatial chunks
# 2. Determine a partition map from histogram (chunk/sub-chunk boundaries).
# 3. Bucket sort input according to partition map.
#
# The current map-reduce implementation keeps all map output in memory
# rather than storing it on disk and performing an external sort. In
# any case, the partition map is potentially large, which makes the
# naive strategy impractical for some inputs. Instead, the following
# strategy is employed:
#
# 1. Compute fine grained latitude histogram
# 2. Use latitude histogram to determine a set of stripes with
#    row counts that are a multiple of the target chunk row count;
#    polar stripes consist of one chunk and are sized accordingly.
# 3. Bucket sort input into stripes
# 4. For each non-polar stripe, compute fine grained longitude histogram.
# 5. Use longitude histogram to break stripes into chunks with uniform
#    row counts
# 6. Bucket sort stripes into chunks
# 7. Read in each chunk and sort on latitude angle; break the chunk into
#    sub-stripes containing a multiple of the target sub-chunk row count.
# 8. For each sub-stripe of each chunk, sort on longitude angle and break
#    sub-stripes into sub-chunks containing the desired number of rows;
#    the resulting partition map is written out simultaneously.
#
# This requires 5 reads and 3 writes (modulo overlap expansion) of the
# entire input dataset, so a slowdown of at least 4x relative to standard
# spatial chunking is expected. In practice the slowdown is much smaller,
# possibly because bucket sorting on chunks directly (rather than into stripes
# and then into chunks) can result in lots of small writes if the input is
# randomly distributed across the sky. In the adaptive case it is important
# to set the target chunk count such that a N chunks can fit entirely in
# memory, where N is the number of concurrent worker processes. Otherwise,
# swapping will sap performance.
#
# Problems and future work:
#
# If multiple tables need to be partitioned and spatially joined, then
# the partition map of a primary table could be used to partition the
# others. However, the primary table may be the only one to benefit from
# uniform partitioning. There is also the issue that if the spatial coverage
# of the tables differ, then the partition map emitted for the primary table
# may not cover some parts of the other tables. An algorithm that operates on
# the complete set of catalogs to be partitioned, and which produces
# sub-chunks with (say) no more than a per-catalog maximum number of rows per
# sub-chunk appears to be called for. This would be easier to accomplish using
# a heirarchical partitioning scheme instead of spherical coordinate boxes.
#
# For now, and for the reasons above, the adaptive scheme does not output
# full overlap information. Use standard spatial chunking for the multi-table
# case.

class Histogram1D(object):
    def __init__(self, binCounts, binSize):
        self.counts = reduce(np.add, binCounts)
        self.sums = np.cumsum(self.counts)
        self.size = binSize

    def estimateRows(self, x1, x2):
        """Returns an estimate of the number of rows between x1 and x2
        as a float.
        """
        b1,  b2  = x1 / self.size, x2 / self.size
        b1f, b2f = math.floor(b1), math.floor(b2)
        b1i, b2i = int(b1f), int(b2f)
        if b1i == b2i:
            return (b2 - b1) * self.counts[b1i]
        if b2i >= len(self.sums):
            b2i = len(self.sums) - 1
        f1 = 1.0 - (b1 - b1f)
        f2 = (b2 - b2f) - 1.0
        return (f1 * self.counts[b1i] + f2 * self.counts[b2i] +
                (self.sums[b2i] - self.sums[b1i]))

class Histogram1DMapper(object):
    """Mapper which computes a longitude or latitude angle histogram
    of an input split; map output is (0, numpy.array). An optional overlap
    column can be set; if it is, rows with an overlap value of '1' are
    ignored.
    """
    def __init__(self, conf, i):
        self.binSize = conf.binSize * DEG_PER_ARCSEC
        if hasattr(conf, 'overlapColumn'):
            self.overlapColumn = conf.overlapColumn
        else:
            self.overlapColumn = None
        if conf.direction == 'phi':
            self.column = conf.phiColumn
            self.angleOff = 90.0
            self.bins = np.empty(int(math.ceil(180.0 / self.binSize)),
                                 dtype=np.int64)
        else:
            self.column = conf.thetaColumn
            self.angleOff = 0.0
            self.bins = np.empty(int(math.ceil(360.0 / self.binSize)),
                                 dtype=np.int64)
        self.bins.fill(0)

    def map(self, row):
        if self.overlapColumn != None:
            if row[self.overlapColumn] == '1':
                return
        angle = float(row[self.column]) + self.angleOff
        i = int(math.floor(angle / self.binSize))
        if i == len(self.bins):
            i -= 1
        self.bins[i] += 1

    def finish(self, conf):
        return (0, self.bins)

def findSplit(hist1d, rangeMin, rangeMax, rowCount):
    """Return a split coordinate S such that the range
    [rangeMin, S] contains approximately rowCount rows.
    """
    if hist1d.estimateRows(rangeMin, rangeMax) <= rowCount:
        return rangeMax
    rmin, rmax = rangeMin, rangeMax
    while True:
        middle = 0.5 * (rmin + rmax)
        n = hist1d.estimateRows(rangeMin, middle)
        if abs(n - rowCount) < 0.5:
            return middle
        if n < rowCount: rmin = middle
        else: rmax = middle

def computePhiSplits(hist1d, phiMin, phiMax, rowCount):
    """Computes latitude angles corresponding to stripe bounds such
    that each stripe contains approximately a multiple of rowCount
    rows.
    """
    assert phiMin < phiMax and phiMin > 0.0 and phiMax < 180.0
    assert rowCount > 0
    results = [phiMax]
    stack = [(phiMin, phiMax)]
    while len(stack) != 0:
        bounds = stack.pop()
        n = hist1d.estimateRows(*bounds)
        if n <= rowCount:
            results.append(bounds[0])
        else:
            nc = int(round(n / rowCount))
            w = segmentWidth(bounds[0] - 90.0, bounds[1] - 90.0, nc)
            aspect = (bounds[1] - bounds[0]) / (w + EPSILON)
            if aspect < 2.5 or nc == 1:
                results.append(bounds[0])
            else:
                target = n/(2.0*rowCount)
                if target >= 1.0:
                    target = math.floor(target) * rowCount
                else:
                    target = rowCount
                phiSplit = findSplit(hist1d, bounds[0], bounds[1], target)
                stack.append((bounds[0], phiSplit))
                stack.append((phiSplit, bounds[1]))
    return results

def computeUniformSplits(hist1d, rangeMin, rangeMax, rowCount):
    """Computes split coordinates [s_0, s_1, ... ] such that each
    range [s_i, s_i+1) contains approximately rowCount rows.
    """
    results = [rangeMin]
    nsplits = int(round(float(hist1d.sums[-1]) / rowCount)) - 1
    for i in xrange(nsplits):
        rangeMin = findSplit(hist1d, rangeMin, rangeMax, rowCount)
        results.append(rangeMin)
        if rangeMin == rangeMax:
            return results
    results.append(rangeMax)
    return results

class StripeBucketSorter(object):
    """Mapper class that bucket sorts input CSV files into stripes. An
    overlap column is prepended to the input and set to '1' for records
    that fall in the stripe self-overlap regions.
    """
    def __init__(self, conf, i):
        self.stripes = conf.stripes
        self.overlap = conf.overlap
        self.phiColumn = conf.phiColumn
        self.cache = LRUCache(conf.maxOpenWriters)
        self.writers = []
        for j in xrange(len(self.stripes) - 1):
            p = os.path.join(conf.outputDir, ".stripe_%d.csv" % j)
            self.writers.append(CsvFileWriter(p, j, conf))

    def map(self, row):
        phi = float(row[self.phiColumn]) + 90.0
        i = self.stripes.searchsorted(phi)
        if i > 0 and phi < self.stripes[i]:
            i -= 1
        w = self.writers[i]
        self.cache.update(w)
        w.writerow(['0'] + row)
        if (i < len(self.writers) - 1 and
            self.stripes[i + 1] - phi <= self.overlap):
            # row is in self-overlap region of the stripe above
            w = self.writers[i + 1]
            self.cache.update(w)
            w.writerow(['1'] + row)

    def finish(self, conf):
        self.cache.close()

class ChunkBucketSorter(object):
    """Mapper class that bucket sorts CSV files into chunks. The value of the
    first column is set to '2' for records that fall in the chunk (but not the
    corresponding stripe) self-overlap regions.
    """
    def __init__(self, conf, i):
        self.chunks = conf.chunks
        s = conf.stripeId
        self.alpha = maxAlpha(conf.overlap,
                              max(abs(conf.stripes[s] - 90.0),
                                  abs(conf.stripes[s + 1] - 90.0)))
        self.thetaColumn = conf.thetaColumn
        self.cache = LRUCache(conf.maxOpenWriters)
        self.writers = []
        baseDir = os.path.join(conf.outputDir, "stripe_%d" % s)
        chunkIdOff = s << 16
        for j in xrange(len(self.chunks) - 1):
            chunkId = j + chunkIdOff
            p = os.path.join(baseDir, ".chunk_%d.csv" % chunkId)
            self.writers.append(CsvFileWriter(p, chunkId, conf))

    def map(self, row):
        theta = float(row[self.thetaColumn])
        i = self.chunks.searchsorted(theta)
        if i > 0 and theta < self.chunks[i]:
            i -= 1
        w = self.writers[i]
        self.cache.update(w)
        w.writerow(row)
        thetaMin, thetaMax = self.chunks[i:i+2]
        if row[0] == '1' and thetaMax - theta <= self.alpha:
            j = i + 1
            if j == len(self.writers):
                j = 0
            w = self.writers[j]
            self.cache.update(w)
            w.writerow(row)
        if theta - thetaMin < self.alpha:
            if row[0] == '0':
                row[0] = '2'
            w = self.writers[i - 1]
            self.cache.update(w)
            w.writerow(row)

    def finish(self, conf):
        self.cache.close()

def _lineOffsets(m):
    """Generator returning the starting offset of each line in m,
    which must be return value of a mmap.mmap() call. The last value
    yielded is the offset of the byte following the last byte in m.
    """
    yield m.tell()
    for line in MMapIter(m):
        yield m.tell()

def parseChunk(path, conf):
    """Read chunk and extract record positions and offsets. Returns a tuple
    (f, mem, records) where file is an open file object for path, mem is
    the result of memory-mapping the entire chunk file, and records is a
    numpy structured array containing (phi, theta, off, len, overlap) tuples
    for each record in the chunk.
    """
    f = open(path, 'rb')
    mem = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ)
    offsets = np.fromiter(_lineOffsets(mem), dtype=np.int64)
    records = np.empty(len(offsets) - 1, dtype=np.dtype([
        ('phi', np.float64), ('theta', np.float64),
        ('offset', np.int64), ('length', np.int32), ('overlap', np.int8)]))
    # Read through again, hopefully not hitting disk this time, and
    # create an array of (theta, phi, off, len, overlap) records.
    mem.seek(0)
    reader = csv.reader(MMapIter(mem), _csvArgs(conf))
    thetaColumn = conf.thetaColumn
    phiColumn = conf.phiColumn
    line = reader.line_num
    for i, row in enumerate(reader):
        records[i][0] = float(row[phiColumn])
        records[i][1] = float(row[thetaColumn])
        # skip the overlap column prepended by StripeBucketSorter
        saveoff = mem.tell()
        mem.seek(offsets[line])
        while mem.read_byte() != conf.delimiter: pass
        roff = mem.tell()
        mem.seek(saveoff)
        records[i][2] = roff
        records[i][3] = offsets[reader.line_num] - roff - 1
        assert records[i][3] > 0
        records[i][4] = int(row[0])
        line = reader.line_num
    records = np.resize(records, i)
    return (f, mem, records)

def computeSubStripes(recs, nrows, phiMax):
    """Compute sub-stripes for a chunk. Return a tuple
    (A, S, P) where A is a numpy array of indexes listing the elements of
    recs in ascending latitude angle order, S is a list of sub-stripe
    starting indexes in A, and P is a list of sub-stripe minimum latitude
    angles.
    """
    a = recs.argsort(order='phi')
    # Don't include overlap records in the partitioning decisions
    counts = (recs['overlap'][a] == 0).cumsum()
    # The chunk may not have an exact multiple of nrows rows; adjust nrows
    # to produce more uniformly sized sub-chunks
    nrows = counts[-1] / round(float(counts[-1]) / nrows)
    # Scan for the index of the last record with overlap != 1
    for i in reversed(xrange(len(a))):
        if recs[a[i]][4] != 1:
            break
    if i == -1:
        # chunk contains no non-overlap records
        return None, None, None
    # Determine sub-stripe bounds
    iMin = 0
    iMax = i + 1
    s = []
    stack = [(iMin, iMax)]
    while len(stack) != 0:
        r = stack.pop()
        if r[0] > 0:
            countOff = counts[r[0] - 1]
        else:
            countOff = 0
        n = counts[r[1] - 1] - countOff
        if n <= nrows + 1:
            s.append(r[0])
        else:
            nc = int(round(float(n) / nrows))
            phiMin = recs[a[r[0]]][0]
            phiMax = recs[a[r[1] - 1]][0]
            w = segmentWidth(phiMin, phiMax, nc)
            aspect = (phiMax - phiMin) / (w + EPSILON)
            if aspect < 2.0 or nc == 1:
                s.append(r[0])
            else:
                target = float(n) / (2.0 * nrows)
                if target >= 1.0:
                    target = int(math.floor(target) * nrows)
                else:
                    target = int(round(nrows))
                t = counts[r[0]:r[1]].searchsorted(target + countOff) + r[0]
                phi = recs[a[t]][0]
                # expand partition until a distinct
                # latitude angle is found
                while t < r[1] - 1:
                    if recs[a[t + 1]][0] != phi:
                        break
                    t += 1
                if t > r[0] and t < r[1] - 1:
                    stack.append((t, r[1]))
                    stack.append((r[0], t))
                else:
                    s.append(r[0])
    s.append(iMax) # s is produced in sorted order
    sp = np.empty(len(s), dtype=np.float64)
    sp[:-1] = recs['phi'][a[s[:-1]]]
    sp[-1] = phiMax
    return (a, s, sp)

class SubChunker(object):
    """Class for sub-partitioning a single chunk file.
    """
    def __init__(self, chunkId, thetaMax, phiMax, path, conf):
        self.chunkId = chunkId
        self.thetaMax = thetaMax
        self.phiMax = phiMax
        self.overlap = conf.overlap
        self.rowsPerSubChunk = conf.rowsPerSubChunk
        self.delimiter = conf.delimiter_out
        self.file, self.mem, self.records = parseChunk(path, conf)
        prefixes = (conf.chunkPrefix, conf.chunkPrefix + 'SelfOverlap')
        d = os.path.dirname(path)
        paths = [os.path.join(d, p + ("_%d.csv" % chunkId)) for p in prefixes]
        self.outFiles = [open(p, 'wb') for p in paths]
        partFile = os.path.join(conf.outputDir,
                                conf.chunkPrefix + 'Partitions.csv')
        self.partitionWriter = CsvFileWriter(partFile, chunkId, conf)
        self.buf = sio.StringIO()
        self.idWriter = csv.writer(self.buf, _csvArgs(conf, mode='w'))

    def _writeRow(self, which, subChunkId, record):
        self.idWriter.writerow((self.chunkId, subChunkId))
        self.mem.seek(record[2])
        self.outFiles[which].write(self.mem.read(record[3]))
        self.outFiles[which].write(self.delimiter)
        self.outFiles[which].write(self.buf.getvalue())
        self.buf.truncate(0)

    def subPartition(self):
        """Compute and write out sub-chunk boundaries, write out chunk
        sorted by sub-chunk id, and write out self-overlap entries.
        """
        arr, ss, ssPhi = computeSubStripes(self.records,
                                           self.rowsPerSubChunk, self.phiMax)
        if arr == None:
            return
        sc = []
        alpha = []
        # Loop over sub-stripes, sort on theta, and write out
        # sub-chunks and sub-chunk metadata.
        for i in xrange(len(ss) - 1):
            iMin, iMax = ss[i], ss[i + 1]
            # The sub-stripe may not contain an exact multiple of
            # self.rowsPerSubChunk rows; adjust to produce more uniformly
            # sized sub-chunks.
            rpsc = (iMax - iMin) / round(float(iMax - iMin) /
                                         self.rowsPerSubChunk)
            scRows = rpsc
            phiMin, phiMax = ssPhi[i], ssPhi[i + 1]
            alpha.append(maxAlpha(self.overlap, max(abs(phiMin), abs(phiMax))))
            subarr = arr[iMin:iMax]
            subarr = subarr[self.records[subarr].argsort(order='theta')]
            arr[iMin:iMax] = subarr
            r = self.records[subarr]
            for cMax in reversed(xrange(len(r))):
                if r[cMax][4] == 0: break
            lc = 0
            scId = i << 16
            b = [r[0][1]]
            for j in xrange(len(r)):
                if j == cMax:
                    if len(b) > 0x10000:
                        raise RuntimeError(dedent("""\
                            More than 65536 sub-chunks generated in
                            sub-stripe: use coarser partitioning!"""))
                    # Write out last sub-chunk to the partition map
                    b.append(self.thetaMax)
                    self.partitionWriter.writerow((self.chunkId, scId,
                        j - lc + 1, b[-2], b[-1], phiMin, phiMax,
                        self.overlap, alpha[-1]))
                    scRows = len(r)
                if r[j][4] == 0:
                    if (j > scRows and r[j][1] != r[j - 1][1]):
                        if len(b) > 0x10000:
                            raise RuntimeError(dedent("""\
                                More than 65536 sub-chunks generated in
                                sub-stripe: use coarser partitioning!"""))
                        # Write out a sub-chunk to the partition map
                        b.append(r[j][1])
                        self.partitionWriter.writerow((self.chunkId, scId,
                            j - lc, b[-2], b[-1], phiMin, phiMax,
                            self.overlap, alpha[-1]))
                        lc = j
                        scRows += rpsc
                        scId += 1
                    # Write row to current chunk
                    self._writeRow(0, scId, r[j])
            sc.append(np.array(b, dtype=np.float64))

        # Sub-chunk boundaries are now known - handle overlap
        for i in xrange(len(ss) - 1):
            iMin, iMax = ss[i], ss[i + 1]
            phiMin, phiMax = ssPhi[i], ssPhi[i + 1]
            r = self.records[arr[iMin:iMax]]
            scId = i << 16
            b = sc[i]
            a = alpha[i]
            for j in xrange(len(r)):
                phi = r[j][0]
                theta = r[j][1]
                if r[j][4] != 1:
                    # Self-overlap for sub-chunks to the left
                    cc = b.searchsorted(theta)
                    if cc == len(b) or (cc > 0 and theta < b[cc]):
                        cc -= 1
                    for c in chain(reversed(xrange(cc)),
                                   reversed(xrange(cc + 1, len(b) - 1))):
                        if minDeltaTheta(theta, b[c + 1]) > a:
                            break
                        self._writeRow(1, scId + c, r[j])
                # Self-overlap for sub-stripes above
                tmin, tmax = theta - a, theta + a
                if tmin < 0.0: tmin += 360.0
                if tmax >= 360.0: tmax -= 360.0
                for s in xrange(i + 1, len(ss) - 1):
                    if ssPhi[s] - ssPhi[i + 1] > self.overlap:
                        break
                    if ssPhi[s] - phi <= self.overlap:
                        # Find sub-chunks in sub-stripe s that contain
                        # theta in their self-overlap regions.
                        cmin = sc[s].searchsorted(tmin)
                        cmax = sc[s].searchsorted(tmax)
                        l = len(sc[s])
                        if cmin == l or (cmin > 0 and tmin < sc[s][cmin]):
                            cmin -= 1
                        if cmax == l or (cmax > 0 and tmax < sc[s][cmax]):
                            cmax -= 1
                        if cmin > cmax:
                            itr = chain(xrange(cmin, l), xrange(cmax + 1))
                        else:
                            itr = xrange(cmin, cmax + 1)
                        ssId = s << 16
                        for c in itr:
                            self._writeRow(1, ssId + c, r[j])

    def close(self):
        """Release system resources acquired by this SubChunker.
        """
        self.mem.close()
        self.file.close()
        del self.records
        for f in self.outFiles:
            f.flush()
            f.close()
        self.partitionWriter.close()
        self.buf.close()

def subPartitionChunk(args):
    chunkId, chunkMax, path, conf = args
    if not os.path.exists(path):
        # This chunk file was never created (i.e. was empty)
        return
    sc = SubChunker(chunkId, chunkMax[0], chunkMax[1], path, conf)
    try:
        sc.subPartition()
    finally:
        sc.close()

# If no points are contained within POLE_RADIUS deg of a pole,
# polar stripes are omitted.
POLE_RADIUS = 1.0

def chunkAdaptively(conf, inputFiles):
    """Driver routine for adaptive spatial chunking.
    """
    splits = computeInputSplits(inputFiles, conf.inputSplitSize,
                                conf.skipLines)
    if conf.verbose:
        print "Number of chunks: %d" % conf.numChunks
        print "Target sub-chunk row count: %d" % conf.rowsPerSubChunk
        print "Input splits:"
        for split in splits:
            print "\t%s" % split
    numWorkers = conf.numWorkers
    if numWorkers <= 0:
        numWorkers = None

    # 1. Compute latitude histogram
    #    Performs 1 read of all input
    if conf.verbose:
        print "==== Computing latitude histogram..."
    conf.direction = 'phi'
    conf.overlapColumn = None
    results = mapReduce(Histogram1DMapper, None, conf, splits, numWorkers)
    binSize = conf.binSize * DEG_PER_ARCSEC
    hist = Histogram1D(results[0][1], binSize)
    results = None
    numRows = float(hist.sums[-1])
    if numRows == 0:
        print "Input contains no rows!"
        sys.exit(1)
    rowsPerChunk = numRows / conf.numChunks

    # 2. Determine stripe boundaries
    if conf.verbose:
        print "==== Determining stripe boundaries..."
    stripes = []
    # south pole stripe
    for i in xrange(len(hist.counts)):
        if hist.counts[i] != 0: break
    phiMin = i * binSize
    if phiMin < POLE_RADIUS:
        stripes.append(0.0)
        phiMin = findSplit(hist, 0.0, 180.0, rowsPerChunk)
    # north pole stripe
    for i in reversed(xrange(len(hist.counts))):
        if hist.counts[i] != 0: break
    phiMax = (i + 1) * binSize
    if 180.0 - phiMax < POLE_RADIUS:
        stripes.append(180.0 + EPSILON)
        phiMax = findSplit(hist, 0.0, 180.0, numRows - rowsPerChunk)
    stripes += computePhiSplits(hist, phiMin, phiMax, rowsPerChunk)
    stripes = np.array(stripes, dtype=np.float64)
    stripes.sort()
    conf.stripes = stripes

    # 3. Bucket sort input into stripes
    #    Performs 1 read and 1 write of all input
    if conf.verbose:
        print "==== Bucket sorting into stripes:"
        print stripes
    mapReduce(StripeBucketSorter, None, conf, splits, numWorkers)
    conf.overlapColumn = 0 # prepended by StripeBucketSorter
    conf.phiColumn += 1
    conf.thetaColumn += 1

    chunkIds = []
    chunkMax = []
    chunkPaths = []
    # 4. Compute per stripe longitude histograms
    #    Performs 1 read of all input
    conf.direction = 'theta'
    mins = 0
    maxs = len(stripes) - 1
    if stripes[mins] == 0.0:
        # No need to compute longitude histogram and chunk up the
        # south pole stripe
        d = os.path.join(conf.outputDir, "stripe_0")
        if not os.path.exists(d):
            os.makedirs(d)
        chunkIds.append(0)
        chunkPaths.append(os.path.join(d, ".chunk_0.csv"))
        chunkMax.append((360.0, stripes[1] - 90.0))
        os.rename(os.path.join(conf.outputDir, ".stripe_0.csv"),
                  chunkPaths[-1])
        mins += 1
    if stripes[maxs] >= 180.0:
        # No need to compute latitude histogram and chunk up the
        # north pole stripe
        maxs -= 1
        d = os.path.join(conf.outputDir, "stripe_%d" % maxs)
        if not os.path.exists(d):
            os.makedirs(d)
        chunkIds.append(maxs << 16)
        chunkPaths.append(os.path.join(d, ".chunk_%d.csv" % chunkIds[-1]))
        chunkMax.append((360.0, stripes[maxs + 1] - 90.0))
        os.rename(os.path.join(conf.outputDir, ".stripe_%d.csv" % maxs),
                  chunkPaths[-1])

    # Iterate over non-polar stripes.
    #
    # Parallelizing this loop by assigning each stripe to a map task would
    # be better for performance (see Amdahl), especially so if stripes end
    # up being relatively small. However, that would require generalizing
    # the way inputs are fed to mappers. For now, make the input split size
    # smaller in an attempt to recover parallelism.
    for stripeId in xrange(mins, maxs):
        conf.stripeId = stripeId
        splitSize = conf.inputSplitSize / min(maxs - mins, 32)
        stripeDir = os.path.join(conf.outputDir, "stripe_%d" % stripeId)
        stripePath = os.path.join(conf.outputDir,
                                  ".stripe_%d.csv" % stripeId)
        splits = computeInputSplits([stripePath], splitSize, 0)
        if conf.verbose:
            print "== Computing longitude histogram for stripe %d [%f,%f)" % (
                  stripeId, stripes[stripeId] - 90.0,
                  stripes[stripeId + 1] - 90.0)
        results = mapReduce(Histogram1DMapper, None, conf, splits, numWorkers)
        hist = Histogram1D(results[0][1], binSize)

        # 5. Determine chunk boundaries
        if conf.verbose:
            print "== Determining chunk boundaries for stripe ..."
        for i in xrange(len(hist.counts)):
            if hist.counts[i] != 0: break
        thetaMin = i * binSize
        for i in reversed(xrange(len(hist.counts))):
            if hist.counts[i] != 0: break
        thetaMax = (i + 1) * binSize
        chunks = computeUniformSplits(hist, thetaMin, thetaMax, rowsPerChunk)
        if len(chunks) > 65536:
            # chunkId generation scheme breaks with this many chunks
            print dedent("""\
                         Sorry - stripe contains more than 65536 chunks;
                         decrease --num-chunks and try again.""" % (
                         stripes[stripeId], stripes[stripeId + 1]))
            sys.exit(1)
        i = stripeId << 16
        chunkIds += [i + j for j in xrange(len(chunks) - 1)]
        chunkPaths += [os.path.join(stripeDir, ".chunk_%d.csv" % (i + j))
                       for j in xrange(len(chunks) - 1)]
        chunkMax += zip(chunks[1:], repeat(stripes[stripeId + 1] - 90.0))
        chunks = np.array(chunks, dtype=np.float64)
        chunks.sort()
        conf.chunks = chunks
        # 6. Bucket sort stripes into chunks
        #    Performs 1 read and 1 write of all input
        if conf.verbose:
            print "== Bucket sorting stripe into chunks:"
            print chunks
        mapReduce(ChunkBucketSorter, None, conf, splits, numWorkers)
        # Remove stripe file to economize on disk space
        os.remove(stripePath)

    # 7. and 8. Memory map each chunk, sort on latitude angle, determine
    # sub-stripe bounds, sort sub-stripes on longitude angle and determine
    # sub-chunk bounds. This could also be cast as a map task if map input
    # were generalized.
    if conf.verbose:
        print "==== Sub-partitioning chunks..."
    if _have_mp and numWorkers != 1 and len(chunkPaths) != 1:
        poolType = mp.Pool
    else:
        poolType = SerialPool
    pool = poolType(numWorkers)
    try:
        t = time.time()
        pool.map(subPartitionChunk, izip(chunkIds, chunkMax,
                                         chunkPaths, repeat(conf)))
        if conf.verbose:
            print "sub-partitioning finished in %f sec" % (time.time() - t)
    finally:
        pool.close()
    # Remove intermediate chunk files created during processing
    if conf.verbose:
        print "==== Removing intermediate chunk files..."
    t = time.time()
    map(os.remove,
        glob(os.path.join(conf.outputDir, "stripe_*", ".chunk_*.csv")))
    if conf.verbose:
        print "removed intermediate chunk files in %f sec" % (time.time() - t)


# -- Command line interface --------
def makeGeneralOptGroup(parser):
    # General options
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
        "--chunk-column", type="int", dest="chunkColumn", default=None,
        help=dedent("""\
        0-based index of existing chunkId and subChunkId columns in the
        input data. If unspecified (default), concatenate chunkId and
        subChunkId columns at the end of the existing set. If specified,
        the column and the next adjacent column are assumed to be chunkId
        and subChunkId columns that the partitioner should overwrite.
        (PT1.1: 225)."""))
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
    return general


def makeChunkingOptGroup(parser):
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
    return chunking

def addChunkingOpts(parser):
    chunking = makeChunkingOptGroup(parser)
    parser.add_option_group(chunking)

def addAdaptiveOpts(parser):
    # Adaptive chunking options
    adaptive = optparse.OptionGroup(parser, "Adaptive chunking options")
    adaptive.add_option(
        "-C", "--num-chunks", type="long", dest="numChunks",
        help=dedent("""\
        Target number of chunks; implies adaptive chunking.
        If specified, all standard chunking options are ignored."""))
    adaptive.add_option(
        "-r", "--rows-per-subchunk", type="long", default=256,
        dest="rowsPerSubChunk", help=dedent("""\
        Target row count per sub-chunk; defaults to %default. Ignored if
        standard chunking is being used."""))
    adaptive.add_option(
        "-B", "--bin-size", type="float", default=1.0,
        dest="binSize", help=dedent("""\
        Bin size in arcsec for the histogram used to determine chunk/sub-chunk
        boundaries. The defaults is %default arcsec. Ignored if standard
        chunking is being used."""))
    parser.add_option_group(adaptive)

def addCsvOpts(parser):
    # CSV format options
    fmt = optparse.OptionGroup(
        parser, "CSV format options", dedent("""\
        See http://docs.python.org/library/csv.html#csv-fmt-params for
        details."""))
    fmt.add_option(
        "-D", "--delimiter", type="string", dest="delimiter", default=",",
        help=dedent("""\
        One character string used to separate fields in the
        input TSV, CSV files. The default is %default."""))
    fmt.add_option(
        "-E", "--delimiter-out", type="string", dest="delimiter_out", default=",",
        help=dedent("""\
        One character string used to separate fields in the
        output CSV files. The default is %default."""))
    fmt.add_option(
        "-n", "--no-doublequote", dest="doublequote", action="store_false",
        help=dedent("""\
        Turn off double quoting of quote characters inside a CSV field."""))
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
        One character string to quote fields with; defaults to %default."""))
    fmt.add_option(
        "-I", "--skipinitialspace", dest="skipinitialspace",
        action="store_true", help=dedent("""\
        Ignore whitespace immediately following delimiters."""))
    parser.add_option_group(fmt)

def addTuningOpts(parser):
    # Tuning options
    tuning = optparse.OptionGroup(parser, "Tuning options")
    tuning.add_option(
        "-b", "--output-buffer-size", type="float", default=1.0,
        dest="outputBufferSize", help=dedent("""\
        Size in MiB of in-memory output buffers. This is approximately the
        granularity at which file writes occur; fractional values are allowed.
        The default is %default."""))
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

def check_char(option, opt, value):
    try:
        return value.decode('string-escape')
        #return value
    except ValueError:
        raise OptionValueError(
            "option %s: invalid char value: %r" % (opt, value))

class CharOption (optparse.Option):
    TYPES = optparse.Option.TYPES + ("char",)
    TYPE_CHECKER = copy(optparse.Option.TYPE_CHECKER)
    TYPE_CHECKER["char"] = check_char

def main():
    # Command line parsing/usage
    t = time.time()

    usage = "usage: %prog [options] input_1 input_2 ..."
    parser = optparse.OptionParser(usage, option_class=CharOption)

    def explainArgs(option,opt,value,parser):
        conf = parser.values
        if conf.numChunks:
            print "Adaptive chunking with", conf.numChunks, "chunks."
        else:
            print "Fixed spatial chunking:"
            c = Chunker(conf)
            c.printConfig()
        print "Overlap:", conf.overlap, "deg (%d min)" %(conf.overlap * 60)
        pass

    general = makeGeneralOptGroup(parser)
    general.add_option(
        "--explain", action="callback", callback=explainArgs,
        help="Print current understanding of options and parameters")
    addChunkingOpts(parser)
    addAdaptiveOpts(parser)
    addCsvOpts(parser)
    addTuningOpts(parser)


    (conf, inputs) = parser.parse_args()

    # Input validation
    if len(inputs) == 0:
        parser.error("At least one input file must be specified")
    if conf.thetaColumn < 0:
        parser.error("Negative longitude angle column index.")
    if conf.phiColumn < 0:
        parser.error("Negative latitude angle column index.")
    if not os.path.isdir(conf.outputDir):
        parser.error(dedent("""\
            Specified output directory does not exist or is not a
            directory."""))
    if len(conf.chunkPrefix) == 0 or conf.chunkPrefix[0] == '.':
        parser.error("Missing or illegal chunk prefix.")
    if conf.outputBufferSize < 1.0 / 1024 or conf.outputBufferSize > 64.0:
        parser.error(dedent("""\
            Output buffer size must be at least 1KiB and no more than
            64MiB."""))
    conf.outputBufferSize = int(conf.outputBufferSize * 1048576.0)
    if conf.inputSplitSize > 256.0:
        parser.error("Input split size must not exceed 256 MiB.")
    conf.inputSplitSize = int(conf.inputSplitSize * 1048576.0)
    if conf.maxOpenWriters < 1:
        parser.error("Maximum open chunk writer cap must be at least 1.")
    if conf.skipLines < 0:
        parser.error("Negative line skip count.")
    if len(conf.delimiter) > 1 or re.match(r'[0-9a-zA-Z]', conf.delimiter):
        parser.error("Illegal CSV field delimiter for input files : %s" % conf.delimiter)
    if len(conf.delimiter_out) > 1 or re.match(r'[0-9a-zA-Z]', conf.delimiter_out):
        parser.error("Illegal CSV field delimiter for output files : %s" % conf.delimiter_out)

    if len(conf.quotechar) > 1 or conf.delimiter == conf.quotechar:
        parser.error("Illegal CSV field quote character.")

    if conf.numChunks:
        # Adaptive chunking
        if conf.numChunks < 10:
            parser.error("Target number of chunks must be >= 10.")
        if conf.rowsPerSubChunk < 1:
            parser.error("Target sub-chunk row count must be positive.")
        if conf.binSize < 0.0001:
            parser.error("Histogram bin size must be at least 1 milliarcsec.")
        chunkAdaptively(conf, inputs)
    else:
        # Standard chunking
        if conf.numStripes < 1 or conf.numSubStripes < 1:
            parser.error(dedent("""\
                Number of stripes and sub-stripes per stripe must be
                positive"""))
        if conf.overlap < 0.0 or conf.overlap > 10.0:
            parser.error(
                "Invalid overlap distance; specify a value in [0, 10] deg")
        stripeHeight = 180.0 / conf.numStripes
        subStripeHeight = 180.0 / (conf.numStripes * conf.numSubStripes)
        if subStripeHeight < conf.overlap:
            parser.error(dedent("""\
                The height of a single sub-stripe is less than the overlap
                radius; please use coarser chunking parameters"""))
        if subStripeHeight <= 1*DEG_PER_ARCSEC:
            parser.error(dedent("""\
                The height of a single sub-stripe must be 1 arcsec or
                greater"""))
        chunk(conf, inputs)
    if conf.verbose:
        print "Total time: %f sec" % (time.time() - t)

if __name__ == "__main__":
    main()
