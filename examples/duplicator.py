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

# duplicator.py

## Python
import csv
from itertools import izip
import math
import random
import string
import time

## Local
import partition

## Tools
def computeHeaderDict(headerRow):
    return dict(zip(headerRow, range(len(headerRow))))

def stretchFactor(phiOldRad, phiNew):
    if phiNew < -90.0: return 1e5 # Saturate at some number < Inf
    if phiNew > 90.0: return 1e5 # Saturate at some number < Inf
    b =  math.cos(phiOldRad) / math.cos(math.radians(phiNew))
    return b
    
# Normalization function (only normalizes by one period)
def normalize(low, high, identity, val):
    if val < low: return val + identity
    if val > high: return val - identity
    return val

def transformThetaPhi(thetaPhi, thetaPhiOff, norm=True):
    (theta, phi) = thetaPhi
    (thetaOff, phiOff) = thetaPhiOff
    thetaRaw = theta + thetaOff
    phiRaw = phi + phiOff
    if norm:
        return [normalize(0.0, 360, 360, 
                          thetaRaw * stretchFactor(math.radians(phi), 
                                                       phiRaw)),
                normalize(-90.0, 90.0, 180, phiRaw)]
    else:
        return [thetaRaw * stretchFactor(math.radians(phi), phiRaw), phiRaw]
    pass

def translateThetaPhiCorrected(thetaPhi, thetaPhiOff, thetaMid, phiRef, 
                               norm=True):
    """ Translate a source point on a spherical surface to another location
    specified by offsets.  Correct stretching using the theta midpoint and
    the reference phi in the source input patch.  The reference phi should 
    be chosen as the phiStart when the output patch is mostly below the 
    equator, and phiEnd when the patch is mostly above the equator.

    The correction acts to re-center the output patch so that the 
    coordinates pole-bound are not smeared off-center.

    @param thetaPhi [theta, phi] position in the source patch 
    @param thetaPhiOff [thetaOff, phiOff] offset of the relocated patch 
    @param thetaMid avg(thetaRight, thetaLeft) in the original patch
    @param phiRef Either phiLower or phiUpper, as described above.

    thetaRight, thetaLeft, phiLower, phiUpper describe the input patch 
    bounding box, where thetaRight < thetaLeft and phiLower < phiUpper.

    All parameters are in degrees.
    """
    phiRaw = thetaPhi[1] + thetaPhiOff[1]
    stretch = stretchFactor(math.radians(thetaPhi[1]), phiRaw)
    cStretch = stretchFactor(math.radians(phiRef), 
                             phiRef + thetaPhiOff[1])
    theta = (thetaPhi[0] - thetaMid) * stretch + ((thetaMid + thetaPhiOff[0])
                                                  * cStretch)
    if norm:
        return [normalize(0, 360, 360, theta),
                normalize(-90, 90, 180, phiRaw)]
    else:
        return [theta, phiRaw]

## Classes
class ChunkBounds:
    def __init__(self, chunkId, bounds):
        self.chunkId = chunkId
        self.bounds = bounds[:]
        pass
        
    def __str__(self):
        return "%d : %s" % (self.chunkId, str(self.bounds))
    def __repr__(self):
        return "ChunkBounds(%s)" % self.__str__()

class DefaultConf:
    @staticmethod
    def getPartitionDef():
        dc = DefaultConf()
        dc.thetaColumn = 0
        dc.phiColumn = 1
        dc.overlap = 1/60.0
        dc.numStripes = 18
        dc.numSubStripes = 100
        dc.chunkPrefix = "Test"
        dc.outputDir = "/tmp/"
        # CSV params
        dc.delimiter = ","
        dc.doublequote = True
        dc.escapechar = '\\'
        dc.quoting = csv.QUOTE_MINIMAL
        dc.quotechar = '"'
        dc.skipinitialspace = True
        # Tuning
        dc.outputBufferSize = 1.0
        dc.maxOpenWriters = 32
        dc.inputSplitSize = 64.0
        dc.debug = False
        return dc

    @staticmethod
    def getDuplicationDef():
        dc = DefaultConf()
        dc.bounds = "-2.0221828620000224,5.21559213586,-6.80690075667,7.11656414672"
        return dc

class PartitionDef:
    def __init__(self, conf):
        s = partition.SpatialChunkMapper(conf,int(time.time()))
        self.partitionStripes = self._extractPartitionBounds(s.writers, 
                                                             s.chunker)
        
    def _extractPartitionBounds(self, writers, chunker):
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
                #print w.chunkId, bounds
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
        return stripes

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


    def printPartBounds(self, plottable=False):
        for s in self.partitionStripes:
            if plottable:
                phi = s[0].bounds[2:4]
                print "ChunkPlot %f %f" % (phi[0], phi[1]),
                for c in s:
                    print "%d %.2f %.2f" % (c.chunkId, 
                                            c.bounds[0], c.bounds[1]),
                print
            else:
                print "\n".join(map(str, s))
            #a += 1
            #if a > 2: break


class DuplicationDef:
    def __init__(self, conf):
        self._importConf(conf)
        self._layoutDupeMap()
        pass

    def _importConf(self, conf):
        bstr = conf.bounds
        if bstr:
            bounds = map(float, bstr.split(","))
        else:
            raise StandardError("Faulty input bounding specification: %s"
                                % bstr)
        self.bounds = bounds

    def _layoutDupeMap(self):        
        bounds = self.bounds
        thetaSize = bounds[1] - bounds[0]
        phiSize = bounds[3] - bounds[2]
        
        phiOffsetUnits = int(math.ceil(90.0 / phiSize))
        thetaOffsetUnits = int(math.ceil(360.0 / thetaSize))
        
        boundsRad = map(math.radians, bounds)
        def stretchMin(phi): return stretchFactor(boundsRad[2], phi)
        def stretchMax(phi): return stretchFactor(boundsRad[3], phi)

        stripes = {}
        phiIndex = []
        phiLast = (-phiOffsetUnits * phiSize) + bounds[2]
        thetaIndices = []
        copyNum = 1
        # Phi stripes
        # This range of offset units assumes an equal number of 
        # pos offset stripes and neg offset stripes
        for i in range(-phiOffsetUnits, phiOffsetUnits, 1):
            stripe = []
            thetaIndex = []
            phiOffset = i * phiSize # phi has no stretching factor
            (phiMin, phiMax) = (phiLast, bounds[3] + phiOffset)
            if (phiMax < -90.0) or (phiMin > 90.0): 
                phiLast = phiMax
                continue
            phiMid = 0.5 * (phiMin + phiMax) 
            
            # theta blocks in a stripe
            phiPositive = (phiMid > 0)
            if phiPositive:
                stretch = stretchMin(phiMin)
                phiRef = bounds[2]
            else:
                stretch = stretchMax(phiMax)
                phiRef = bounds[3]
            thetaStart = stretch * bounds[0]
            if False: # Debug
                compareMin = bounds[0] * stretchFactor(math.radians(bounds[2]), 
                                                       phiMin)
                compareMax = bounds[0] * stretchFactor(math.radians(bounds[3]), 
                                                       phiMax)
                pn = transformThetaPhi([bounds[0],bounds[2]], [0,phiOffset], False)
                px = transformThetaPhi([bounds[0],bounds[3]], [0,phiOffset], False)        
                print "ThetaStart", thetaStart, compareMin, compareMax, pn, px

            thetaEnd = 360 + thetaStart 
            thetaLast = thetaStart
            for j in range(0, thetaOffsetUnits):
                # Constant RA sides
                thetaNext = thetaStart + ((j+1)  * thetaSize * stretch)
                if False: # Debug
                    thetaOff = (j+1) * thetaSize
                    thetaNextRaw = (bounds[0] + ((j+1)*thetaSize))
                    compareMin = thetaNextRaw * stretchFactor(
                        math.radians(bounds[2]), phiMin)
                    compareMax = thetaNextRaw * stretchFactor(
                        math.radians(bounds[3]), phiMax)
                    pn = transformThetaPhi([bounds[0],bounds[2]], 
                                           [thetaOff,phiOffset], False)
                    px = transformThetaPhi([bounds[0],bounds[3]], 
                                           [thetaOff,phiOffset], False)        
                    print "ThetaN", thetaNext, compareMin, compareMax, pn, px

                if thetaNext > thetaEnd:
                    thetaNext = thetaEnd
                thetaIndex.append(thetaLast)
                #print "Copy (%d,%d) : theta (%f, %f), phi (%f, %f) " % (
                #    j, i, thetaMin, thetaMax, phiMin, phiMax)
                stripe.append([[j, i], # Offset in units
                               copyNum, # ordinal number
                               [thetaLast, thetaNext, phiMin, phiMax], # bounds
                               [j*thetaSize, phiOffset], # Offset in deg
                               phiRef]) # phi reference
                copyNum += 1
                if thetaNext == thetaEnd: break
                thetaLast = thetaNext

            thetaIndex.append(thetaLast) 
            thetaIndices.append(thetaIndex)
            phiIndex.append(phiMin)
            phiLast = phiMax
            #print "Iterate"
            stripes[i] = stripe
        phiIndex.append(phiLast)
        # prep
        self.stripes = stripes
        #print "Phi index:", phiIndex
        self.phiIndex = phiIndex
        self.thetaIndices = thetaIndices
        self.dupeCount = sum(map(lambda l: len(l)-1, thetaIndices))
        #for s in thetaIndices:
        #    print " ".join(map(lambda c:  "%2.1f" % c, s))
        return stripes

    def printCopyInfo(self, plottable=False):
        stripes = self.stripes
        copyCount = 0
        for i in sorted(stripes.keys()):
            copyCount += len(stripes[i])
            first = stripes[i][0][2]
            lastTheta = first[0]
            phi = first[2:4]
            if plottable:
                print "DupePlot %f %f" % (phi[0], phi[1]),
            else:
                print "phi: %f %f (%f,%d)" % (phi[0], phi[1], 
                                              stretchFactor(0,sum(phi)/2.0),
                                              len(stripes[i])),
                print "%.2f" % lastTheta,
            for dupe in stripes[i]:
                b = dupe[2]
                if plottable:
                    print "%d %.2f %.2f" % (dupe[1], b[0], b[1]),
                else:
                    print "%.2f" % b[1],
                assert b[2] == phi[0]
                assert b[3] == phi[1]
                assert b[0] == lastTheta
                lastTheta = b[1]
            print ""
        print copyCount, "duplicates"
        
    
    def findEnclosing(self, index, lower, upper):
        first = -1
        last = -1
        for i in range(len(index)-1):
            if first == -1:
                if (lower >= index[i]) and (lower < index[i+1]):
                    #print "(", lower, index[i],index[i+1], ")"
                    first = i
            if last == -1:
                if upper <= index[i+1]: 
                    last = i
            pass
        return range(first,last+1)

    def testEnclosing(self):
        index = range(0,110,10)
        tests = [[0.01, 3],
                 [31.2, 34],
                 [35,54],
                 [9.9,10.1],
                 [99,100]]
        for t in tests:
            print t,"---",self.findEnclosing(index, t[0], t[1])

    def computeAllCopies(self, boundsList):
        s = []
        for b in boundsList:
            s.extend(self.computeCopies(b))
        last = None
        reduced = []
        for b in sorted(s):
            if b == last: continue
            reduced.append(b)
            last = b
        return reduced

    def computeCopies(self, bounds):
        #bounds[3]=40
        [thetaMin, thetaMax] = bounds[0:2]
        [phiMin,phiMax] = bounds[2:4]
        sList = sorted(self.stripes.keys())
        dupeList = []
        for i in self.findEnclosing(self.phiIndex, phiMin, phiMax):
            phiOff = sList[i]
            #print "For phiOff=",phiOff, 
            #print self.thetaIndices[i]
            for j in self.findEnclosing(self.thetaIndices[i], 
                                        thetaMin, thetaMax):
                dupeList.append([j,phiOff])
        #print bounds
        #self.printDupeList(dupeList)
        return dupeList

    def getDupeInfo(self, coord):
        """@param coord: thetaOff, phiOff in stripe units

        @return [[thetaOff(stripe units), phiOffset(stripe units)],
        copyNum, # copy number
        [thetaMin, thetaMax, phiMin, phiMax], # copy bounds
        [thetaOffset(degrees), phiOffset(degrees)], # w/o stretch factor
        phiRef] # reference phi for correction
        """
        return self.stripes[coord[1]][coord[0]]

    def printDupeList(self, dList):        
        try:
            for (thetaOff, phiOff) in dList:
                stripe = self.stripes[phiOff]
                b = self.stripes[phiOff][thetaOff]
                print b
        except Exception, e:
            print "errr!!", e
            print dList

    def checkDupeSanityCoord(self, coord):
        return self.checkDupeSanity(coord[1], coord[0])

    def checkDupeSanity(self, phiOffUnits, thetaOffUnits):
        thetaMid = 0.5 * (self.bounds[0] + self.bounds[1])
        dupe = self.stripes[phiOffUnits][thetaOffUnits]
        [thetaMin0, thetaMax0, phiMin0, phiMax0] = self.bounds
        [thetaMin1, thetaMax1, phiMin1, phiMax1] = dupe[2]
        [thetaOff,phiOff] = dupe[3]
        def compare(theta0, phi0, theta1, phi1, posStr):
            thetaPhiNew = translateThetaPhiCorrected([theta0, phi0], 
                                                     dupe[3],
                                                     thetaMid,
                                                     dupe[4], False)
            print "%s CopyBound %f \t%f \tGot %f \t%f" % (
                posStr, theta1, phi1, thetaPhiNew[0], thetaPhiNew[1])
            diff = (thetaPhiNew[0] - theta1, thetaPhiNew[1] - phi1)
            print "%s diff is %f %f" % (posStr, diff[0], diff[1])
            return diff

        print "Dupe Sanity for copynum=%d at offset %d,%d" % (dupe[1], 
                                                              dupe[0][1], 
                                                              dupe[0][0])
        # lower left
        diff = compare(thetaMin0, phiMin0, thetaMin1, phiMin1, "LL")
        diff = compare(thetaMax0, phiMin0, thetaMax1, phiMin1, "LR")
        diff = compare(thetaMin0, phiMax0, thetaMin1, phiMax1, "UL")
        diff = compare(thetaMax0, phiMax0, thetaMax1, phiMax1, "UR")
        pass



class CsvSchema:
    def __init__(self, conf=None):
        """Compute various properties from a schema file.
        Optionally, with a config containing schemaFile, thetaName, phiName,
        compute column numbers for phi and theta.
        """
        self.thetaColumn = None
        self.phiColumn = None
        if conf: self._setAsConfig(conf)
        pass

    def readSchemaFile(self, schemaFile):
        columns = self._readSchema(open(schemaFile))
        self._updateColumnNames(columns)
        pass

    def _updateColumnNames(self, columnNames):
        self.headerColumns = dict(izip(columnNames, range(len(columnNames))))
        self.columns = columnNames

    def readCsvHeader(self, csvLine, delimiter=","):
        """Read in column names from first line of csv file
        @return True if successful"""
        columns = csvLine.split(delimiter)
        if len(columns) < 2: # Trivial test. must have at least ra,decl.
            return False
        self._updateColumnNames(columns)
        return True

    def applyColumnNameList(self, columnNames):
        self._updateColumnNames(columnNames)

    def _readSchema(self, fileObj):
        """Read a sql header file (formatted in the way MySQL for
        "show create table x")
        @return a list of column names
        """
        create = None
        term = None
        createStr = "CREATE TABLE"
        contents = fileObj.read();
        lines = filter(lambda x:x, map(string.strip, contents.split("\n")))
        num = 0
        # Find CREATE section
        for l in lines:
            if not create and l.upper()[:len(createStr)] == createStr:
                create = num
            elif create and not term and l[-1] == ";":
                term = num
            num += 1
        colLines = filter(lambda x: x and "`" == x[0], lines[create:term])
        columns = [s[s.find("`")+1 : s.rfind("`")] for s in colLines]
        return columns

    def _setAsConfig(self, conf):
        self.readSchemaFile(conf.schemaFile)
        self.thetaColumn = self.headerColumns[conf.thetaName]
        self.phiColumn = self.headerColumns[conf.phiName]
        
class Transformer:
    def __init__(self, opts):
        self.phiCol = opts.phiColumn
        self.thetaCol = opts.thetaColumn
        self.headerColumns = opts.headerColumns
        bounds = map(float, opts.bounds.split(","))
        self.thetaMid = (bounds[0] + bounds[1])/2.0

        raFunc = lambda old,r,d: str(normalize(0, 360, 360, float(old) + raOff))
        declFunc = lambda old,r,d: str(normalize(-90, 90, 180, float(old) + declOff))
        skytileRange = 194400
             
        self.columnMap = {
            "scienceCcdExposureId" : lambda old, copyNum: str((copyNum << 36) + int(old)),
            "rawAmpExposureId" : lambda old, copyNum: str((copyNum << 41) + int(old)),
            "sourceId" : lambda old, copyNum: str((copyNum << 44) + int(old)),
            "objectId" : lambda old, copyNum: str(((copyNum*skytileRange) << 32) + int(old)),
            "snapCcdExposureId" : lambda old, copyNum: str((copyNum << 38) + int(old)),
            }
        self._buildPairList(opts.headerColumns)
        self.transformMap = {}
        # Build transform map for this csv file.
        for c,f in self.columnMap.items():
            if c in self.headerColumns:
                self.transformMap[self.headerColumns[c]] = f
                
    def _buildPairList(self, headerColumns):
        self.thetaPhiPairs = []
        for (r,d) in [
            ('ra_PS', 'decl_PS'),
            ('ra_SG', 'decl_SG'),
            ('ra', 'decl'),
            ('raPeak', 'declPeak'),
            ('raAstrom', 'declAstrom'),
            ('raObject', 'declObject'),
            ('crval1', 'crval2'),
            ]:
            if (r in headerColumns) and (d in headerColumns):
                self.thetaPhiPairs.append((headerColumns[r], headerColumns[d]))
                
    def _isInBounds(self, test, bounds):
        theta = bounds[:2]
        phi = bounds[2:4]
        return ((theta[0] < test[0] < theta[1]) and
                (phi[0] < test[1] < phi[1]))


    def transform(self, row, dupeInfo):
        test = translateThetaPhiCorrected((float(row[self.thetaCol]),
                                           float(row[self.phiCol])),
                                          dupeInfo[3],
                                          self.thetaMid,
                                          dupeInfo[4], False)
 #       print "Orig %0.3f %0.3f" % (float(row[self.thetaCol]), 
 #                                   float(row[self.phiCol])),
 #       print "Offset %0.2f %0.2f" % tuple(dupeInfo[3]),

        # Clip?
        if not self._isInBounds(test, dupeInfo[2]): #return None
#            print "reject"," ".join(map(lambda f: "%0.3f"%(f), test)),
#            print "bounds"," ".join(map(lambda f: "%0.3f"%(f), dupeInfo[2]))
            return None
#        else: print "accept"

        newRow = row[:] 
        # Transform the RA/decl pairs specially.  Only transform in pairs
        for pair in self.thetaPhiPairs: 
            thetaC = pair[0]
            phiC = pair[1]
            thetaphi = (float(row[thetaC]), float(row[phiC]))
            thetaphiNew = translateThetaPhiCorrected(
                thetaphi, dupeInfo[3], self.thetaMid, dupeInfo[4])
            (newRow[thetaC], newRow[phiC]) = map(str, thetaphiNew)

        for col,f in self.transformMap.items():
            old = row[col]
            if old != "\\N":  # skip SQL null columns
                newRow[col] = f(old, dupeInfo[1])
        #print "old",filter(lambda s: "\\N" != s, row)
        #print "new", filter(lambda s: "\\N" != s, newRow)
        return newRow

    def transformOnly(self, row, colList):
        l = []
        for c in colList:
            colNum = self.headerDict[c]
            l.append(self.transformMap[colNum](row[colNum]))
        return l

    def hasTransform(self):
        return len(self.transformMap) != 0

    def testBounds(self):
        self.bounds

########################################################################
## 
########################################################################

def main():
    pd = PartitionDef(DefaultConf.getPartitionDef())
    pstripes = pd.partitionStripes
    dd = DuplicationDef(DefaultConf.getDuplicationDef())
    dd.computeCopies(random.choice(random.choice(pstripes)).bounds)
    pass

if __name__ == "__main__":
    main()
