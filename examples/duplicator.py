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
import math
import time

## Local
import partition

def computeHeaderDict(headerRow):
    return dict(zip(headerRow, range(len(headerRow))))

class ChunkBounds:
    def __init__(self, chunkId, bounds):
        self.chunkId = chunkId
        self.bounds = bounds[:]
        pass
        
    def __str__(self):
        return "%d : %s" % (self.chunkId, str(self.bounds))

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


    def printPartBounds(self):
        for s in self.partStripes:
            print "Stripe", a
            print "\n".join(map(str, s))
            a += 1
            #if a > 2: break

class DuplicationDef:
    def __init__(self, conf):
        self.layoutDupeMap()
        
        pass

    def layoutDupeMap(self):
        inputBoundsPt11 = [-2.0221828620000224, 5.21559213586,
                            -6.80690075667, 7.11656414672]
        
        bounds = inputBoundsPt11
        thetaSize = bounds[1] - bounds[0]
        phiSize = bounds[3] - bounds[2]
        
        phiOffsetUnits = int(math.ceil(90.0 / phiSize))
        thetaOffsetUnits = int(math.ceil(360.0 / thetaSize))
        
        boundsRad = map(math.radians, bounds)
        def stretchMin(phi):
            if phi < -90.0: return 1e5 # Saturate at some number < Inf
            return math.cos(boundsRad[2]) / math.cos(boundsRad[2] 
                                                     + math.radians(phi))
        def stretchMax(phi):
            if phi > 90.0: return 1e5 # Saturate at some number < Inf
            return math.cos(boundsRad[3]) / math.cos(boundsRad[3]
                                                     + math.radians(phi))

        stripes = {}
        phiIndex = []
        phiLast = (-phiOffsetUnits * phiSize) + bounds[2]
        thetaIndices = []
        # Phi stripes
        for i in range(-phiOffsetUnits, phiOffsetUnits, 1):
            stripe = []
            thetaIndex = []
            phiOffset = i * phiSize # phi has no stretching factor
            (phiMin, phiMax) = (phiLast, bounds[3] + phiOffset)
            if (phiMax < -90.0) or (phiMin > 90.0): 
                phiLast = phiMax
                continue
            # theta blocks in a stripe
            thetaMinPhiMin0 = 360 + (bounds[0] * stretchMin(phiMin))
            thetaMinPhiMax0 = 360 + (bounds[0] * stretchMax(phiMax))
            phiPositive = (math.fabs(phiMin) < math.fabs(phiMax))
            thetaMaxLast = None
            for j in range(0, thetaOffsetUnits):
                # Constant RA sides
                thetaMinRaw = (bounds[0] + (j * thetaSize))
                thetaMinPhiMin = thetaMinRaw * stretchMin(phiMin)
                thetaMinPhiMax = thetaMinRaw * stretchMax(phiMax) 
                if phiPositive:
                    if thetaMinPhiMin > thetaMinPhiMin0: break
                    thetaMin = thetaMinPhiMin
                    thetaMax = thetaMinPhiMin + (thetaSize * stretchMin(phiMin))
                else:
                    if thetaMinPhiMax > thetaMinPhiMax0: break
                    thetaMin = thetaMinPhiMax
                    thetaMax = thetaMinPhiMax + (thetaSize * stretchMax(phiMax))
                if thetaMaxLast != None:
                    thetaMin = thetaMaxLast
                thetaIndex.append(thetaMin)
                thetaMaxLast = thetaMax

                #print "Copy (%d,%d) : theta (%f, %f), phi (%f, %f) " % (
                #    j, i, thetaMin, thetaMax, phiMin, phiMax)
                stripe.append([[j,i],[thetaMin, thetaMax, phiMin, phiMax]])
            thetaIndex.append(thetaMaxLast)
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

        #for s in thetaIndices:
        #    print " ".join(map(lambda c:  "%2.1f" % c, s))
        return stripes

    def printCopyInfo(self):
        copyCount = 0
        for i in sorted(stripes.keys()):
            copyCount += len(stripes[i])
            first = stripes[i][0][1]
            lastTheta = first[0]
            phi = first[2:4]
            print "phi: %f %f (%f,%d)" % (phi[0], phi[1], 
                                          stretchMax(phi[1]), 
                                          len(stripes[i])),
            print "%.2f" % lastTheta,
            for dupe in stripes[i]:
                b = dupe[1]
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
        s = set()
        for b in boundsList:
            s.update(self.computeCopies(b))
        return s

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
        print bounds
        self.printDupeList(dupeList)
        return dupeList

    def printDupeList(self, dList):        
        try:
            for (thetaOff, phiOff) in dList:
                stripe = self.stripes[phiOff]
                b = self.stripes[phiOff][thetaOff]
                print b
        except Exception, e:
            print "errr!!", e
            print dList



class Transform:
    def __init__(self, copyParam, headerDict):
        (raOff, declOff, copyNum) = copyParam
        self.headerDict = headerDict
        
        # Normalization function.
        def normalize(low, high, identity, val):
            if val < low: return val + identity
            if val > high: return val - identity
            return val

        raFunc = lambda old: str(normalize(0, 360, 360, float(old) + raOff))
        declFunc = lambda old: str(normalize(-90, 90, 180, float(old) + declOff))
        skytileRange = 194400
        self.columnMap = {
            "scienceCcdExposureId" : lambda old: str((copyNum << 36) + int(old)),
            "rawAmpExposureId" : lambda old: str((copyNum << 41) + int(old)),
            "sourceId" : lambda old: str((copyNum << 44) + int(old)),
            "objectId" : lambda old: str(((copyNum*skytileRange) << 32) + int(old)),
            "snapCcdExposureId" : lambda old: str((copyNum << 38) + int(old)),
            }
        # Add ra and decl column functions
        for c in ['ra_PS',
                  'ra_SG',  
                  # raRange is a size, not a coordinate???
                  "ra",
                  # raFlux ???
                  "raPeak",
                  "raAstrom",
                  "raObject",
                  "crval1"
                  ]:
            self.columnMap[c] = raFunc
        for c in ['decl_PS',
                  'decl_SG',  
                  "decl",
                  "declPeak",
                  "declAstrom",
                  "declObject",
                  "crval2",
                  ]:
            self.columnMap[c] = declFunc
            
        self.transformMap = {}
        # Build transform map for this csv file.
        for c,f in self.columnMap.items():
            if c in headerDict:
                self.transformMap[headerDict[c]] = f

    def transform(self, row):
        newRow = row[:]
        for col,f in self.transformMap.items():
            old = row[col]
            if old != "\\N":  # skip SQL null columns
                newRow[col] = f(row[col])
        return newRow

    def transformOnly(self, row, colList):
        l = []
        for c in colList:
            colNum = self.headerDict[c]
            l.append(self.transformMap[colNum](row[colNum]))
        return l

    def hasTransform(self):
        return len(self.transformMap) != 0
