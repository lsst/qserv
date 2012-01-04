#!/usr/bin/env python

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

import time
import qservMaster_timing as timingVars

def splitWord(w):
    s = w.find("Start")
    f = w.find("Finish")

    if s != -1:
        return (w[:s],w[s:])
    elif f != -1:
        return (w[:f],w[f:])
    return
    
def writeCsv(timing):
    times = {}
    #print timing
    mintime=time.time()
    for k in timing:
        (t,typ) = splitWord(k)
        entry = times.get(t,[0,0])
        if typ == "Start":
            entry[0] = timing[k]
            mintime = min(mintime, timing[k])
        elif typ == "Finish":
            entry[1] = timing[k]
        times[t] = entry

    items = times.items()
    items.sort()
    for(k,v) in items:
        print "%s,%f,%f" % (k, v[0]-mintime, v[1]-v[0])
        
    
def doStuff():
    timings = filter(lambda x:"timing_" == x[:7], dir(timingVars))
    timings.sort()
    for t in timings:
        # Get only one query result
        (name,timing) = getattr(timingVars, t).items()[0]
        print "## results for %s/%s" %(t,name)
        writeCsv(timing)
        print ""



doStuff()
