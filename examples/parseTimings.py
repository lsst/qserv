#!/usr/bin/env python
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
