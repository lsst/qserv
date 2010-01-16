// -*- LSST-C++ -*-

#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/dispatcher.h"

namespace qMaster = lsst::qserv::master;

void initDispatcher() {
    xrdInit();
}

int submitQuery(int session, int chunk, char* str, int len, char* savePath) {
    
}

QueryState joinChunk(int session, int chunk);
QueryState joinSession(int session);
void discardSession(int session);
XrdTransResult getQueryResult(int session, int chunk);
