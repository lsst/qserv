// -*- LSST-C++ -*-

#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/dispatcher.h"
#include "lsst/qserv/master/thread.h"
#include "lsst/qserv/master/xrootd.h"

namespace qMaster = lsst::qserv::master;

namespace {
    qMaster::QueryManager& getManager(int session) {
	// Singleton for now. //
	static boost::shared_ptr<qMaster::QueryManager> qm;
	if(qm.get() == NULL) {
	    qm = boost::make_shared<qMaster::QueryManager>();
	}
	return *qm;
    }

    qMaster::AsyncQueryManager& getAsyncManager(int session) {
	// Singleton for now. //
	static boost::shared_ptr<qMaster::AsyncQueryManager> qm;
	if(qm.get() == NULL) {
	    qm = boost::make_shared<qMaster::AsyncQueryManager>();
	}
	assert(qm.get() != NULL);
	return *qm;
    }
    
}

void qMaster::initDispatcher() {
    xrdInit();
}

/// @param session Int for the session (the top-level query)
/// @param chunk Chunk number within this session (query)
/// @param str Query string (c-string)
/// @param len Query string length
/// @param savePath File path (with name) which will store the result (file, not dir)
/// @return a token identifying the session??? FIXME
int qMaster::submitQuery(int session, int chunk, char* str, int len, char* savePath) {
    TransactionSpec t;
    t.path = savePath;
    t.query = std::string(str, len);
    t.bufferSize = 8192000;
    t.path = qMaster::makeUrl("query", chunk);
    return submitQuery(session, TransactionSpec(t));
}

int qMaster::submitQuery(int session, qMaster::TransactionSpec const& s) {
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
    int queryId = 0;
    /* queryId = */ qm.add(s); 
    return queryId;
}

qMaster::QueryState qMaster::joinQuery(int session, int id) {
    // Block until specific query id completes.
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
    //qm.join(id);
    //qm.status(id); // get status
    // If error, report
    return UNKNOWN; // FIXME: convert status to querystate.
}

qMaster::QueryState qMaster::tryJoinQuery(int session, int id) {
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
#if 0 // Not implemented yet
    // Just get the status and return it.
    if(qm.tryJoin(id)) {
	return SUCCESS; 
    } else {
	return ERROR;
    }   
#endif
}

qMaster::QueryState qMaster::joinSession(int session) {
    // FIXME
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
    qm.joinEverything();
}

int qMaster::newSession() {
    return 1; // For now, always give session # 1
}

void qMaster::discardSession(int session) {
    // FIXME
    return; // For now, don't discard.
}

qMaster::XrdTransResult qMaster::getQueryResult(int session, int chunk) {
    // FIXME
}
