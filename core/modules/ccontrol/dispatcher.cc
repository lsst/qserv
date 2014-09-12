// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
/**
  * @file
  *
  * @brief SWIG-exported interface to dispatching queries.
  * Basic usage:
  *
  * newSession() // Init a new session
  *
  * setupQuery(int session, std::string const& query) // setup the session with
  * a query. This triggers a parse.
  *
  * getSessionError(int session) // See if there are errors
  *
  * getConstraints(int session)  // Retrieve the detected constraints so that we
  * can apply them to see which chunks we need. (done in Python)
  *
  * addChunk(int session, lsst::qserv::qproc::ChunkSpec const& cs ) // add the
  * computed chunks to the query
  *
  * submitQuery3(int session) // Trigger the dispatch of all chunk queries for
  * the session.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "ccontrol/dispatcher.h"

// System headers
#include <fstream>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "ccontrol/AsyncQueryManager.h"
#include "ccontrol/SessionManagerAsync.h"
#include "ccontrol/thread.h"
#include "ccontrol/TmpTableName.h"
#include "global/constants.h"
#include "obsolete/QservPath.h"
#include "qproc/ChunkSpec.h"
#include "qproc/QuerySession.h"
#include "qproc/TaskMsgFactory2.h"
#include "util/StringHash.h"
#include "util/xrootd.h"
#include "xrdc/xrdfile.h"

namespace {
std::string makeSavePath(std::string const& dir,
                         int sessionId,
                         int chunkId,
                         unsigned seq=0) {
    std::stringstream ss;
    ss << dir << "/" << sessionId << "_" << chunkId << "_" << seq;
    return ss.str();
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace ccontrol {

int
submitQuery(int session, qdisp::TransactionSpec const& s,
            std::string const& resultName) {
    LOGF_DEBUG("EXECUTING submitQuery(%1%, TransactionSpec s, %2%)"
               % session % resultName);
    AsyncQueryManager& qm = getAsyncManager(session);
    qm.add(s, resultName);
    LOGF_DEBUG("Dispatcher added %1%" % s.chunkId);
    return 0;
}

struct mergeStatus {
    mergeStatus(bool& success, bool shouldPrint_=false, int firstN_=5)
        : isSuccessful(success), shouldPrint(shouldPrint_),
          firstN(firstN_) {
        isSuccessful = true;
    }
    void operator() (AsyncQueryManager::Result const& x) {
        if(!x.second.isSuccessful()) {
            if(shouldPrint || (firstN > 0)) {
                LOGF_INFO("Chunk %1% error " % x.first);
                LOGF_INFO("open: %1% qWrite: %2% read: %3% lWrite: %4%"
                          % x.second.open % x.second.queryWrite
                          % x.second.read % x.second.localWrite);
                --firstN;
            }
            isSuccessful = false;
        } else {
            if(shouldPrint) {
                LOGF_INFO("Chunk %1% OK (%2%)\t"
                          % x.first % x.second.localWrite);
            }
        }
    }
    bool& isSuccessful;
    bool shouldPrint;
    int firstN;
};

void
setupQuery(int session, std::string const& query, std::string const& resultTable) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    qs.setResultTable(resultTable);
    qs.setQuery(query);
}

std::string const&
getSessionError(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    return qs.getError();
}

query::Constraint
getC(int base) {
    // SWIG test.
    std::stringstream ss;
    query::Constraint c;
    ss << "box" << base; c.name = ss.str(); ss.str("");
    ss << base << "1"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "2"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "3"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "4"; c.params.push_back(ss.str()); ss.str("");
    return c; // SWIG test.
 }

query::ConstraintVec
getConstraints(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    return query::ConstraintVec(qs.getConstraints());
}

std::string const&
getDominantDb(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    return qs.getDominantDb();
}

bool
containsDb(int session, std::string const& dbName) {
    return getAsyncManager(session).getQuerySession().containsDb(dbName);
}

css::StripingParams
getDbStriping(int session) {
    return getAsyncManager(session).getQuerySession().getDbStriping();
}

void
addChunk(int session, qproc::ChunkSpec const& cs ) {
#if 0 // SWIG plumbing debug
    typedef std::vector<int> Vect;
    int count=0;
    if (LOG_CHECK_INFO()) {
        LOGF_INFO("Received chunk=%1% " % cs.chunkId);
        std::stringstream ss;
        for(Vect::const_iterator i = cs.subChunks.begin();
            i != cs.subChunks.end(); ++i) {
            if(++count > 1) {
                ss << ", ";
            }
            ss << *i;
        }
        LOG_INFO("%1%" % ss.str());
    }
#endif
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    // If this is not a chunked query, only accept the dummy chunk.
    // This should collapse out when chunk geometry coverage is moved from Python to C++.
    if(qs.hasChunks() || cs.chunkId == DUMMY_CHUNK) {
        qs.addChunk(cs);
    }
}

/// Submit the query.
void
submitQuery3(int session) {
    LOGF_DEBUG("EXECUTING submitQuery3(%1%)" % session);
    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    qproc::TaskMsgFactory2 f(session);

    qs.finalize();
    std::string const hp = qm.getXrootdHostPort();
    TmpTableName ttn(session, qs.getOriginal());
    std::ostringstream ss;
    qproc::QuerySession::Iter i;
    qproc::QuerySession::Iter e = qs.cQueryEnd();
    // Writing query for each chunk
    for(i = qs.cQueryBegin(); i != e; ++i) {
        qproc::ChunkQuerySpec& cs = *i;
        std::string chunkResultName = ttn.make(cs.chunkId);
        f.serializeMsg(cs, chunkResultName, ss);

        qdisp::TransactionSpec t;
        obsolete::QservPath qp;
        qp.setAsCquery(cs.db, cs.chunkId);
        std::string path=qp.path();
        t.chunkId = cs.chunkId;
        t.query = ss.str();
        LOGF_INFO("Msg cid=%1% with size=%2%" % cs.chunkId % t.query.size());
        t.bufferSize = 8192000;
        t.path = util::makeUrl(hp.c_str(), qp.path());
        t.savePath = makeSavePath(qm.getScratchPath(), session, cs.chunkId);
        ss.str(""); // reset stream
        qm.add(t, chunkResultName);
    }
}

QueryState
joinSession(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qm.joinEverything();
    AsyncQueryManager::ResultDeque const& d = qm.getFinalState();
    bool successful;
    std::for_each(d.begin(), d.end(), mergeStatus(successful));

    if(successful) {
        LOGF_INFO("Joined everything (success)");
        return SUCCESS;
    } else {
        LOGF_ERROR("Joined everything (failure!)");
        return ERROR;
    }
}

std::string
getErrorDesc(int session) {

    class ErrMsgStr_ {
    public:
        ErrMsgStr_(const std::string& name): _name(name) {}

        void add(int x) {
            if (_ss.str().length() == 0) {
                _ss << _name << " failed for chunk(s):";
            }
            _ss << ' ' << x;
        }
        std::string toString() {
            return _ss.str();
        }
    private:
        const std::string _name;
        std::stringstream _ss;
    };

    std::stringstream ss;
    AsyncQueryManager& qm = getAsyncManager(session);
    AsyncQueryManager::ResultDeque const& d = qm.getFinalState();
    AsyncQueryManager::ResultDequeCItr itr;
    ErrMsgStr_ openV("open");
    ErrMsgStr_ qwrtV("queryWrite");
    ErrMsgStr_ readV("read");
    ErrMsgStr_ lwrtV("localWrite");

    for (itr=d.begin() ; itr!=d.end(); ++itr) {
        if (itr->second.open <= 0) {
            openV.add(itr->first);
        } else if (itr->second.queryWrite <= 0) {
            qwrtV.add(itr->first);
        } else if (itr->second.read < 0) {
            readV.add(itr->first);
        } else if (itr->second.localWrite <= 0) {
            lwrtV.add(itr->first);
        }
    }
    // Handle open, write, read errors first. If we have
    // any of these errors, we will get localWrite errors
    // for every chunk, because we are not writing result,
    // so don't bother reporting them.
    ss << openV.toString();
    ss << qwrtV.toString();
    ss << readV.toString();
    if (ss.str().empty()) {
        ss << lwrtV.toString();
    }
    return ss.str();
}

int
newSession(std::map<std::string,std::string> const& config) {
    try {
        boost::shared_ptr<AsyncQueryManager> m(new AsyncQueryManager(config));
        return getSessionManagerAsync().newSession(m);
    } catch(AsyncQueryManager::ConfigError& e) {
        LOGF_ERROR("Cannot create AsyncQueryManager, invalid config.");
        return -1;
    }
}

void
configureSessionMerger(int session, rproc::TableMergerConfig const& c) {
    getAsyncManager(session).configureMerger(c);
}

void
configureSessionMerger3(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qproc::QuerySession& qs = qm.getQuerySession();
    std::string const& resultTable = qs.getResultTable();
    rproc::MergeFixup m = qs.makeMergeFixup();
    qm.configureMerger(m, resultTable);
}

std::string
getSessionResultName(int session) {
    return getAsyncManager(session).getMergeResultName();
}

void
discardSession(int session) {
    getSessionManagerAsync().discardSession(session);
}

}}} // namespace lsst::qserv::ccontrol
