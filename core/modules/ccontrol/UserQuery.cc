// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
  * @brief Interface for managing the execution of user queries, that is,
  * queries as they are submitted by the user. The generation of smaller
  * chunk-level queries is handled here or by delegate classes.
  *
  * Basic usage:
  *
  * After constructing a UserQuery object with the UserQueryFactory...
  *
  * getConstraints() -- retrieve constraints of the user query to be passed to
  * spatial region selection code in another layer.
  *
  * getDominantDb() -- retrieve the "dominantDb", that is, the database whose
  * partitioning will be used for chunking and dispatch.
  *
  * getDbStriping() -- retrieve the striping parameters of the dominantDb.
  *
  * getError() -- See if there are errors
  *
  * getExecDesc() -- see how execution is progressing
  *
  * addChunk() -- Add a chunk number (and subchunks, as appropriate) to be
  * dispatched during submit(). The czar uses getConstraints and getDbStriping
  * to query a region selector over a chunk number generator and an emptychunks
  * list to compute the relevant chunk numbers.
  *
  * submit() -- send the query (in generated fragments) to the cluster for
  * execution.
  *
  * join() -- block until query execution is complete (or encounters errors)
  *
  * kill() -- stop a query in progress
  *
  * discard() -- release resources for this query.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "ccontrol/UserQuery.h"

// System headers
#include <cassert>

// Qserv headers
#include "ccontrol/TmpTableName.h"
#include "ccontrol/ResultReceiver.h"
#include "ccontrol/UserQueryError.h"
#include "global/constants.h"
#include "global/MsgReceiver.h"
#include "log/Logger.h"
#include "proto/worker.pb.h"
#include "proto/ProtoImporter.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qproc/QuerySession.h"
#include "qproc/TaskMsgFactory2.h"
#include "rproc/TableMerger.h"
#include "rproc/InfileMerger.h"
#include "util/Callable.h"

namespace lsst {
namespace qserv {

/// A class that can be used to parameterize a ProtoImporter<TaskMsg> for
/// debugging purposes
class ProtoPrinter : public util::UnaryCallable<void, boost::shared_ptr<proto::TaskMsg> > {
public:
    ProtoPrinter() {}
    virtual void operator()(boost::shared_ptr<proto::TaskMsg> m) {
        std::cout << "Got taskmsg ok";
    }
};

/// Factory to create chunkid-specific MsgReceiver objs linked to the right
/// messagestore
class ChunkMsgReceiver : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
            messageStore->addMessage(chunkId, code, msg);
        }
    static boost::shared_ptr<ChunkMsgReceiver>
    newInstance(int chunkId,
                boost::shared_ptr<qdisp::MessageStore> ms) {
        boost::shared_ptr<ChunkMsgReceiver> r(new ChunkMsgReceiver);
        r->chunkId = chunkId;
        r->messageStore = ms;
        return r;
    }

    int chunkId;
    boost::shared_ptr<qdisp::MessageStore> messageStore;
};

////////////////////////////////////////////////////////////////////////
// UserQuery implementation
namespace ccontrol {

std::string const& UserQuery::getError() const {
    return _qSession->getError();
}

// Consider exposing querySession to userQueryProxy

/// For valid constraints: see lsst.qserv.czar.spatial
/// Region factory must handle or explicitly ignore all constraints.
/// Most constraints generated in QsRestrictor
lsst::qserv::query::ConstraintVec UserQuery::getConstraints() const {
    return _qSession->getConstraints();
}
/// @return database for partitioning and dispatch
std::string const& UserQuery::getDominantDb() const {
    return _qSession->getDominantDb();
}

/// @return striping parameters for use by chunk scope evaluation
lsst::qserv::css::StripingParams UserQuery::getDbStriping() const {
    return _qSession->getDbStriping();
}

/// Attempt to kill in progress.
void UserQuery::kill() {
    _executive->squash();
}

/// Add a chunk to be executed
void UserQuery::addChunk(qproc::ChunkSpec const& cs) {
    // If this is not a chunked query, only accept the dummy chunk.
    // This should collapse out when chunk geometry coverage is moved from Python to C++.
    if(_qSession->hasChunks() || cs.chunkId == DUMMY_CHUNK) {
        _qSession->addChunk(cs);
    }
}

/// Begin running on all chunks added so far.
void UserQuery::submit() {
    _qSession->finalize();
    _setupMerger();
    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    qproc::TaskMsgFactory2 f(_sessionId);
    TmpTableName ttn(_sessionId, _qSession->getOriginal());
    std::ostringstream ss;
    proto::ProtoImporter<proto::TaskMsg> pi;
    int msgCount = 0;
    LOGGER_INF << "UserQuery beginning submission\n" << std::flush;
    LOGGER_DBG << std::flush;
    LOGGER_WRN << std::flush;
    LOGGER_ERR << std::flush;
    assert(_infileMerger);
    qproc::QuerySession::Iter i;
    qproc::QuerySession::Iter e = _qSession->cQueryEnd();
    // Writing query for each chunk
    for(i = _qSession->cQueryBegin(); i != e; ++i) {
        qproc::ChunkQuerySpec& cs = *i;
        std::string chunkResultName = ttn.make(cs.chunkId);
        ++msgCount;
        f.serializeMsg(cs, chunkResultName, ss);
        std::string msg = ss.str();

        pi(msg.data(), msg.size());
        if(pi.getNumAccepted() != msgCount) {
            throw UserQueryBug("Error serializing TaskMsg.");
        }

        ResourceUnit ru;
        ru.setAsDbChunk(cs.db, cs.chunkId);
        boost::shared_ptr<ResultReceiver> rr;
        boost::shared_ptr<ChunkMsgReceiver> cmr;
        cmr = ChunkMsgReceiver::newInstance(cs.chunkId, _messageStore);
        rr.reset(new ResultReceiver(cmr, _infileMerger, chunkResultName));
        int refNum = ++_sequence;
        qdisp::Executive::Spec s = { ru,
                                     ss.str(),
                                     rr };
        _executive->add(refNum, s);
        ss.str(""); // reset stream
    }
}

/// Block until a submit()'ed query completes.
/// @return the QueryState indicating success or failure
QueryState UserQuery::join() {
    bool successful = _executive->join(); // Wait for all data
    _infileMerger->finalize(); // Wait for all data to get merged
    if(successful) {
        LOGGER_INF << "Joined everything (success)" << std::endl;
        return SUCCESS;
    } else {
        LOGGER_ERR << "Joined everything (failure!)" << std::endl;
        return ERROR;
    }
    _discardMerger();
}

/// Release resources held by the merger
void UserQuery::_discardMerger() {
    _infileMergerConfig.reset();
    if(_infileMerger && !_infileMerger->isFinished()) {
        throw UserQueryError("merger unfinished, cannot discard");
    }
    _infileMerger.reset();
}

/// Release resources.
void UserQuery::discard() {
    // Make sure resources are released.
    if(_executive && _executive->getNumInflight() > 0) {
        throw UserQueryError("Executive unfinished, cannot discard");
    }
    _executive.reset();
    _messageStore.reset();
    _qSession.reset();
    try {
        _discardMerger();
    } catch(UserQueryError e) {
        // Silence merger discarding errors, because this object is being released.
        // client no longer cares about merger errors.
    }
    LOGGER_INF << "Discarded UserQuery(" << _sessionId << ")" << std::endl;
}

/// Check for database existence
/// @return true if db is known to Qserv
bool UserQuery::containsDb(std::string const& dbName) const {
    return _qSession->containsDb(dbName);
}

/// Constructor. Most setup work done by the UserQueryFactory
UserQuery::UserQuery(boost::shared_ptr<qproc::QuerySession> qs)
    :  _messageStore(new qdisp::MessageStore()),
       _qSession(qs), _sequence(0) {
    // Some configuration done by factory: See UserQueryFactory
}
/// @return a plaintext description of query execution progress
std::string UserQuery::getExecDesc() const {
    return _executive->getProgressDesc();
}
/// Setup merger (for results handling and aggregation)
void UserQuery::_setupMerger() {
    LOGGER_INF << "UserQuery::_setupMerger()" << std::endl;
    _infileMergerConfig->mergeStmt = _qSession->getMergeStmt();
    _infileMerger = boost::make_shared<rproc::InfileMerger>(*_infileMergerConfig);
}

}}} // lsst::qserv::ccontrol
