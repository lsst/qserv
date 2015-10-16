// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "ccontrol/UserQuery.h"

// System headers
#include <cassert>
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MergingHandler.h"
#include "ccontrol/TmpTableName.h"
#include "ccontrol/UserQueryError.h"
#include "global/constants.h"
#include "global/MsgReceiver.h"
#include "proto/worker.pb.h"
#include "proto/ProtoImporter.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qmeta/QMeta.h"
#include "qproc/geomAdapter.h"
#include "qproc/IndexMap.h"
#include "qproc/QuerySession.h"
#include "qproc/TaskMsgFactory2.h"
#include "query/FromList.h"
#include "query/JoinRef.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "util/Callable.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER getLogger() {
    static LOG_LOGGER logger = LOG_GET("lsst.qserv.ccontrol.UserQuery");
    return logger;
}
}

namespace lsst {
namespace qserv {

/// A class that can be used to parameterize a ProtoImporter<TaskMsg> for
/// debugging purposes
class ProtoPrinter: public util::UnaryCallable<void, std::shared_ptr<proto::TaskMsg> > {
public:
    ProtoPrinter() {}
    virtual void operator()(std::shared_ptr<proto::TaskMsg> m) {
        std::cout << "Got taskmsg ok";
    }
    virtual ~ProtoPrinter() {}
};

/// Factory to create chunkid-specific MsgReceiver objs linked to the right
/// messagestore
class ChunkMsgReceiver : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
            messageStore->addMessage(chunkId, code, msg);
        }
    static std::shared_ptr<ChunkMsgReceiver>
    newInstance(int chunkId,
                std::shared_ptr<qdisp::MessageStore> ms) {
        std::shared_ptr<ChunkMsgReceiver> r = std::make_shared<ChunkMsgReceiver>();
        r->chunkId = chunkId;
        r->messageStore = ms;
        return r;
    }

    int chunkId;
    std::shared_ptr<qdisp::MessageStore> messageStore;
};

////////////////////////////////////////////////////////////////////////
// UserQuery implementation
namespace ccontrol {

std::string const& UserQuery::getError() const {
    std::string div = (_errorExtra.size() && _qSession->getError().size())
        ? " " : "";
    _errorExtraCache = _qSession->getError() + div + _errorExtra;
    return _errorExtraCache;
}

/// Attempt to kill in progress.
void UserQuery::kill() {
    LOGF_INFO("UserQuery kill");
    std::lock_guard<std::mutex> lock(_killMutex);
    if(!_killed) {
        _killed = true;
        try {
            _executive->squash();
        } catch(UserQueryError const &e) {
            // Silence merger discarding errors, because this object is being
            // released. Client no longer cares about merger errors.
        }
        _qMetaUpdateStatus(qmeta::QInfo::ABORTED);
    }
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

    // register query in qmeta, this may throw
    _qMetaRegister();

    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    qproc::TaskMsgFactory2 f(_sessionId);
    TmpTableName ttn(_sessionId, _qSession->getOriginal());
    proto::ProtoImporter<proto::TaskMsg> pi;
    int msgCount = 0;
    LOG(getLogger(), LOG_LVL_INFO, "UserQuery beginning submission");
    assert(_infileMerger);
    std::vector<int> chunks;
    // Writing query for each chunk
    for(auto i = _qSession->cQueryBegin(), e = _qSession->cQueryEnd(); i != e; ++i) {
        qproc::ChunkQuerySpec& cs = *i;
        chunks.push_back(cs.chunkId);
        std::string chunkResultName = ttn.make(cs.chunkId);
        ++msgCount;
        std::ostringstream ss;
        f.serializeMsg(cs, chunkResultName, ss);
        std::string msg = ss.str();

        pi(msg.data(), msg.size());
        if(pi.getNumAccepted() != msgCount) {
            throw UserQueryBug("Error serializing TaskMsg.");
        }

        std::shared_ptr<ChunkMsgReceiver> cmr = ChunkMsgReceiver::newInstance(cs.chunkId, _messageStore);
        ResourceUnit ru;
        ru.setAsDbChunk(cs.db, cs.chunkId);
        int refNum = ++_sequence;
        qdisp::JobDescription jobDesc(refNum, ru, ss.str(),
            std::make_shared<MergingHandler>(cmr, _infileMerger, chunkResultName));
        _executive->add(jobDesc);
    }

    _submitted = true;

    // we only care about per-chunk info for ASYNC queries, and
    // currently all queries are SYNC, so we skip this.
    qmeta::QInfo::QType const qType = qmeta::QInfo::SYNC;
    if (qType == qmeta::QInfo::ASYNC) {
        _qMetaAddChunks(chunks);
    }
}

/// Block until a submit()'ed query completes.
/// @return the QueryState indicating success or failure
QueryState UserQuery::join() {
    bool successful = _executive->join(); // Wait for all data
    _infileMerger->finalize(); // Wait for all data to get merged
    _discardMerger();
    if (not _submitted) {
        _qMetaUpdateStatus(qmeta::QInfo::FAILED);
        LOG(getLogger(), LOG_LVL_ERROR, "Not fully submitted (failure!)");
        return ERROR;
    } else if(successful) {
        _qMetaUpdateStatus(qmeta::QInfo::COMPLETED);
        LOG(getLogger(), LOG_LVL_INFO, "Joined everything (success)");
        return SUCCESS;
    } else {
        _qMetaUpdateStatus(qmeta::QInfo::FAILED);
        LOG(getLogger(), LOG_LVL_ERROR, "Joined everything (failure!)");
        return ERROR;
    }
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
    {
        std::lock_guard<std::mutex> lock(_killMutex);
        if(_killed) {
            return;
        }
    }
    // Make sure resources are released.
    if(_executive && _executive->getNumInflight() > 0) {
        throw UserQueryError("Executive unfinished, cannot discard");
    }
    _executive.reset();
    _messageStore.reset();
    _qSession.reset();
    try {
        _discardMerger();
    } catch(UserQueryError const &e) {
        // Silence merger discarding errors, because this object is being released.
        // client no longer cares about merger errors.
    }
    LOGF_INFO("Discarded UserQuery(%1%)" % _sessionId);
}

/// Constructor. Most setup work done by the UserQueryFactory
UserQuery::UserQuery(std::shared_ptr<qproc::QuerySession> qs, qmeta::CzarId czarId)
    :  _messageStore(std::make_shared<qdisp::MessageStore>()),
       _qSession(qs), _qMetaCzarId(czarId), _qMetaQueryId(0),
       _killed(false), _submitted(false), _sessionId(0), _sequence(0) {
    // Some configuration done by factory: See UserQueryFactory
}

/// Setup merger (for results handling and aggregation)
void UserQuery::_setupMerger() {
    LOG(getLogger(), LOG_LVL_TRACE, "Setup merger");
    _infileMergerConfig->mergeStmt = _qSession->getMergeStmt();
    _infileMerger = std::make_shared<rproc::InfileMerger>(*_infileMergerConfig);
}

void UserQuery::_setupChunking() {
    LOG(getLogger(), LOG_LVL_TRACE, "Setup chunking");
    // Do not throw exceptions here, set _errorExtra .
    std::shared_ptr<qproc::IndexMap> im;
    std::string dominantDb = _qSession->getDominantDb();
    if(dominantDb.empty() || !_qSession->validateDominantDb()) {
        // TODO: Revisit this for L3
        throw UserQueryError("Couldn't determine dominantDb for dispatch");
    }

    std::shared_ptr<IntSet const> eSet = _qSession->getEmptyChunks();
    {
        eSet = _qSession->getEmptyChunks();
        if(!eSet) {
            eSet = std::make_shared<IntSet>();
            LOGF(getLogger(), LOG_LVL_WARN, "Missing empty chunks info for %1%" % dominantDb);
        }
    }
    // FIXME add operator<< for QuerySession
    LOGF(getLogger(), LOG_LVL_TRACE, "_qSession: %1%" % _qSession);
    if (_qSession->hasChunks()) {
        std::shared_ptr<query::ConstraintVector> constraints
            = _qSession->getConstraints();
        css::StripingParams partStriping = _qSession->getDbStriping();

        im = std::make_shared<qproc::IndexMap>(partStriping, _secondaryIndex);
        qproc::ChunkSpecVector csv;
        if(constraints) {
            csv = im->getChunks(*constraints);
        } else { // Unconstrained: full-sky
            csv = im->getAllChunks();
        }

        LOGF(getLogger(), LOG_LVL_TRACE, "Chunk specs: %1%" % util::printable(csv));
        // Filter out empty chunks
        for(qproc::ChunkSpecVector::const_iterator i=csv.begin(), e=csv.end();
            i != e;
            ++i) {
            if(eSet->count(i->chunkId) == 0) { // chunk not in empty?
                _qSession->addChunk(*i);
            }
        }
    } else {
        LOG(getLogger(), LOG_LVL_TRACE, "No chunks added, QuerySession will add dummy chunk");
    }
}

// register query in qmeta database
void UserQuery::_qMetaRegister()
{
    qmeta::QInfo::QType qType = qmeta::QInfo::SYNC;  // now all queries are SYNC
    std::string user = "anonymous";    // we do not have access to that info yet

    std::string qTemplate;
    auto const& stmtVector = _qSession->getStmtParallel();
    for (auto itr = stmtVector.begin(); itr != stmtVector.end(); ++ itr) {
        auto stmt = *itr;
        if (stmt) {
            if (not qTemplate.empty()) {
                // if there is more than one statement separate them by
                // special token
                qTemplate += " /*QSEPARATOR*/; ";
            }
            qTemplate += stmt->getQueryTemplate().toString();
        }
    }

    std::string qMerge;
    auto mergeStmt = _qSession->getMergeStmt();
    if (mergeStmt) {
        qMerge = mergeStmt->getQueryTemplate().toString();
    }
    std::string proxyOrderBy = _qSession->getProxyOrderBy();
    qmeta::QInfo qInfo(qType, _qMetaCzarId, user, _qSession->getOriginal(),
                       qTemplate, qMerge, proxyOrderBy);

    // find all table names used by statement (which appear in FROM ... [JOIN ...])
    qmeta::QMeta::TableNames tableNames;
    const auto& tables = _qSession->getStmt().getFromList().getTableRefList();
    for (auto itr = tables.begin(); itr != tables.end(); ++ itr) {
        // add table name
        tableNames.push_back(std::make_pair((*itr)->getDb(), (*itr)->getTable()));

        // add its joins if any
        const auto& joins = (*itr)->getJoins();
        for (auto jtr = joins.begin(); jtr != joins.end(); ++ jtr) {
            const auto& right = (*jtr)->getRight();
            if (right) {
                tableNames.push_back(std::make_pair(right->getDb(), right->getTable()));
            }
        }
    }

    // register query, save its ID
    _qMetaQueryId = _queryMetadata->registerQuery(qInfo, tableNames);

    // Note that ordering is important here, this check must happen after
    // query is registered in qmeta
    for (auto itr = tableNames.begin(); itr != tableNames.end(); ++ itr) {
        if (not _qSession->containsTable(itr->first, itr->second)) {
            // table either does not exist or it is being deleted, we must stop
            // here but we must mark query as failed
            _qMetaUpdateStatus(qmeta::QInfo::FAILED);

            // Throwing exception stops submit() but it does not set any
            // error condition, only prints error message to the log. To communicate
            // error message to caller we need to set _errorExtra
            std::string const msg = "Table '" + itr->first + "." + itr->second + "' does not exist";
            _messageStore->addMessage(-1, 1146, msg, MessageSeverity::MSG_ERROR);
            throw UserQueryError(_errorExtra);
        }
    }
}

// update query status in QMeta
void UserQuery::_qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus)
{
    _queryMetadata->completeQuery(_qMetaQueryId, qStatus);
}

// add chunk information to qmeta
void UserQuery::_qMetaAddChunks(std::vector<int> const& chunks)
{
    _queryMetadata->addChunks(_qMetaQueryId, chunks);
}

}}} // lsst::qserv::ccontrol
