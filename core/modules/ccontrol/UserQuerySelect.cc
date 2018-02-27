// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 AURA/LSST.
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
#include "ccontrol/UserQuerySelect.h"

// System headers
#include <cassert>
#include <chrono>
#include <memory>

// Third-party headers
#include <boost/algorithm/string/replace.hpp>

#include "../qdisp/QdispPool.h"
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
#include "qproc/TaskMsgFactory.h"
#include "query/FromList.h"
#include "query/JoinRef.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQuerySelect");


int timeDiff(std::chrono::time_point<std::chrono::system_clock> const& begin, // TEMPORARY-timing
        std::chrono::time_point<std::chrono::system_clock> const& end) {
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return diff.count();
}

} // namespace

namespace lsst {
namespace qserv {

/// A class that can be used to parameterize a ProtoImporter<TaskMsg> for
/// debugging purposes
class ProtoPrinter {
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
// UserQuerySelect implementation
namespace ccontrol {

/// Constructor
UserQuerySelect::UserQuerySelect(std::shared_ptr<qproc::QuerySession> const& qs,
                                 std::shared_ptr<qdisp::MessageStore> const& messageStore,
                                 std::shared_ptr<qdisp::Executive> const& executive,
                                 std::shared_ptr<rproc::InfileMergerConfig> const& infileMergerConfig,
                                 std::shared_ptr<qproc::SecondaryIndex> const& secondaryIndex,
                                 std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                                 qmeta::CzarId czarId,
                                 std::shared_ptr<qdisp::QdispPool> const& qdispPool,
                                 std::string const& errorExtra,
                                 bool async)
    :  _qSession(qs), _messageStore(messageStore), _executive(executive),
       _infileMergerConfig(infileMergerConfig), _secondaryIndex(secondaryIndex),
       _queryMetadata(queryMetadata), _qMetaCzarId(czarId), _qdispPool(qdispPool),
       _errorExtra(errorExtra), _async(async) {
}

std::string UserQuerySelect::getError() const {
    std::string div = (_errorExtra.size() && _qSession->getError().size())
        ? " " : "";
    return _qSession->getError() + div + _errorExtra;
}

/// Attempt to kill in progress.
void UserQuerySelect::kill() {
    LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << " UserQuerySelect kill");
    std::lock_guard<std::mutex> lock(_killMutex);
    if (!_killed) {
        _killed = true;
        try {
            // make a copy of executive pointer to keep it alive and avoid race
            // with pointer being reset in discard() method
            std::shared_ptr<qdisp::Executive> exec = _executive;
            if (exec != nullptr) {
                exec->squash();
            }
        } catch(UserQueryError const &e) {
            // Silence merger discarding errors, because this object is being
            // released. Client no longer cares about merger errors.
        }
        _qMetaUpdateStatus(qmeta::QInfo::ABORTED);
    }
}


std::string
UserQuerySelect::getProxyOrderBy() const {
    return _qSession->getProxyOrderBy();
}


/// Begin running on all chunks added so far.
void UserQuerySelect::submit() {
    _qSession->finalize();

    // has to be done after result table name
    _setupMerger();

    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << " UserQuerySelect beginning submission");
    assert(_infileMerger);

    auto taskMsgFactory = std::make_shared<qproc::TaskMsgFactory>(_qMetaQueryId);
    TmpTableName ttn(_qMetaQueryId, _qSession->getOriginal());
    std::vector<int> chunks;
    std::mutex chunksMtx;
    int sequence = 0;

    auto queryTemplates = _qSession->makeQueryTemplates();

    std::atomic<int> addTimeSum; // TEMPORARY-timing

    // Writing query for each chunk, stop if query is cancelled.
    auto startAllQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing

#if 1
    // attempt to change priority, requires root
    bool increaseThreadPriority = false;  // TODO: add to configuration
    sched_param sch;
    int policy;
    int originalPriority = 0;
    if (increaseThreadPriority) {
        pthread_getschedparam(pthread_self(), &policy, &sch);
        LOGS(_log, LOG_LVL_DEBUG, "thread priority original policy=" << policy <<
                                  " sch=" << sch.sched_priority);
        originalPriority = sch.sched_priority;
        sch.sched_priority = 10; // TODO:DM-10273 add to configuration
        int result = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sch);
        if (result) {
            LOGS(_log, LOG_LVL_ERROR, "failed to set thread priority a result=" << result <<
                                      " EPERM=" << EPERM);
        }
    }

    _executive->setJobCount(_qSession->getChunksSize());

    for(auto i = _qSession->cQueryBegin(), e = _qSession->cQueryEnd();
             i != e && !_executive->getCancelled(); ++i) {
        auto& chunkSpec = *i;

        std::function<void(util::CmdData*)> funcBuildJob =
               [this, sequence,     // sequence must be a copy
                &chunkSpec, &queryTemplates,
                &chunks, &chunksMtx, &ttn,
                &taskMsgFactory, &addTimeSum](util::CmdData*) {

            auto startbuildQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing
            qproc::ChunkQuerySpec::Ptr cs;
            {
                std::lock_guard<std::mutex> lock(chunksMtx);
                cs = _qSession->buildChunkQuerySpec(queryTemplates, chunkSpec);
                chunks.push_back(cs->chunkId);
            }
            std::string chunkResultName = ttn.make(cs->chunkId);

            std::shared_ptr<ChunkMsgReceiver> cmr = ChunkMsgReceiver::newInstance(cs->chunkId, _messageStore);
            ResourceUnit ru;
            ru.setAsDbChunk(cs->db, cs->chunkId);
            qdisp::JobDescription::Ptr jobDesc = qdisp::JobDescription::create(
                    _executive->getId(), sequence, ru,
                    std::make_shared<MergingHandler>(cmr, _infileMerger, chunkResultName),
                    taskMsgFactory, cs, chunkResultName);
            _executive->add(jobDesc);
            auto endChunkAddQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing
            { // TEMPORARY-timing
                addTimeSum += timeDiff(startbuildQSJ, endChunkAddQSJ);
            }
        };

        auto cmd = std::make_shared<qdisp::PriorityCommand>(funcBuildJob);
        _executive->queueJobStart(cmd, _qSession->getScanInteractive());
        ++sequence;
    }


    // attempt to restore original thread priority, requires root
    if (increaseThreadPriority) {
        sch.sched_priority = originalPriority;
        int result = pthread_setschedparam(pthread_self(), policy, &sch);
        if (result) {
            LOGS(_log, LOG_LVL_ERROR, "failed to set priority b result=" << result);
        }
        pthread_getschedparam(pthread_self(), &policy, &sch);
        LOGS(_log, LOG_LVL_DEBUG, "thread end set priority policy=" << policy <<
                                  " sch=" << sch.sched_priority);
    }

#else
    // This block increases the rate at which jobs get sent to the workers but eats more
    // CPU time and sends the chunks significantly out of order, which has detrimental
    // effects when the workers can return the answer very quickly.
    // So, it is helpful in "select count(*) from Object", but that's about it.
    // Make ChunkSpecVectors of up to 20,000 items from _qSession->cQueryBegin()
    struct ChunkSpecPtrId {
        ChunkSpecPtrId(qproc::ChunkSpec *chunkSpec_, int seq) : chunkSpec(chunkSpec_), sequence(seq) {}
        qproc::ChunkSpec *chunkSpec;
        int sequence;
    };
    typedef std::vector<ChunkSpecPtrId> ChunkSpecPtrIdVect;


    // Function for the loop thread
    auto funcOuterLoop = [this, &queryTemplates,
            &chunks, &chunksMtx, &ttn,
            &taskMsgFactory, &addTimeSum](std::shared_ptr<ChunkSpecPtrIdVect> chunkPtrIdVect){
      for (auto const& chunkPtrId : *chunkPtrIdVect) {
          util::InstanceCount("funcOuterLoop");
          if (_executive->getCancelled()) return;

          qproc::ChunkSpec &chunkSpec = *(chunkPtrId.chunkSpec);
          int sequence = chunkPtrId.sequence;


          // Function for the command
          std::function<void(util::CmdData*)> funcBuildJob =
                 [this, sequence,
                  &chunkSpec, &queryTemplates,
                  &chunks, &chunksMtx, &ttn,
                  &taskMsgFactory, &addTimeSum](util::CmdData*) {
              util::InstanceCount("funbuildJob");
              auto startbuildQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing
              qproc::ChunkQuerySpec::Ptr cs;
              {
                  std::lock_guard<std::mutex> lock(chunksMtx);
                  cs = _qSession->buildChunkQuerySpec(queryTemplates, chunkSpec);

                  chunks.push_back(cs->chunkId);
              }
              std::string chunkResultName = ttn.make(cs->chunkId);

              std::shared_ptr<ChunkMsgReceiver> cmr = ChunkMsgReceiver::newInstance(cs->chunkId, _messageStore);
              ResourceUnit ru;
              ru.setAsDbChunk(cs->db, cs->chunkId);
              qdisp::JobDescription::Ptr jobDesc = qdisp::JobDescription::create(
                      _executive->getId(), sequence, ru,
                      std::make_shared<MergingHandler>(cmr, _infileMerger, chunkResultName),
                      taskMsgFactory, cs, chunkResultName);
              _executive->add(jobDesc);
              auto endChunkAddQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing
              { // TEMPORARY-timing
                  addTimeSum += timeDiff(startbuildQSJ, endChunkAddQSJ);
              }
          };

          auto cmd = std::make_shared<qdisp::PriorityCommand>(funcBuildJob);
          _executive->queueJobStart(cmd, _qSession->getScanInteractive());
      }
    };


    // Create threads to loop through subsets of the chunks for the query.
    std::vector<std::thread> loopThreads;
    auto chunkVectP = std::make_shared<ChunkSpecPtrIdVect>();
    _executive->setJobCount(_qSession->getChunksSize());
    LOGS(_log, LOG_LVL_DEBUG, "priQ jobCount=" << _qSession->getChunksSize());

    for(auto i = _qSession->cQueryBegin(), e = _qSession->cQueryEnd(); i != e; ++i) {
        chunkVectP->emplace_back(&(*i), sequence++);

        if (chunkVectP->size() >= 10000) {
            LOGS(_log, LOG_LVL_DEBUG, "priQ creating loop sz=" << chunkVectP->size());
            // Start a new thread with this chunkVector
            std::thread threadOuterLoop(funcOuterLoop, chunkVectP);
            loopThreads.push_back(std::move(threadOuterLoop));
            chunkVectP = std::make_shared<ChunkSpecPtrIdVect>();
        }
    }

    if (chunkVectP->size() > 0) {
        // Start a new thread with this chunkVector
        std::thread threadOuterLoop(funcOuterLoop, chunkVectP);
        loopThreads.push_back(std::move(threadOuterLoop));
    }
    chunkVectP.reset();

    LOGS(_log, LOG_LVL_DEBUG, "priQJoin a");

    for (std::thread &t : loopThreads) {
        t.join();
    }

    LOGS(_log, LOG_LVL_DEBUG, "priQJoin b");

#endif

    LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() <<" total jobs in query=" << sequence);
    _executive->waitForAllJobsToStart();
    auto endAllQSJ = std::chrono::system_clock::now(); // TEMPORARY-timing
    { // TEMPORARY-timing
        std::lock_guard<std::mutex> sumLock(_executive->sumMtx);
        LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << "QSJ Total=" <<  timeDiff(startAllQSJ, endAllQSJ)
                << "\nQSJ **sequence=" << sequence
                << "\nQSJ   addTimeSum      =" << addTimeSum
                << "\nQSJ     cancelLockQSEASum =" << _executive->cancelLockQSEASum
                << "\nQSJ     jobQueryQSEASum   =" << _executive->jobQueryQSEASum
                << "\nQSJ     addJobQSEASum     =" << _executive->addJobQSEASum
                << "\nQSJ     trackQSEASum      =" << _executive->trackQSEASum
                << "\nQSJ     endQSEASum        =" << _executive->endQSEASum );

    }

    // we only care about per-chunk info for ASYNC queries
    if (_async) {
        std::lock_guard<std::mutex> lock(chunksMtx);
        _qMetaAddChunks(chunks);
    }
}


/// Block until a submit()'ed query completes.
/// @return the QueryState indicating success or failure
QueryState UserQuerySelect::join() {
    bool successful = _executive->join(); // Wait for all data
    // Since all data are in, run final SQL commands like GROUP BY.
    if (!_infileMerger->finalize()) {
        successful = false;
        LOGS(_log, LOG_LVL_ERROR, getQueryIdString() << " InfileMerger::finalize failed");
        // Error: 1105 SQLSTATE: HY000 (ER_UNKNOWN_ERROR) Message: Unknown error
        _messageStore->addMessage(-1, 1105, "Failure while merging result",
                MessageSeverity::MSG_ERROR);
    }
    try {
        _discardMerger();
    } catch (std::exception const& exc) {
        // exception here means error in qserv logic, we do not want to leak
        // it or expose it to user, just dump it to log
        LOGS(_log, LOG_LVL_ERROR, getQueryIdString() << " exception from _discardMerger: "<< exc.what());
    }
    if (successful) {
        _qMetaUpdateStatus(qmeta::QInfo::COMPLETED);
        LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << " Joined everything (success)");
        return SUCCESS;
    } else if (_killed) {
        // status is already set to ABORTED
        LOGS(_log, LOG_LVL_ERROR, getQueryIdString() << " Joined everything (killed)");
        return ERROR;
    } else {
        _qMetaUpdateStatus(qmeta::QInfo::FAILED);
        LOGS(_log, LOG_LVL_ERROR, getQueryIdString() << " Joined everything (failure!)");
        return ERROR;
    }
}

/// Release resources held by the merger
void UserQuerySelect::_discardMerger() {
    _infileMergerConfig.reset();
    if (_infileMerger && !_infileMerger->isFinished()) {
        throw UserQueryError(getQueryIdString() + " merger unfinished, cannot discard");
    }
    _infileMerger.reset();
}

/// Release resources.
void UserQuerySelect::discard() {
    {
        std::lock_guard<std::mutex> lock(_killMutex);
        if (_killed) {
            return;
        }
    }
    // Make sure resources are released.
    if (_executive && _executive->getNumInflight() > 0) {
        throw UserQueryError(getQueryIdString() + " Executive unfinished, cannot discard");
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
    LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << " Discarded UserQuerySelect");
}

/// Setup merger (for results handling and aggregation)
void UserQuerySelect::_setupMerger() {
    LOGS(_log, LOG_LVL_TRACE, getQueryIdString() << " Setup merger");
    _infileMergerConfig->targetTable = _resultTable;
    _infileMergerConfig->mergeStmt = _qSession->getMergeStmt();
    _infileMerger = std::make_shared<rproc::InfileMerger>(*_infileMergerConfig);
}

void UserQuerySelect::setupChunking() {
    LOGS(_log, LOG_LVL_TRACE, getQueryIdString() << "Setup chunking");
    // Do not throw exceptions here, set _errorExtra .
    std::shared_ptr<qproc::IndexMap> im;
    std::string dominantDb = _qSession->getDominantDb();
    if (dominantDb.empty() || !_qSession->validateDominantDb()) {
        // TODO: Revisit this for L3
        throw UserQueryError(getQueryIdString() + " Couldn't determine dominantDb for dispatch");
    }

    std::shared_ptr<IntSet const> eSet = _qSession->getEmptyChunks();
    {
        eSet = _qSession->getEmptyChunks();
        if (!eSet) {
            eSet = std::make_shared<IntSet>();
            LOGS(_log, LOG_LVL_WARN, getQueryIdString() << " Missing empty chunks info for " << dominantDb);
        }
    }
    // FIXME add operator<< for QuerySession
    LOGS(_log, LOG_LVL_TRACE, getQueryIdString() << " _qSession: " << _qSession);
    if (_qSession->hasChunks()) {
        std::shared_ptr<query::ConstraintVector> constraints = _qSession->getConstraints();
        css::StripingParams partStriping = _qSession->getDbStriping();

        im = std::make_shared<qproc::IndexMap>(partStriping, _secondaryIndex);
        qproc::ChunkSpecVector csv;
        if (constraints) {
            csv = im->getChunks(*constraints);
        } else { // Unconstrained: full-sky
            csv = im->getAllChunks();
        }

        LOGS(_log, LOG_LVL_TRACE, getQueryIdString() << " Chunk specs: " << util::printable(csv));
        // Filter out empty chunks
        for(qproc::ChunkSpecVector::const_iterator i=csv.begin(), e=csv.end();
            i != e;
            ++i) {
            if (eSet->count(i->chunkId) == 0) { // chunk not in empty?
                _qSession->addChunk(*i);
            }
        }
    } else {
        LOGS(_log, LOG_LVL_TRACE, getQueryIdString() << " No chunks added, QuerySession will add dummy chunk");
    }
    _qSession->setScanInteractive();
}

// register query in qmeta database
void UserQuerySelect::qMetaRegister(std::string const& resultLocation, std::string const& msgTableName)
{
    qmeta::QInfo::QType qType = _async ? qmeta::QInfo::ASYNC : qmeta::QInfo::SYNC;
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
            qTemplate += stmt->getQueryTemplate().sqlFragment();
        }
    }

    std::string qMerge;
    auto mergeStmt = _qSession->getMergeStmt();
    if (mergeStmt) {
        qMerge = mergeStmt->getQueryTemplate().sqlFragment();
    }
    std::string proxyOrderBy = _qSession->getProxyOrderBy();
    _resultLoc = resultLocation;
    if (_resultLoc.empty()) {
        // Special token #QID# is replaced with query ID later.
        _resultLoc = "table:result_#QID#";
    }
    qmeta::QInfo qInfo(qType, _qMetaCzarId, user, _qSession->getOriginal(),
                       qTemplate, qMerge, proxyOrderBy, _resultLoc, msgTableName);

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
    _queryIdStr = QueryIdHelper::makeIdStr(_qMetaQueryId);
    LOGS(_log, LOG_LVL_DEBUG, getQueryIdString() << " UserQuery registered " << _qSession->getOriginal());

    // update #QID# with actual query ID
    boost::replace_all(_resultLoc, "#QID#", std::to_string(_qMetaQueryId));

    // guess query result location
    if (_resultLoc.compare(0, 6, "table:") == 0) {
        _resultTable = _resultLoc.substr(6);
    } else {
        // we only support results going to tables for now, abort for anything else
        std::string const msg = "Unexpected result location '" + _resultLoc + "'";
        _messageStore->addMessage(-1, 1146, msg, MessageSeverity::MSG_ERROR);
        throw UserQueryError(getQueryIdString() + _errorExtra);
    }

    if (_executive != nullptr) {
        _executive->setQueryId(_qMetaQueryId);
    } else {
        LOGS(_log, LOG_LVL_WARN, "No Executive, assuming invalid query");
    }

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
            throw UserQueryError(getQueryIdString() + _errorExtra);
        }
    }
}

// update query status in QMeta
void UserQuerySelect::_qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus)
{
    _queryMetadata->completeQuery(_qMetaQueryId, qStatus);
}

// add chunk information to qmeta
void UserQuerySelect::_qMetaAddChunks(std::vector<int> const& chunks)
{
    _queryMetadata->addChunks(_qMetaQueryId, chunks);
}


/// Return this query's QueryId string.
std::string UserQuerySelect::getQueryIdString() const {
    return _queryIdStr;
}

}}} // lsst::qserv::ccontrol
