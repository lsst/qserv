// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 AURA/LSST.
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
  * @brief TaskMsgFactory is a factory for TaskMsg (protobuf) objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qproc/TaskMsgFactory.h"

// System headers
#include <stdexcept>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qproc/ChunkQuerySpec.h"
#include "qproc/QueryProcessingBug.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.TaskMsgFactory");
}

namespace lsst {
namespace qserv {
namespace qproc {


std::shared_ptr<proto::TaskMsg> TaskMsgFactory::_makeMsg(ChunkQuerySpec const& chunkQuerySpec,
                                                        std::string const& chunkResultName,
                                                        uint64_t queryId, int jobId) {
    std::string resultTable("Asdfasfd");
    if (!chunkResultName.empty()) { resultTable = chunkResultName; }
    auto taskMsg = std::make_shared<proto::TaskMsg>();
    // shared
    taskMsg->set_session(_session);
    taskMsg->set_db(chunkQuerySpec.db);
    taskMsg->set_protocol(2);
    taskMsg->set_queryid(queryId);
    taskMsg->set_jobid(jobId);
    // scanTables (for shared scans)
    // check if more than 1 db in scanInfo
    std::string db;
    for(auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        if (db.empty()) {
            db = sTbl.db;
        } else if (db != sTbl.db) {
            throw QueryProcessingBug("Multiple dbs prohibited");
        }
    }

    for(auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        lsst::qserv::proto::TaskMsg_ScanTable *msgScanTbl = taskMsg->add_scantable();
        sTbl.copyToScanTable(msgScanTbl);
    }

    taskMsg->set_scanpriority(chunkQuerySpec.scanInfo.scanRating);
    taskMsg->set_scaninteractive(chunkQuerySpec.scanInteractive);

    // per-chunk
    taskMsg->set_chunkid(chunkQuerySpec.chunkId);
    // per-fragment
    // TODO refactor to simplify
    if (chunkQuerySpec.nextFragment.get()) {
        ChunkQuerySpec const* sPtr = &chunkQuerySpec;
        while(sPtr) {
            LOGS(_log, LOG_LVL_DEBUG, "nextFragment");
            for(unsigned int t=0;t<(sPtr->queries).size();t++){
                LOGS(_log, LOG_LVL_DEBUG, (sPtr->queries).at(t));
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables,
                         sPtr->subChunkIds, sPtr->queries);
            sPtr = sPtr->nextFragment.get();
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "no nextFragment");
        for(unsigned int t=0;t<(chunkQuerySpec.queries).size();t++){
            LOGS(_log, LOG_LVL_DEBUG, (chunkQuerySpec.queries).at(t));
        }
        _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables,
                     chunkQuerySpec.subChunkIds, chunkQuerySpec.queries);
    }
    return taskMsg;
}


void TaskMsgFactory::_addFragment(proto::TaskMsg& taskMsg, std::string const& resultName,
                                  std::vector<std::string> const& subChunkTables,
                                  std::vector<int> const& subChunkIds,
                                  std::vector<std::string> const& queries) {
     proto::TaskMsg::Fragment* frag = taskMsg.add_fragment();
     frag->set_resulttable(resultName);

     for(auto& qry : queries)  {
         frag->add_query(qry);
     }

     proto::TaskMsg_Subchunk sc;

     for(auto& tbl : subChunkTables) {
         sc.add_table(tbl);
     }

     for(auto& subChunkId : subChunkIds) {
         sc.add_id(subChunkId);
     }

     frag->mutable_subchunks()->CopyFrom(sc);
}


void TaskMsgFactory::serializeMsg(ChunkQuerySpec const& s,
                                  std::string const& chunkResultName,
                                  uint64_t queryId, int jobId,
                                  std::ostream& os) {
    std::shared_ptr<proto::TaskMsg> m = _makeMsg(s, chunkResultName, queryId, jobId);
    m->SerializeToOstream(&os);
}

}}} // namespace lsst::qserv::qproc
