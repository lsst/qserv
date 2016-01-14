// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
#include "proto/worker.pb.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/QueryProcessingBug.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.TaskMsgFactory");
}

namespace lsst {
namespace qserv {
namespace qproc {


////////////////////////////////////////////////////////////////////////
// class TaskMsgFactory::Impl
////////////////////////////////////////////////////////////////////////
class TaskMsgFactory::Impl {
public:
    Impl(uint64_t session, std::string const& resultTable)
        : _session(session), _resultTable(resultTable) {
    }
    std::shared_ptr<proto::TaskMsg> makeMsg(ChunkQuerySpec const& s,
                                            std::string const& chunkResultName);
private:
    template <class C1, class C2, class C3>
    void addFragment(proto::TaskMsg& m, std::string const& resultName,
                     C1 const& subChunkTables,
                     C2 const& subChunkIds,
                     C3 const& queries) {
        proto::TaskMsg::Fragment* frag = m.add_fragment();
        frag->set_resulttable(resultName);
        // For each query, apply: frag->add_query(q)
        for(typename C3::const_iterator i=queries.begin();
            i != queries.end(); ++i) {
            frag->add_query(*i);
        }
        proto::TaskMsg_Subchunk sc;
        for(typename C1::const_iterator i=subChunkTables.begin();
            i != subChunkTables.end(); ++i) {
            sc.add_table(*i);
        }
        // For each int in subChunks, apply: sc.add_subchunk(1000000);
        std::for_each(
            subChunkIds.begin(), subChunkIds.end(),
            std::bind1st(std::mem_fun(&proto::TaskMsg_Subchunk::add_id), &sc));
        frag->mutable_subchunks()->CopyFrom(sc);
    }

    uint64_t _session;
    std::string _resultTable;
    std::shared_ptr<proto::TaskMsg> _taskMsg;
};

std::shared_ptr<proto::TaskMsg>
TaskMsgFactory::Impl::makeMsg(ChunkQuerySpec const& s,
                              std::string const& chunkResultName) {
    std::string resultTable = _resultTable;
    if (!chunkResultName.empty()) { resultTable = chunkResultName; }
    _taskMsg = std::make_shared<proto::TaskMsg>();
    // shared
    _taskMsg->set_session(_session);
    _taskMsg->set_db(s.db);
    _taskMsg->set_protocol(2);
    // scanTables (for shared scans)
    // check if more than 1 db in scanInfo
    std::string db = "";
    for(auto const& sTbl : s.scanInfo.infoTables) {
        if (db.empty()) {
            db = sTbl.db;
        } else if (db != sTbl.db) {
            throw QueryProcessingBug("Multiple dbs prohibited");
        }
    }

    for(auto const& sTbl : s.scanInfo.infoTables) {
        lsst::qserv::proto::TaskMsg_ScanTable *msgScanTbl = _taskMsg->add_scantable();
        msgScanTbl->set_db(sTbl.db);
        msgScanTbl->set_table(sTbl.table);
        msgScanTbl->set_lockinmemory(sTbl.lockInMemory);
        msgScanTbl->set_scanspeed(sTbl.scanSpeed);
    }

    _taskMsg->set_scanpriority(s.scanInfo.priority);

    // per-chunk
    _taskMsg->set_chunkid(s.chunkId);
    // per-fragment
    // TODO refactor to simplify
    if (s.nextFragment.get()) {
        ChunkQuerySpec const* sPtr = &s;
        while(sPtr) {
            LOGS(_log, LOG_LVL_DEBUG, "nextFragment");
            for(unsigned int t=0;t<(sPtr->queries).size();t++){
                LOGS(_log, LOG_LVL_DEBUG, (sPtr->queries).at(t));
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            addFragment(*_taskMsg, resultTable,
                        s.subChunkTables,
                        sPtr->subChunkIds,
                        sPtr->queries);
            sPtr = sPtr->nextFragment.get();
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "no nextFragment");
        for(unsigned int t=0;t<(s.queries).size();t++){
            LOGS(_log, LOG_LVL_DEBUG, (s.queries).at(t));
        }
        addFragment(*_taskMsg, resultTable,
                    s.subChunkTables, s.subChunkIds, s.queries);
    }
    return _taskMsg;
}


////////////////////////////////////////////////////////////////////////
// class TaskMsgFactory
////////////////////////////////////////////////////////////////////////
TaskMsgFactory::TaskMsgFactory(uint64_t session)
    : _impl(std::make_shared<Impl>(session, "Asdfasfd" )) {
}

void TaskMsgFactory::serializeMsg(ChunkQuerySpec const& s,
                                  std::string const& chunkResultName,
                                  std::ostream& os) {
    std::shared_ptr<proto::TaskMsg> m = _impl->makeMsg(s, chunkResultName);
    m->SerializeToOstream(&os);
}

}}} // namespace lsst::qserv::qproc
