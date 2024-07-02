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
#include "nlohmann/json.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "qproc/ChunkQuerySpec.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.TaskMsgFactory");
}

using namespace std;

namespace lsst::qserv::qproc {

//&&&uj - Probaly just delete this
bool TaskMsgFactory::fillTaskMsg(proto::TaskMsg* taskMsg, ChunkQuerySpec const& chunkQuerySpec,
                                 std::string const& chunkResultName, QueryId queryId, int jobId,
                                 int attemptCount, qmeta::CzarId czarId) {
    std::string resultTable("Asdfasfd");
    if (!chunkResultName.empty()) {
        resultTable = chunkResultName;
    }
    // shared
    taskMsg->set_db(chunkQuerySpec.db);
    taskMsg->set_queryid(queryId);
    taskMsg->set_jobid(jobId);
    taskMsg->set_attemptcount(attemptCount);
    taskMsg->set_czarid(czarId);
    // LOGS(_log, LOG_LVL_INFO, "&&& _makeMsg ses=" << _session << " db=" << chunkQuerySpec.db << " qId=" <<
    // queryId << " jId=" << jobId << " att=" << attemptCount << " cz=" << czarId);
    //  scanTables (for shared scans)
    //  check if more than 1 db in scanInfo
    std::string db;
    for (auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        if (db.empty()) {
            db = sTbl.db;
        }
    }

    for (auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        lsst::qserv::proto::TaskMsg_ScanTable* msgScanTbl = taskMsg->add_scantable();
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
        while (sPtr) {
            LOGS(_log, LOG_LVL_TRACE, "nextFragment");
            for (unsigned int t = 0; t < (sPtr->queries).size(); t++) {
                LOGS(_log, LOG_LVL_TRACE, (sPtr->queries).at(t));
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables, sPtr->subChunkIds,
                         sPtr->queries);
            sPtr = sPtr->nextFragment.get();
        }
    } else {
        LOGS(_log, LOG_LVL_TRACE, "no nextFragment");
        for (unsigned int t = 0; t < (chunkQuerySpec.queries).size(); t++) {
            LOGS(_log, LOG_LVL_TRACE, (chunkQuerySpec.queries).at(t));
        }
        _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables, chunkQuerySpec.subChunkIds,
                     chunkQuerySpec.queries);
    }
    return true;
}

std::shared_ptr<proto::TaskMsg> TaskMsgFactory::_makeMsg(ChunkQuerySpec const& chunkQuerySpec,
                                                         std::string const& chunkResultName, QueryId queryId,
                                                         int jobId, int attemptCount, qmeta::CzarId czarId) {
    std::string resultTable("Asdfasfd");
    if (!chunkResultName.empty()) {
        resultTable = chunkResultName;
    }
    auto taskMsg = std::make_shared<proto::TaskMsg>();
    // shared
    taskMsg->set_db(chunkQuerySpec.db);
    taskMsg->set_queryid(queryId);
    taskMsg->set_jobid(jobId);
    taskMsg->set_attemptcount(attemptCount);
    taskMsg->set_czarid(czarId);
    // scanTables (for shared scans)
    // check if more than 1 db in scanInfo
    std::string db;
    for (auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        if (db.empty()) {
            db = sTbl.db;
        }
    }

    for (auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        lsst::qserv::proto::TaskMsg_ScanTable* msgScanTbl = taskMsg->add_scantable();
        sTbl.copyToScanTable(msgScanTbl);
    }

    taskMsg->set_scanpriority(chunkQuerySpec.scanInfo.scanRating);
    taskMsg->set_scaninteractive(chunkQuerySpec.scanInteractive);
    taskMsg->set_maxtablesize_mb(cconfig::CzarConfig::instance()->getMaxTableSizeMB());

    // per-chunk
    taskMsg->set_chunkid(chunkQuerySpec.chunkId);
    // per-fragment
    // TODO refactor to simplify
    if (chunkQuerySpec.nextFragment.get()) {
        ChunkQuerySpec const* sPtr = &chunkQuerySpec;
        while (sPtr) {
            LOGS(_log, LOG_LVL_TRACE, "nextFragment");
            for (unsigned int t = 0; t < (sPtr->queries).size(); t++) {
                LOGS(_log, LOG_LVL_TRACE, (sPtr->queries).at(t));
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables, sPtr->subChunkIds,
                         sPtr->queries);
            sPtr = sPtr->nextFragment.get();
        }
    } else {
        LOGS(_log, LOG_LVL_TRACE, "no nextFragment");
        for (unsigned int t = 0; t < (chunkQuerySpec.queries).size(); t++) {
            LOGS(_log, LOG_LVL_TRACE, (chunkQuerySpec.queries).at(t));
        }
        _addFragment(*taskMsg, resultTable, chunkQuerySpec.subChunkTables, chunkQuerySpec.subChunkIds,
                     chunkQuerySpec.queries);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& TaskMsgFactory::_makeMsg  end");
    return taskMsg;
}

void TaskMsgFactory::_addFragment(proto::TaskMsg& taskMsg, std::string const& resultName,
                                  DbTableSet const& subChunkTables, std::vector<int> const& subChunkIds,
                                  std::vector<std::string> const& queries) {
    proto::TaskMsg::Fragment* frag = taskMsg.add_fragment();
    frag->set_resulttable(resultName);

    for (auto& qry : queries) {
        frag->add_query(qry);
    }

    proto::TaskMsg_Subchunk sc;

    // Add the db+table pairs to the subchunk.
    for (auto& tbl : subChunkTables) {
        proto::TaskMsg_Subchunk_DbTbl* dbTbl = sc.add_dbtbl();
        dbTbl->set_db(tbl.db);
        dbTbl->set_tbl(tbl.table);
        LOGS(_log, LOG_LVL_TRACE, "added dbtbl=" << tbl.db << "." << tbl.table);
    }

    for (auto& subChunkId : subChunkIds) {
        sc.add_id(subChunkId);
    }

    frag->mutable_subchunks()->CopyFrom(sc);
}

void TaskMsgFactory::serializeMsg(ChunkQuerySpec const& s, std::string const& chunkResultName,
                                  QueryId queryId, int jobId, int attemptCount, qmeta::CzarId czarId,
                                  std::ostream& os) {
    std::shared_ptr<proto::TaskMsg> m = _makeMsg(s, chunkResultName, queryId, jobId, attemptCount, czarId);
    m->SerializeToOstream(&os);
}

std::shared_ptr<nlohmann::json> TaskMsgFactory::makeMsgJson(ChunkQuerySpec const& chunkQuerySpec,
                                                            std::string const& chunkResultName,
                                                            QueryId queryId, int jobId, int attemptCount,
                                                            qmeta::CzarId czarId) {
    LOGS(_log, LOG_LVL_WARN, "&&& TaskMsgFactory::makeMsgJson  start");
    std::string resultTable("Asdfasfd");
    if (!chunkResultName.empty()) {
        resultTable =
                chunkResultName;  // &&&uj this probably needs to be replaced with whatever it should be now.
    }

    auto jsJobMsgPtr = std::shared_ptr<nlohmann::json>(new nlohmann::json(
            {{"czarId", czarId},
             {"queryId", queryId},
             {"jobId", jobId},
             {"attemptCount", attemptCount},
             {"querySpecDb", chunkQuerySpec.db},
             {"scanPriority", chunkQuerySpec.scanInfo.scanRating},
             {"scanInteractive", chunkQuerySpec.scanInteractive},
             {"maxTableSize", (cconfig::CzarConfig::instance()->getMaxTableSizeMB())},
             {"chunkScanTables", nlohmann::json::array()},
             {"chunkId", chunkQuerySpec.chunkId},
             {"queryFragments", nlohmann::json::array()}}));  // &&&uj verify that these can be put in the
                                                              // uberjob to reduce copies.

    auto& jsJobMsg = *jsJobMsgPtr;

    auto& chunkScanTables = jsJobMsg["chunkScanTables"];
    for (auto const& sTbl : chunkQuerySpec.scanInfo.infoTables) {
        nlohmann::json cst = {{"db", sTbl.db},
                              {"table", sTbl.table},
                              {"lockInMemory", sTbl.lockInMemory},
                              {"tblScanRating", sTbl.scanRating}};
        chunkScanTables.push_back(move(cst));
    }

    auto& jsFragments = jsJobMsg["queryFragments"];
    if (chunkQuerySpec.nextFragment.get()) {
        ChunkQuerySpec const* sPtr = &chunkQuerySpec;
        while (sPtr) {
            LOGS(_log, LOG_LVL_TRACE, "nextFragment");
            for (unsigned int t = 0; t < (sPtr->queries).size(); t++) {  // &&& delete block
                LOGS(_log, LOG_LVL_TRACE, (sPtr->queries).at(t));
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC q=" << (sPtr->queries).at(t));
            }
            for (auto const& sbi : sPtr->subChunkIds) {  // &&& delete block
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC sbi=" << sbi);
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            _addFragmentJson(jsFragments, resultTable, chunkQuerySpec.subChunkTables, sPtr->subChunkIds,
                             sPtr->queries);
            sPtr = sPtr->nextFragment.get();
        }
    } else {
        LOGS(_log, LOG_LVL_TRACE, "no nextFragment");
        for (unsigned int t = 0; t < (chunkQuerySpec.queries).size(); t++) {
            LOGS(_log, LOG_LVL_TRACE, (chunkQuerySpec.queries).at(t));
        }
        _addFragmentJson(jsFragments, resultTable, chunkQuerySpec.subChunkTables, chunkQuerySpec.subChunkIds,
                         chunkQuerySpec.queries);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& TaskMsgFactory::makeMsg  end " << jsJobMsg);
    return jsJobMsgPtr;
}

void TaskMsgFactory::_addFragmentJson(nlohmann::json& jsFragments, std::string const& resultName,
                                      DbTableSet const& subChunkTables, std::vector<int> const& subchunkIds,
                                      std::vector<std::string> const& queries) {
    nlohmann::json jsFrag = {{"resultTable", resultName},
                             {"queries", nlohmann::json::array()},
                             {"subchunkTables", nlohmann::json::array()},
                             {"subchunkIds", nlohmann::json::array()}};
    LOGS(_log, LOG_LVL_WARN, "&&&SUBCa jsFrag=" << jsFrag);

    string tmp("&&&SUBCa queries=");
    for (auto const& qstr : queries) {
        tmp += "~";
        tmp += qstr + "~";
    }
    LOGS(_log, LOG_LVL_WARN, __func__ << tmp);

    tmp = "&&&SUBCa scids=";
    for (auto const& scid : subchunkIds) {
        tmp += "~";
        tmp += to_string(scid) + "~";
    }
    LOGS(_log, LOG_LVL_WARN, __func__ << tmp);

    auto& jsQueries = jsFrag["queries"];
    for (auto& qry : queries) {
        nlohmann::json jsQry = {{"subQuery", qry}};
        jsQueries.push_back(move(jsQry));
    }
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jsQueries=" << jsQueries);

    // Add the db+table pairs to the subchunk.
    auto& jsSubchunkTables = jsFrag["subchunkTables"];
    for (auto& tbl : subChunkTables) {
        nlohmann::json jsSubchunkTbl = {{"scDb", tbl.db}, {"scTable", tbl.table}};
        jsSubchunkTables.push_back(move(jsSubchunkTbl));
        LOGS(_log, LOG_LVL_TRACE, "added dbtbl=" << tbl.db << "." << tbl.table);
    }
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jsSubchunkTables=" << jsSubchunkTables);

    // Add subchunk id numbers
    auto& jsSubchunkIds = jsFrag["subchunkIds"];
    for (auto& subchunkId : subchunkIds) {
        jsSubchunkIds.push_back(subchunkId);
    }
    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBC jsSubchunkIds=" << jsSubchunkIds);

    LOGS(_log, LOG_LVL_WARN, __func__ << "&&&SUBCz jsFrag=" << jsFrag);
    jsFragments.push_back(move(jsFrag));
}

}  // namespace lsst::qserv::qproc
