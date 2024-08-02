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

std::shared_ptr<nlohmann::json> TaskMsgFactory::makeMsgJson(ChunkQuerySpec const& chunkQuerySpec,
                                                            std::string const& chunkResultName,
                                                            QueryId queryId, int jobId, int attemptCount,
                                                            qmeta::CzarId czarId) {
    // TODO:UJ DM-45384 &&& remove duplicate elements from the json message
    // TODO:UJ &&&    see: JobDescription::incrAttemptCountScrubResultsJson
    // TODO:UJ &&&    see: wbase::UberJobData::create
    // TODO:UJ &&&    see: Task::createTasksForChunk
    // TODO:UJ &&&    see: wdb/testQueryRunner.cc
    // TODO:UJ &&&    see: wsched/testSchedulers.cc
    std::string resultTable("Asdfasfd");
    if (!chunkResultName.empty()) {
        resultTable = chunkResultName;
    }

    // TODO:UJ verify that these can be put in the uberjob to reduce duplicates
    //         and the size of the message.
    auto jsJobMsgPtr = std::shared_ptr<nlohmann::json>(
            new nlohmann::json({{"czarId", czarId},
                                {"queryId", queryId},
                                {"jobId", jobId},
                                {"attemptCount", attemptCount},
                                {"querySpecDb", chunkQuerySpec.db},
                                {"scanPriority", chunkQuerySpec.scanInfo.scanRating},
                                {"scanInteractive", chunkQuerySpec.scanInteractive},
                                {"maxTableSize", (cconfig::CzarConfig::instance()->getMaxTableSizeMB())},
                                {"chunkScanTables", nlohmann::json::array()},
                                {"chunkId", chunkQuerySpec.chunkId},
                                {"queryFragments", nlohmann::json::array()}}));

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
            for (unsigned int t = 0; t < (sPtr->queries).size(); t++) {
                LOGS(_log, LOG_LVL_DEBUG, __func__ << " q=" << (sPtr->queries).at(t));
            }
            for (auto const& sbi : sPtr->subChunkIds) {
                LOGS(_log, LOG_LVL_DEBUG, __func__ << " sbi=" << sbi);
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

    return jsJobMsgPtr;
}

void TaskMsgFactory::_addFragmentJson(nlohmann::json& jsFragments, std::string const& resultName,
                                      DbTableSet const& subChunkTables, std::vector<int> const& subchunkIds,
                                      std::vector<std::string> const& queries) {
    nlohmann::json jsFrag = {{"resultTable", resultName},
                             {"queries", nlohmann::json::array()},
                             {"subchunkTables", nlohmann::json::array()},
                             {"subchunkIds", nlohmann::json::array()}};

    auto& jsQueries = jsFrag["queries"];
    for (auto& qry : queries) {
        nlohmann::json jsQry = {{"subQuery", qry}};
        jsQueries.push_back(move(jsQry));
    }

    // Add the db+table pairs to the subchunk.
    auto& jsSubchunkTables = jsFrag["subchunkTables"];
    for (auto& tbl : subChunkTables) {
        nlohmann::json jsSubchunkTbl = {{"scDb", tbl.db}, {"scTable", tbl.table}};
        jsSubchunkTables.push_back(move(jsSubchunkTbl));
        LOGS(_log, LOG_LVL_TRACE, "added dbtbl=" << tbl.db << "." << tbl.table);
    }

    // Add subchunk id numbers
    auto& jsSubchunkIds = jsFrag["subchunkIds"];
    for (auto& subchunkId : subchunkIds) {
        jsSubchunkIds.push_back(subchunkId);
    }

    jsFragments.push_back(move(jsFrag));
}

}  // namespace lsst::qserv::qproc
