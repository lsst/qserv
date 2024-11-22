/*
 * LSST Data Management System
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

// Class header
#include "protojson/UberJobMsg.h"

#include <stdexcept>

// Qserv headers
#include "http/Client.h"
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"
#include "qdisp/JobQuery.h"
#include "qdisp/JobDescription.h"
#include "qproc/ChunkQuerySpec.h"
#include "util/common.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.UberJobMsg");
}  // namespace

namespace lsst::qserv::protojson {

UberJobMsg::UberJobMsg(unsigned int metaVersion, std::string const& replicationInstanceId,
                       std::string const& replicationAuthKey,
                       //&&&CzarContactInfo::Ptr const& czInfo, WorkerContactInfo::Ptr const& wInfo,
                       CzarContactInfo::Ptr const& czInfo, string const& workerId, QueryId qId,
                       UberJobId ujId, int rowLimit, int maxTableSizeMB,
                       std::vector<std::shared_ptr<qdisp::JobQuery>> const& jobs)
        : _metaVersion(metaVersion),
          _replicationInstanceId(replicationInstanceId),
          _replicationAuthKey(replicationAuthKey),
          _czInfo(czInfo),
          _workerId(workerId),
          //&&&_workerId(wInfo->wId),
          //&&&_wInfo(wInfo),
          _qId(qId),
          _ujId(ujId),
          _rowLimit(rowLimit),
          _maxTableSizeMB(maxTableSizeMB) {
    //&&&_jobs(jobs) {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::UberJobMsg start");

    for (auto& jobPtr : jobs) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::UberJobMsg loop");
        // This creates the JobMsg objects for all relates jobs and their fragments.
        auto jobMsg = JobMsg::create(jobPtr, _jobSubQueryTempMap, _jobDbTablesMap);
        _jobMsgVect.push_back(jobMsg);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::UberJobMsg end");
}

json UberJobMsg::serializeJson() const {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::serializeJson a");

    json ujmJson = {{"version", _metaVersion},
                    {"instance_id", _replicationInstanceId},
                    {"auth_key", _replicationAuthKey},
                    {"worker", _workerId},
                    {"queryid", _qId},
                    {"uberjobid", _ujId},
                    {"czarinfo", _czInfo->serializeJson()},
                    {"rowlimit", _rowLimit},
                    {"subqueries_map", _jobSubQueryTempMap->serializeJson()},
                    {"dbtables_map", _jobDbTablesMap->serializeJson()},
                    {"maxtablesizemb", _maxTableSizeMB},
                    {"jobs", json::array()}};
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::serializeJson b");

    auto& jsJobs = ujmJson["jobs"];
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::serializeJson c");
    for (auto const& jbMsg : _jobMsgVect) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::serializeJson c1");
        json jsJob = jbMsg->serializeJson();
        jsJobs.push_back(jsJob);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::serializeJson d");

    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& ujmJson=" << ujmJson);

    return ujmJson;
}

UberJobMsg::Ptr UberJobMsg::createFromJson(nlohmann::json const& ujmJson) {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson a");
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson ujmJson=" << ujmJson);
    try {
        if (ujmJson["version"] != http::MetaModule::version) {
            LOGS(_log, LOG_LVL_ERROR, "UberJobMsg::createFromJson bad version " << ujmJson["version"]);
            return nullptr;
        }

        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson b");
        auto czInfo_ = CzarContactInfo::createFromJson(ujmJson["czarinfo"]);
        if (czInfo_ == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, "UberJobMsg::createFromJson czar could not be parsed in " << ujmJson);
            return nullptr;
        }

        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson c");
        auto metaVersion = http::RequestBodyJSON::required<unsigned int>(ujmJson, "version");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson d");
        auto replicationInstanceId = http::RequestBodyJSON::required<string>(ujmJson, "instance_id");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson e");
        auto replicationAuthKey = http::RequestBodyJSON::required<string>(ujmJson, "auth_key");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson f");
        auto workerId = http::RequestBodyJSON::required<string>(ujmJson, "worker");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson g");
        auto qId = http::RequestBodyJSON::required<QueryId>(ujmJson, "queryid");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson h");
        auto ujId = http::RequestBodyJSON::required<UberJobId>(ujmJson, "uberjobid");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson i");
        auto rowLimit = http::RequestBodyJSON::required<int>(ujmJson, "rowlimit");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson j");
        auto maxTableSizeMB = http::RequestBodyJSON::required<int>(ujmJson, "maxtablesizemb");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson k");
        auto czInfo = CzarContactInfo::createFromJson(ujmJson["czarinfo"]);
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson l");
        auto jsUjJobs = http::RequestBodyJSON::required<json>(ujmJson, "jobs");

        LOGS(_log, LOG_LVL_INFO,
             " &&& " << metaVersion << replicationInstanceId << replicationAuthKey << workerId << qId << ujId
                     << rowLimit << jsUjJobs);

        std::vector<std::shared_ptr<qdisp::JobQuery>> emptyJobs;

        Ptr ujmPtr = Ptr(new UberJobMsg(metaVersion, replicationInstanceId, replicationAuthKey, czInfo,
                                        workerId, qId, ujId, rowLimit, maxTableSizeMB, emptyJobs));

        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson m");
        auto const& jsSubQueriesMap = http::RequestBodyJSON::required<json>(ujmJson, "subqueries_map");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson n");
        ujmPtr->_jobSubQueryTempMap = JobSubQueryTempMap::createFromJson(jsSubQueriesMap);

        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson o");
        auto jsDbTablesMap = http::RequestBodyJSON::required<json>(ujmJson, "dbtables_map");
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson p");
        ujmPtr->_jobDbTablesMap = JobDbTablesMap::createFromJson(jsDbTablesMap);

        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson q");
        for (auto const& jsUjJob : jsUjJobs) {
            LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson q1");
            JobMsg::Ptr jobMsgPtr =
                    JobMsg::createFromJson(jsUjJob, ujmPtr->_jobSubQueryTempMap, ujmPtr->_jobDbTablesMap);
            ujmPtr->_jobMsgVect.push_back(jobMsgPtr);
        }
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson end");

        return ujmPtr;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "UberJobMsg::createFromJson invalid " << exc.what() << " json=" << ujmJson);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobMsg::createFromJson end error");
    return nullptr;
}

std::string UberJobMsg::dump() const {
    stringstream os;
    os << "&&& NEEDS CODE";
    return os.str();
}

JobMsg::Ptr JobMsg::create(std::shared_ptr<qdisp::JobQuery> const& jobPtr,
                           JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                           JobDbTablesMap::Ptr const& jobDbTablesMap) {
    auto jMsg = Ptr(new JobMsg(jobPtr, jobSubQueryTempMap, jobDbTablesMap));
    return jMsg;
}

JobMsg::JobMsg(std::shared_ptr<qdisp::JobQuery> const& jobPtr,
               JobSubQueryTempMap::Ptr const& jobSubQueryTempMap, JobDbTablesMap::Ptr const& jobDbTablesMap)
        : _jobSubQueryTempMap(jobSubQueryTempMap), _jobDbTablesMap(jobDbTablesMap) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg start");
    auto const descr = jobPtr->getDescription();
    if (descr == nullptr) {
        throw util::Bug(ERR_LOC, cName(__func__) + " description=null for job=" + jobPtr->getIdStr());
    }
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg a");
    auto chunkQuerySpec = descr->getChunkQuerySpec();
    _jobId = descr->id();
    //&&&{"attemptCount", attemptCount},
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg b");
    _attemptCount = descr->getAttemptCount();  // &&& may need to increment descr->AttemptCount at this time.
    //&&&{"querySpecDb", chunkQuerySpec.db},
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg c");
    _chunkQuerySpecDb = chunkQuerySpec->db;
    //&&&{"scanPriority", chunkQuerySpec.scanInfo.scanRating},
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg d");
    _scanRating = chunkQuerySpec->scanInfo.scanRating;
    //&&&{"scanInteractive", chunkQuerySpec.scanInteractive},
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg e");
    _scanInteractive = chunkQuerySpec->scanInteractive;
    //&&&{"maxTableSize", (cconfig::CzarConfig::instance()->getMaxTableSizeMB())},
    //_maxTableSizeMB; // &&& move up to UberJob
    //&&&{"chunkScanTables", nlohmann::json::array()},
    //&&&{"chunkId", chunkQuerySpec.chunkId},
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg f");
    _chunkId = chunkQuerySpec->chunkId;
    //&&&{"queryFragments", nlohmann::json::array()}}));
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg g");
    _chunkResultName = descr->getChunkResultName();

    // Add scan tables (&&& not sure is this is the same for all jobs or not)
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg h");
    for (auto const& sTbl : chunkQuerySpec->scanInfo.infoTables) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg h1");
        /* &&&
        nlohmann::json cst = {{"db", sTbl.db},
                              {"table", sTbl.table},
                              {"lockInMemory", sTbl.lockInMemory},
                              {"tblScanRating", sTbl.scanRating}};
        chunkScanTables.push_back(move(cst));
        */
        int index = jobDbTablesMap->findDbTable(make_pair(sTbl.db, sTbl.table));
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg h2");
        jobDbTablesMap->setScanRating(index, sTbl.scanRating, sTbl.lockInMemory);
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg h3");
        _chunkScanTableIndexes.push_back(index);
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg h4");
    }

    // Add fragments
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg i");
    _jobFragments =
            JobFragment::createVect(*chunkQuerySpec, jobSubQueryTempMap, jobDbTablesMap, _chunkResultName);
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::JobMsg end");
}

nlohmann::json JobMsg::serializeJson() const {
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson a");
    auto jsJobMsg =
            nlohmann::json({//&&&{"czarId", czarId},
                            //&&&{"queryId", queryId},
                            {"jobId", _jobId},
                            {"attemptCount", _attemptCount},
                            {"querySpecDb", _chunkQuerySpecDb},
                            {"scanPriority", _scanRating},
                            {"scanInteractive", _scanInteractive},
                            //&&&{"maxTableSize", (cconfig::CzarConfig::instance()->getMaxTableSizeMB())},
                            //&&&{"chunkScanTables", nlohmann::json::array()},
                            {"chunkId", _chunkId},
                            {"chunkresultname", _chunkResultName},
                            {"chunkscantables_indexes", nlohmann::json::array()},
                            {"queryFragments", json::array()}});

    // These are indexes into _jobDbTablesMap, which is shared between all JobMsg in this UberJobMsg.
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson b");
    auto& jsqCstIndexes = jsJobMsg["chunkscantables_indexes"];
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson c");
    for (auto const& index : _chunkScanTableIndexes) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson c1");
        jsqCstIndexes.push_back(index);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson d");
    auto& jsqFrags = jsJobMsg["queryFragments"];
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson e");
    for (auto& jFrag : _jobFragments) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson e1");
        auto jsFrag = jFrag->serializeJson();
        LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson e2");
        jsqFrags.push_back(jsFrag);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::serializeJson end");
    return jsJobMsg;
}

JobMsg::JobMsg(JobSubQueryTempMap::Ptr const& jobSubQueryTempMap, JobDbTablesMap::Ptr const& jobDbTablesMap,
               JobId jobId, int attemptCount, std::string const& chunkQuerySpecDb, int scanRating,
               bool scanInteractive, int chunkId, std::string const& chunkResultName)
        : _jobId(jobId),
          _attemptCount(attemptCount),
          _chunkQuerySpecDb(chunkQuerySpecDb),
          _scanRating(scanRating),
          _scanInteractive(scanInteractive),
          _chunkId(chunkId),
          _chunkResultName(chunkResultName),
          _jobSubQueryTempMap(jobSubQueryTempMap),
          _jobDbTablesMap(jobDbTablesMap) {}

JobMsg::Ptr JobMsg::createFromJson(nlohmann::json const& ujJson,
                                   JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                                   JobDbTablesMap::Ptr const& jobDbTablesMap) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson a");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson ujJson=" << ujJson);
    JobId jobId = http::RequestBodyJSON::required<JobId>(ujJson, "jobId");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson b");
    int attemptCount = http::RequestBodyJSON::required<int>(ujJson, "attemptCount");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson c");
    string chunkQuerySpecDb = http::RequestBodyJSON::required<string>(ujJson, "querySpecDb");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson d");
    int scanRating = http::RequestBodyJSON::required<int>(ujJson, "scanPriority");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson e");
    bool scanInteractive = http::RequestBodyJSON::required<bool>(ujJson, "scanInteractive");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson f");
    int chunkId = http::RequestBodyJSON::required<int>(ujJson, "chunkId");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson g");
    string chunkResultName = http::RequestBodyJSON::required<string>(ujJson, "chunkresultname");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson h");

    json jsQFrags = http::RequestBodyJSON::required<json>(ujJson, "queryFragments");
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson i");

    Ptr jMsgPtr = Ptr(new JobMsg(jobSubQueryTempMap, jobDbTablesMap, jobId, attemptCount, chunkQuerySpecDb,
                                 scanRating, scanInteractive, chunkId, chunkResultName));
    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson j");
    jMsgPtr->_jobFragments = JobFragment::createVectFromJson(
            jsQFrags, jMsgPtr->_jobSubQueryTempMap, jMsgPtr->_jobDbTablesMap, jMsgPtr->_chunkResultName);

    LOGS(_log, LOG_LVL_WARN, "&&& JobMsg::createFromJson end");
    return jMsgPtr;
}

json JobSubQueryTempMap::serializeJson() const {
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson a");

    // std::map<int, std::string> _qTemplateMap;
    json jsSubQueryTemplateMap = {{"subquerytemplate_map", json::array()}};

    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson b");
    LOGS(_log, LOG_LVL_WARN,
         "&&& JobSubQueryTempMap::serializeJson jsSubQueryTemplateMap=" << jsSubQueryTemplateMap);
    auto& jsSqtMap = jsSubQueryTemplateMap["subquerytemplate_map"];
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson c");
    for (auto const& [key, templ] : _qTemplateMap) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson c1");
        json jsElem = {{"index", key}, {"template", templ}};
        LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson c2");
        jsSqtMap.push_back(jsElem);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson e");
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& " << jsSqtMap);

    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::serializeJson end");
    return jsSubQueryTemplateMap;
}

JobSubQueryTempMap::Ptr JobSubQueryTempMap::createFromJson(nlohmann::json const& ujJson) {
    LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson a");
    Ptr sqtMapPtr = create();
    LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson b");
    auto& sqtMap = sqtMapPtr->_qTemplateMap;
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::createFromJson " << ujJson);
    auto const& jsElements = ujJson["subquerytemplate_map"];
    LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c");
    for (auto const& jsElem : jsElements) {
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c1");
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson jsElem=" << jsElem);
        //&&&int index = jsElem["index"];
        int index = http::RequestBodyJSON::required<int>(jsElem, "index");
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c2");
        //&&&string templ = jsElem["template"];
        string templ = http::RequestBodyJSON::required<string>(jsElem, "template");
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c3");
        auto res = sqtMap.insert(make_pair(index, templ));
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c4");
        if (!res.second) {
            throw invalid_argument(sqtMapPtr->cName(__func__) + "index=" + to_string(index) + "=" + templ +
                                   " index already found in " + to_string(ujJson));
        }
        LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson c5");
    }
    LOGS(_log, LOG_LVL_WARN, "JobSubQueryTempMap::createFromJson end");
    return sqtMapPtr;
}

int JobSubQueryTempMap::findSubQueryTemp(string const& qTemp) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp start");
    // The expected number of templates is expected to be small, less than 4,
    // so this shouldn't be horribly expensive.
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp qTemp=" << qTemp);
    for (auto const& [key, temp] : _qTemplateMap) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp key=" << key << " t=" << temp);
        if (temp == qTemp) {
            LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp end key=" << key);
            return key;
        }
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp endloop");
    // Need to insert
    int index = _qTemplateMap.size();
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp index=" << index);
    _qTemplateMap[index] = qTemp;
    LOGS(_log, LOG_LVL_WARN, "&&& JobSubQueryTempMap::findSubQueryTemp end");
    return index;
}

int JobDbTablesMap::findDbTable(pair<string, string> const& dbTablePair) {
    // The expected number of templates is expected to be small, less than 4,
    // so this shouldn't be horribly expensive.
    for (auto const& [key, dbTbl] : _dbTableMap) {
        if (dbTablePair == dbTbl) {
            return key;
        }
    }

    // Need to insert
    int index = _dbTableMap.size();
    _dbTableMap[index] = dbTablePair;
    return index;
}

json JobDbTablesMap::serializeJson() const {
    json jsDbTablesMap = {{"dbtable_map", json::array()}, {"scanrating_map", json::array()}};

    auto& jsDbTblMap = jsDbTablesMap["dbtable_map"];
    for (auto const& [key, valPair] : _dbTableMap) {
        json jsDbTbl = {{"index", key}, {"db", valPair.first}, {"table", valPair.second}};
        jsDbTblMap.push_back(jsDbTbl);
    }

    auto& jsScanRatingMap = jsDbTablesMap["scanrating_map"];
    for (auto const& [key, valPair] : _scanRatingMap) {
        json jsScanR = {{"index", key}, {"scanrating", valPair.first}, {"lockinmem", valPair.second}};
        jsScanRatingMap.push_back(jsScanR);
    }

    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& " << jsDbTablesMap);

    return jsDbTablesMap;
}

JobDbTablesMap::Ptr JobDbTablesMap::createFromJson(nlohmann::json const& ujJson) {
    Ptr dbTablesMapPtr = create();
    auto& dbTblMap = dbTablesMapPtr->_dbTableMap;
    auto& scanRMap = dbTablesMapPtr->_scanRatingMap;

    LOGS(_log, LOG_LVL_WARN, "&&& JobDbTablesMap::createFromJson " << ujJson);

    json const& jsDbTbl = ujJson["dbtable_map"];
    LOGS(_log, LOG_LVL_WARN, "&&& JobDbTablesMap::createFromJson dbtbl=" << jsDbTbl);
    for (auto const& jsElem : jsDbTbl) {
        //&&&int index = jsElem["index"];
        int index = http::RequestBodyJSON::required<int>(jsElem, "index");
        //&&&string db = jsElem["db"];
        string db = http::RequestBodyJSON::required<string>(jsElem, "db");
        //&&&string tbl = jsElem["table"];
        string tbl = http::RequestBodyJSON::required<string>(jsElem, "table");
        auto res = dbTblMap.insert(make_pair(index, make_pair(db, tbl)));
        if (!res.second) {
            throw invalid_argument(dbTablesMapPtr->cName(__func__) + " index=" + to_string(index) + "=" + db +
                                   +"." + tbl + " index already found in " + to_string(jsDbTbl));
        }
    }

    json const& jsScanR = ujJson["scanrating_map"];
    LOGS(_log, LOG_LVL_WARN, "&&& JobDbTablesMap::createFromJson jsScanR=" << jsScanR);
    for (auto const& jsElem : jsScanR) {
        //&&&int index = jsElem["index"];
        int index = http::RequestBodyJSON::required<int>(jsElem, "index");
        //&&&int scanR = jsElem["scanrating"];
        int scanR = http::RequestBodyJSON::required<int>(jsElem, "scanrating");
        //&&&bool lockInMem = jsElem["lockinmem"];
        bool lockInMem = http::RequestBodyJSON::required<bool>(jsElem, "lockinmem");
        auto res = scanRMap.insert(make_pair(index, make_pair(scanR, lockInMem)));
        if (!res.second) {
            throw invalid_argument(dbTablesMapPtr->cName(__func__) + " index=" + to_string(index) + "=" +
                                   to_string(scanR) + +", " + to_string(lockInMem) +
                                   " index already found in " + to_string(jsDbTbl));
        }
    }

    return dbTablesMapPtr;
}

void JobDbTablesMap::setScanRating(int index, int scanRating, bool lockInMemory) {
    auto iter = _scanRatingMap.find(index);
    if (iter == _scanRatingMap.end()) {
        _scanRatingMap[index] = make_pair(scanRating, lockInMemory);
    } else {
        auto& elem = *iter;
        auto& pr = elem.second;
        auto& [sRating, lInMem] = pr;
        if (sRating != scanRating || lInMem != lockInMemory) {
            auto [dbName, tblName] = getDbTable(index);
            LOGS(_log, LOG_LVL_ERROR,
                 cName(__func__) << " unexpected change in scanRating for " << dbName << "." << tblName
                                 << " from " << sRating << " to " << scanRating << " lockInMemory from "
                                 << lInMem << " to " << lockInMemory);
            if (scanRating > sRating) {
                sRating = scanRating;
                lInMem = lockInMemory;
            }
        }
    }
}

JobFragment::JobFragment(JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                         JobDbTablesMap::Ptr const& jobDbTablesMap, std::string const& resultTblName)
        : _jobSubQueryTempMap(jobSubQueryTempMap),
          _jobDbTablesMap(jobDbTablesMap),
          _resultTblName(resultTblName) {
    LOGS(_log, LOG_LVL_WARN,
         "&&& JobFragment::JobFragment _jobSubQueryTempMap!=nullptr=" << (_jobSubQueryTempMap != nullptr));
    LOGS(_log, LOG_LVL_WARN,
         "&&& JobFragment::JobFragment _jobDbTablesMap!=nullptr=" << (_jobDbTablesMap != nullptr));
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::JobFragment resultTblName=" << resultTblName);
}

vector<JobFragment::Ptr> JobFragment::createVect(qproc::ChunkQuerySpec const& chunkQuerySpec,
                                                 JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                                                 JobDbTablesMap::Ptr const& jobDbTablesMap,
                                                 string const& resultTable) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect start");

    vector<Ptr> jFragments;
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a");
    if (chunkQuerySpec.nextFragment.get()) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a1");
        qproc::ChunkQuerySpec const* sPtr = &chunkQuerySpec;
        while (sPtr) {
            LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a1a");
            LOGS(_log, LOG_LVL_TRACE, "nextFragment");
            for (unsigned int t = 0; t < (sPtr->queries).size(); t++) {  // &&& del loop
                LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a1a1");
                LOGS(_log, LOG_LVL_DEBUG, __func__ << " q=" << (sPtr->queries).at(t));
            }
            LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a2");
            for (auto const& sbi : sPtr->subChunkIds) {  // &&& del loop
                LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a2a");
                LOGS(_log, LOG_LVL_DEBUG, __func__ << " sbi=" << sbi);
            }
            // Linked fragments will not have valid subChunkTables vectors,
            // So, we reuse the root fragment's vector.
            LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a3");
            _addFragment(jFragments, resultTable, chunkQuerySpec.subChunkTables, sPtr->subChunkIds,
                         sPtr->queries, jobSubQueryTempMap, jobDbTablesMap);
            sPtr = sPtr->nextFragment.get();
        }
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect a4");
    } else {
        LOGS(_log, LOG_LVL_TRACE, "no nextFragment");
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect b1");
        for (unsigned int t = 0; t < (chunkQuerySpec.queries).size(); t++) {  // &&& del loop
            LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect b1a");
            LOGS(_log, LOG_LVL_TRACE, (chunkQuerySpec.queries).at(t));
        }
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect b2");
        _addFragment(jFragments, resultTable, chunkQuerySpec.subChunkTables, chunkQuerySpec.subChunkIds,
                     chunkQuerySpec.queries, jobSubQueryTempMap, jobDbTablesMap);
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect b3");
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVect end");
    return jFragments;
}

void JobFragment::_addFragment(std::vector<Ptr>& jFragments, std::string const& resultTblName,
                               DbTableSet const& subChunkTables, std::vector<int> const& subchunkIds,
                               std::vector<std::string> const& queries,
                               JobSubQueryTempMap::Ptr const& subQueryTemplates,
                               JobDbTablesMap::Ptr const& dbTablesMap) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment a");
    Ptr jFrag = Ptr(new JobFragment(subQueryTemplates, dbTablesMap, resultTblName));

    // queries: The query string is stored in `_jobSubQueryTempMap` and the list of
    // integer indexes, `_subQueryTempIndexes`, points back to the specific template.
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment b");
    for (auto& qry : queries) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment b1");
        int index = jFrag->_jobSubQueryTempMap->findSubQueryTemp(qry);
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment b2");
        jFrag->_jobSubQueryTempIndexes.push_back(index);
        LOGS(_log, LOG_LVL_INFO, jFrag->cName(__func__) << "&&& added frag=" << qry << " index=" << index);
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment b4");
    }

    // Add the db+table pairs to the subchunks for the fragment.
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment c");
    for (auto& tbl : subChunkTables) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment c1");
        int index = jFrag->_jobDbTablesMap->findDbTable(make_pair(tbl.db, tbl.table));
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment c2");
        jFrag->_jobDbTablesIndexes.push_back(index);
        LOGS(_log, LOG_LVL_INFO,
             jFrag->cName(__func__) << "&&& added dbtbl=" << tbl.db << "." << tbl.table
                                    << " index=" << index);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment d");

    // Add subchunk id numbers
    for (auto& subchunkId : subchunkIds) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment d1");
        jFrag->_subchunkIds.push_back(subchunkId);
        LOGS(_log, LOG_LVL_INFO, jFrag->cName(__func__) << "&&& added subchunkId=" << subchunkId);
    }
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment e");

    jFragments.push_back(move(jFrag));
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::_addFragment end");
}

nlohmann::json JobFragment::serializeJson() const {
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::serializeJson a");

    json jsFragment = {{"resulttblname", _resultTblName},
                       {"subquerytemplate_indexes", _jobSubQueryTempIndexes},
                       {"dbtables_indexes", _jobDbTablesIndexes},
                       {"subchunkids", _subchunkIds}};
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::serializeJson b");

    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& " << jsFragment);

    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::serializeJson end");
    return jsFragment;
}

JobFragment::Vect JobFragment::createVectFromJson(nlohmann::json const& jsFrags,
                                                  JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                                                  JobDbTablesMap::Ptr const& dbTablesMap,
                                                  std::string const& resultTblName) {
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson " << jsFrags);
    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson a");

    JobFragment::Vect jobFragments;

    for (auto const& jsFrag : jsFrags) {
        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson b");
        Ptr jobFrag = Ptr(new JobFragment(jobSubQueryTempMap, dbTablesMap, resultTblName));

        jobFrag->_resultTblName = http::RequestBodyJSON::required<json>(jsFrag, "resulttblname");
        if (jobFrag->_resultTblName != resultTblName) {
            // &&& hoping to remove _resultTblName from JobFragment.
            LOGS(_log, LOG_LVL_ERROR,
                 jobFrag->cName(__func__) + " _resultTblName != resultTblName for " + to_string(jsFrag));
            throw util::Bug(ERR_LOC, jobFrag->cName(__func__) + " _resultTblName != resultTblName for " +
                                             to_string(jsFrag));
        }

        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson c");
        //&&&std::vector<int> _jobSubQueryTempIndexes; ///< &&& doc
        jobFrag->_jobSubQueryTempIndexes = jsFrag["subquerytemplate_indexes"].get<std::vector<int>>();
        for (int j : jobFrag->_jobSubQueryTempIndexes) {
            try {
                string tem = jobSubQueryTempMap->getSubQueryTemp(j);
                LOGS(_log, LOG_LVL_WARN, jobFrag->cName(__func__) << " &&&T j=" << j << " =" << tem);
            } catch (std::out_of_range const& ex) {
                LOGS(_log, LOG_LVL_ERROR,
                     jobFrag->cName(__func__) << " index=" << j << " not found in template map " << jsFrag);
                // rethrow as something callers expect.
                throw std::invalid_argument(jobFrag->cName(__func__) + " template index=" + to_string(j) +
                                            " " + ex.what());
            }
        }

        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson d");
        jobFrag->_jobDbTablesIndexes = jsFrag["dbtables_indexes"].get<std::vector<int>>();
        for (int j : jobFrag->_jobDbTablesIndexes) {
            try {
                auto dbTblPr = dbTablesMap->getDbTable(j);
                LOGS(_log, LOG_LVL_WARN,
                     jobFrag->cName(__func__)
                             << " &&&T j=" << j << " =" << dbTblPr.first << "." << dbTblPr.second);
            } catch (std::out_of_range const& ex) {
                LOGS(_log, LOG_LVL_ERROR,
                     jobFrag->cName(__func__) << " index=" << j << " not found in dbTable map " << jsFrag);
                // rethrow as something callers expect.
                throw std::invalid_argument(jobFrag->cName(__func__) + " dbtable index=" + to_string(j) +
                                            " " + ex.what());
            }
        }

        LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson e");
        jobFrag->_subchunkIds = jsFrag["subchunkids"].get<std::vector<int>>();
        jobFragments.push_back(jobFrag);
    }

    LOGS(_log, LOG_LVL_WARN, "&&& JobFragment::createVectFromJson end");
    return jobFragments;
}

}  // namespace lsst::qserv::protojson
