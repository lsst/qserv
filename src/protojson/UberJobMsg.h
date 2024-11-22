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
#ifndef LSST_QSERV_PROTOJSON_UBERJOBMSG_H
#define LSST_QSERV_PROTOJSON_UBERJOBMSG_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
#include "global/clock_defs.h"
#include "global/DbTable.h"
#include "global/intTypes.h"
#include "protojson/WorkerQueryStatusData.h"

namespace lsst::qserv::qdisp {
class JobQuery;
}

namespace lsst::qserv::qproc {
class ChunkQuerySpec;
}

// This header declarations
namespace lsst::qserv::protojson {

/// This class is used to store query template strings names in a reasonably
/// concise fashion.
/// The same templates recur frequently, so the individual occurrences
/// will be replaced with an integer index and use this class to recover the
/// original template.
class JobSubQueryTempMap {
public:
    using Ptr = std::shared_ptr<JobSubQueryTempMap>;

    std::string cName(const char* fName) const { return std::string("JobSubQueryTempMap::") + fName; }

    JobSubQueryTempMap(JobSubQueryTempMap const&) = delete;

    static Ptr create() { return Ptr(new JobSubQueryTempMap()); }

    /// &&& doc
    static Ptr createFromJson(nlohmann::json const& ujJson);

    /// Find or insert qTemp into the map and return its index.
    int findSubQueryTemp(std::string const& qTemp);

    /// Return the SubQueryTemp string at `index`.
    /// @throws std::out_of_range
    std::string getSubQueryTemp(int index) { return _qTemplateMap.at(index); }

    nlohmann::json serializeJson() const;

private:
    JobSubQueryTempMap() = default;

    std::map<int, std::string> _qTemplateMap;
};

/// This class is used to store db.table names in a reasonably concise fashion.
/// The same db+table name pairs recur frequently, so the individual occurrences
/// will be replaced with an integer index and use this class to recover the
/// complete names.
class JobDbTablesMap {
public:
    using Ptr = std::shared_ptr<JobDbTablesMap>;

    std::string cName(const char* fName) const { return std::string("JobDbTablesMap::") + fName; }

    JobDbTablesMap(JobDbTablesMap const&) = delete;

    static Ptr create() { return Ptr(new JobDbTablesMap()); }

    /// &&& doc
    static Ptr createFromJson(nlohmann::json const& ujJson);

    /// Find or insert the db.table pair into the map and return its index.
    int findDbTable(std::pair<std::string, std::string> const& dbTablePair);

    /// Return the db.table pair at `index`.
    /// @throws std::out_of_range
    std::pair<std::string, std::string> getDbTable(int index) { return _dbTableMap.at(index); }

    /// &&& doc
    void setScanRating(int index, int scanRating, bool lockInMemory);

    /// Return scanRating(int) and lockInMemory(bool) for the dbTable at `index`.
    /// TODO:UJ &&& lockInMemory is expected to go away.
    std::pair<int, bool> getScanRating(int index) { return _scanRatingMap[index]; }

    nlohmann::json serializeJson() const;

private:
    JobDbTablesMap() = default;

    /// Map of db name and table name pairs: db first, table second.
    /// The order in the map is arbitrary, but must be consistent
    /// so that lookups using the int index always return the same pair.
    std::map<int, std::pair<std::string, std::string>> _dbTableMap;

    /// Key is dbTable index, val is scanRating(int) lockInMemory(bool)
    std::map<int, std::pair<int, bool>> _scanRatingMap;
};

/// This class stores the contents of a query fragment, which will be reconstructed
/// and run on a worker to help answer a user query.
class JobFragment {
public:
    using Ptr = std::shared_ptr<JobFragment>;
    using Vect = std::vector<Ptr>;

    std::string cName(const char* fName) const { return std::string("JobFragment::") + fName; }

    JobFragment() = delete;
    JobFragment(JobFragment const&) = delete;

    static Vect createVect(qproc::ChunkQuerySpec const& chunkQuerySpec,
                           JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                           JobDbTablesMap::Ptr const& dbTablesMap, std::string const& resultTblName);

    /// &&& doc
    static Vect createVectFromJson(nlohmann::json const& ujJson,
                                   JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                                   JobDbTablesMap::Ptr const& dbTablesMap, std::string const& resultTblName);

    /// Return a json version of the contents of this class.
    nlohmann::json serializeJson() const;

private:
    JobFragment(JobSubQueryTempMap::Ptr const& subQueryTemplates, JobDbTablesMap::Ptr const& dbTablesMap,
                std::string const& resultTblName);

    /// &&& doc
    static void _addFragment(std::vector<Ptr>& jFragments, std::string const& resultTblName,
                             DbTableSet const& subChunkTables, std::vector<int> const& subchunkIds,
                             std::vector<std::string> const& queries,
                             JobSubQueryTempMap::Ptr const& subQueryTemplates,
                             JobDbTablesMap::Ptr const& dbTablesMap);

    JobSubQueryTempMap::Ptr _jobSubQueryTempMap;  ///< &&& doc
    std::vector<int> _jobSubQueryTempIndexes;     ///< &&& doc

    JobDbTablesMap::Ptr _jobDbTablesMap;   ///< &&& doc
    std::vector<int> _jobDbTablesIndexes;  ///< &&& doc

    std::vector<int> _subchunkIds;  ///< &&& doc

    std::string _resultTblName;  ///< &&& doc &&& probably not needed here. Replace with
                                 ///< JobMsg::_chunkResultName field.
};

/// This class is used to store the information for a single Job (the queries and metadata
/// required to collect rows from a single chunk) in a reasonable manner.
class JobMsg {
public:
    using Ptr = std::shared_ptr<JobMsg>;
    using Vect = std::vector<Ptr>;
    std::string cName(const char* fnc) const { return std::string("JobMsg::") + fnc; }

    JobMsg() = delete;
    JobMsg(JobMsg const&) = delete;
    JobMsg& operator=(JobMsg const&) = delete;

    static Ptr create(std::shared_ptr<qdisp::JobQuery> const& jobs,
                      JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
                      JobDbTablesMap::Ptr const& jobDbTablesMap);

    /// &&& doc
    static Ptr createFromJson(nlohmann::json const& ujJson, JobSubQueryTempMap::Ptr const& subQueryTemplates,
                              JobDbTablesMap::Ptr const& dbTablesMap);

    /// Return a json version of the contents of this class.
    nlohmann::json serializeJson() const;

private:
    JobMsg(std::shared_ptr<qdisp::JobQuery> const& jobPtr, JobSubQueryTempMap::Ptr const& jobSubQueryTempMap,
           JobDbTablesMap::Ptr const& jobDbTablesMap);

    JobMsg(JobSubQueryTempMap::Ptr const& jobSubQueryTempMap, JobDbTablesMap::Ptr const& jobDbTablesMap,
           JobId jobId, int attemptCount, std::string const& chunkQuerySpecDb, int scanRating,
           bool scanInteractive, int chunkId, std::string const& chunkResultName);

    JobId _jobId;
    int _attemptCount;
    std::string _chunkQuerySpecDb;
    int _scanRating;
    bool _scanInteractive;
    int _chunkId;
    std::string _chunkResultName;
    JobFragment::Vect _jobFragments;

    JobSubQueryTempMap::Ptr _jobSubQueryTempMap;  ///< Map of all query templates related to this UberJob.
    JobDbTablesMap::Ptr _jobDbTablesMap;          ///< Map of all db.tables related to this UberJob.

    std::vector<int> _chunkScanTableIndexes;  ///< list of indexes into _jobDbTablesMap.
};

/// This class stores an UberJob, a collection of Jobs meant for a
/// specific worker, so it can be converted to and from a json format
/// and sent to a worker.
/// There are several fields which are the same for each job, so these
/// values are stored in maps and the individual Jobs and Fragments
/// use integer indexes to reduce the size of the final message.
class UberJobMsg : public std::enable_shared_from_this<UberJobMsg> {
public:
    using Ptr = std::shared_ptr<UberJobMsg>;
    std::string cName(const char* fnc) const { return std::string("UberJobMsg::") + fnc; }

    UberJobMsg() = delete;
    UberJobMsg(UberJobMsg const&) = delete;
    UberJobMsg& operator=(UberJobMsg const&) = delete;

    static Ptr create(unsigned int metaVersion, std::string const& replicationInstanceId,
                      std::string const& replicationAuthKey, CzarContactInfo::Ptr const& czInfo,
                      WorkerContactInfo::Ptr const& wInfo, QueryId qId, UberJobId ujId, int rowLimit,
                      int maxTableSizeMB, std::vector<std::shared_ptr<qdisp::JobQuery>> const& jobs) {
        return Ptr(new UberJobMsg(metaVersion, replicationInstanceId, replicationAuthKey, czInfo, wInfo->wId,
                                  qId, ujId, rowLimit, maxTableSizeMB, jobs));
    }

    static Ptr createFromJson(nlohmann::json const& ujJson);

    /// Return a json version of the contents of this class.
    nlohmann::json serializeJson() const;

    std::string dump() const;

private:
    UberJobMsg(unsigned int metaVersion, std::string const& replicationInstanceId,
               std::string const& replicationAuthKey,
               //&&&CzarContactInfo::Ptr const& czInfo, WorkerContactInfo::Ptr const& wInfo,
               CzarContactInfo::Ptr const& czInfo, std::string const& workerId, QueryId qId, UberJobId ujId,
               int rowLimit, int maxTableSizeMB, std::vector<std::shared_ptr<qdisp::JobQuery>> const& jobs);

    unsigned int _metaVersion;  // "version", http::MetaModule::version
    // czar
    std::string _replicationInstanceId;  // "instance_id", czarConfig->replicationInstanceId()
    std::string _replicationAuthKey;     //"auth_key", czarConfig->replicationAuthKey()
    //&&& auto [ciwId, ciwHost, ciwManagment, ciwPort] = _wContactInfo->getAll(); (string, string, string,
    //int)
    CzarContactInfo::Ptr _czInfo;
    std::string _workerId;  // "worker", ciwId
    //&&&WorkerContactInfo::Ptr _wInfo; // &&& probably not needed
    // &&& {"czarinfo",
    //&&&std::string _czarName; // "name", czarConfig->name()
    //&&&qmeta::czarId _czarId; // "id", czarConfig->id()
    //&&&uint16_t _czarManagementPort; // "management-port", czarConfig->replicationHttpPort()
    //&&&std::string _czarManagementHostName; // "management-host-name", util::get_current_host_fqdn()
    // &&& }
    // &&&{"uberjob",
    QueryId _qId;     // "queryid", _queryId
    UberJobId _ujId;  // "uberjobid", _uberJobId
    //&&& CzarIdType _czarId; // "czarid", _czarId
    int _rowLimit;        // "rowlimit", _rowLimit
    int _maxTableSizeMB;  // &&& Need to add initialization.

    std::vector<std::shared_ptr<qdisp::JobQuery>> _jobs;  // &&& needs to be replaced with jobData
    // &&& };

    /// Map of all query templates related to this UberJob.
    JobSubQueryTempMap::Ptr _jobSubQueryTempMap{JobSubQueryTempMap::create()};

    /// Map of all db.tables related to this UberJob.
    JobDbTablesMap::Ptr _jobDbTablesMap{JobDbTablesMap::create()};

    /// List of all job data in this UberJob. "jobs", json::array()
    JobMsg::Vect _jobMsgVect;
};

}  // namespace lsst::qserv::protojson

#endif  // LSST_QSERV_PROTOJSON_UBERJOBMSG_H
