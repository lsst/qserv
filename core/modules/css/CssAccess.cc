/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "css/CssAccess.h"

// System headers
#include <algorithm>
#include <fstream>
#include <map>
#include <sstream>

// Third-party headers
#include "boost/lexical_cast.hpp"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/constants.h"
#include "css/CssConfig.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "css/KvInterface.h"
#include "css/KvInterfaceImplMem.h"
#include "css/KvInterfaceImplMySql.h"
#include "mysql/MySqlConfig.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.css.CssAccess");

// Name of sub-key used for packed data
std::string const _packedKeyName(".packed.json");

}

namespace lsst {
namespace qserv {
namespace css {

std::shared_ptr<CssAccess>
CssAccess::createFromStream(std::istream& stream,
                            std::string const& emptyChunkPath,
                            bool readOnly) {
    LOGS(_log, LOG_LVL_DEBUG, "Create CSS instance with memory store from data in stream");
    return std::shared_ptr<CssAccess>(new CssAccess(std::make_shared<KvInterfaceImplMem>(stream, readOnly),
                                                    std::make_shared<EmptyChunks>(emptyChunkPath)));
}

// Create CssAccess instance from existing key-value data.
std::shared_ptr<CssAccess>
CssAccess::createFromData(std::string const& data,
                          std::string const& emptyChunkPath,
                          bool readOnly) {
    LOGS(_log, LOG_LVL_DEBUG, "Create CSS instance with memory store from data in string");
    std::istringstream str(data);
    return std::shared_ptr<CssAccess>(new CssAccess(std::make_shared<KvInterfaceImplMem>(str, readOnly),
                                                    std::make_shared<EmptyChunks>(emptyChunkPath)));
}

// Create CssAccess instance from configuration dictionary.
std::shared_ptr<CssAccess>
CssAccess::createFromConfig(std::map<std::string, std::string> const& config,
                            std::string const& emptyChunkPath,
                            bool readOnly) {
    css::CssConfig cssConfig(config);
    LOGS(_log, LOG_LVL_DEBUG, "Create CSS instance from config map");
    if (cssConfig.getTechnology() == "mem") {
        // optional data or file keys
        std::string iterData = cssConfig.getData();
        std::string iterFile = cssConfig.getFile();
        if (not cssConfig.getData().empty()) {
            // data is in a string
            return createFromData(cssConfig.getData(), emptyChunkPath, readOnly);
        } else if (not cssConfig.getFile().empty()) {
            // read data from file
            std::ifstream f(cssConfig.getFile());
            if (f.fail()) {
                LOGS(_log, LOG_LVL_DEBUG, "failed to open data file " << cssConfig.getFile());
                throw ConfigError("failed to open data file " + cssConfig.getFile());
            }
            LOGS(_log, LOG_LVL_DEBUG,
                 "Create CSS instance with memory store from data file " << cssConfig.getFile());
            auto kvi = std::make_shared<KvInterfaceImplMem>(f, readOnly);
            return std::shared_ptr<CssAccess>(new CssAccess(kvi,
                                                            std::make_shared<EmptyChunks>(emptyChunkPath)));
        } else {
            // no initial data
            LOGS(_log, LOG_LVL_DEBUG, "Create CSS instance with empty memory store");
            auto kvi = std::make_shared<KvInterfaceImplMem>(readOnly);
            return std::shared_ptr<CssAccess>(new CssAccess(kvi,
                                                            std::make_shared<EmptyChunks>(emptyChunkPath)));
        }
    } else if (cssConfig.getTechnology() == "mysql") {
        LOGS(_log, LOG_LVL_DEBUG, "Create CSS instance with mysql store");
        auto kvi = std::make_shared<KvInterfaceImplMySql>(cssConfig.getMySqlConfig(), readOnly);
        return std::shared_ptr<CssAccess>(new CssAccess(kvi, std::make_shared<EmptyChunks>(emptyChunkPath)));
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Unexpected value of \"technology\" key: " << cssConfig.getTechnology());
        throw ConfigError("Unexpected value of \"technology\" key: " + cssConfig.getTechnology());
    }
}

// Construct from KvInterface instance and empty chunk list instance
CssAccess::CssAccess(std::shared_ptr<KvInterface> const& kvInterface,
                     std::shared_ptr<EmptyChunks> const& emptyChunks,
                     std::string const& prefix)
    : _kvI(kvInterface), _emptyChunks(emptyChunks),
      _prefix(prefix), _versionOk(false) {

    // Check CSS version defined in KV, or create key with version
    _checkVersion(false);
    if (not _versionOk) {
        // means key is not there, try to create it
        _kvI->create(VERSION_KEY, VERSION_STR);
        _versionOk = true;
    }
}

int
CssAccess::cssVersion() {
    return VERSION;
}

void
CssAccess::_checkVersion(bool mustExist) const {
    if (_versionOk) return;
    auto version = _kvI->get(VERSION_KEY, "");
    if (not version.empty()) {
        if (version != VERSION_STR) {
            LOGS(_log, LOG_LVL_DEBUG, "version mismatch, expected: "
                 << VERSION_STR << ", found: " << version);
            throw VersionMismatchError(VERSION_STR, version);
        } else {
            _versionOk = true;
        }
    } else if (mustExist) {
        throw VersionMissingError(VERSION_KEY);
    }
}

std::vector<std::string>
CssAccess::getDbNames() const {
    _checkVersion();

    std::string p = _prefix + "/DBS";
    auto names = _kvI->getChildren(p);

    // databases cannot be packed, but just in case remove packed key if any
    auto it = std::remove(names.begin(), names.end(), ::_packedKeyName);
    names.erase(it, names.end());

    return names;
}

std::map<std::string, std::string>
CssAccess::getDbStatus() const {
    _checkVersion();

    std::string p = _prefix + "/DBS";
    auto kvs = _kvI->getChildrenValues(p);

    // databases cannot be packed, but just in case remove packed key if any
    kvs.erase(::_packedKeyName);

    return kvs;
}

void
CssAccess::setDbStatus(std::string const& dbName, std::string const& status) {
    LOGS(_log, LOG_LVL_DEBUG, "setDbStatus(" << dbName << ", " << status << ")");
    _checkVersion();

    _assertDbExists(dbName);
    std::string const dbKey = _prefix + "/DBS/" + dbName;
    _kvI->set(dbKey, status);
}

bool
CssAccess::containsDb(std::string const& dbName) const {
    _checkVersion();
    if (dbName.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Empty database name passed.");
        return false;
    }
    std::string p = _prefix + "/DBS/" + dbName;
    bool ret = _kvI->exists(p);
    LOGS(_log, LOG_LVL_DEBUG, "containsDb(" << dbName << "): " << ret);
    return ret;
}

StripingParams
CssAccess::getDbStriping(std::string const& dbName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getDbStriping(" << dbName << ")");
    _checkVersion();

    StripingParams striping;
    auto dbMap = _getSubkeys(_prefix + "/DBS/" + dbName, {"partitioningId"});
    auto const& partId = dbMap["partitioningId"];
    if (partId.empty()) {
        // if database is not defined throw an exception, otherwise return default values
        _assertDbExists(dbName);
        return striping;
    }

    // get all keys
    std::string pKey = _prefix + "/PARTITIONING/_" + partId;
    std::vector<std::string> subKeys{"nStripes", "nSubStripes", "overlap"};
    auto const keyMap = _getSubkeys(pKey, subKeys);

    // fill the structure
    try {
        striping.partitioningId = std::stoi(partId);
        auto iter = keyMap.find("nStripes");
        if (iter != keyMap.end()) {
            striping.stripes = std::stoi(iter->second);
        }
        iter = keyMap.find("nSubStripes");
        if (iter != keyMap.end()) {
            striping.subStripes = std::stoi(iter->second);
        }
        iter = keyMap.find("overlap");
        if (iter != keyMap.end()) {
            striping.overlap = std::stod(iter->second);
        }
    } catch (std::exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "one of the keys is not numeric: " << util::printable(keyMap));
        throw KeyValueError(pKey, "one of the keys is not numeric: " + std::string(exc.what()));
    }

    return striping;
}

void
CssAccess::createDb(std::string const& dbName,
                    StripingParams const& striping,
                    std::string const& storageClass,
                    std::string const& releaseStatus) {
    LOGS(_log, LOG_LVL_DEBUG, "createDb(" << dbName << ")");
    _checkVersion(false);

    std::string partId;
    if (striping.stripes > 0) {
        // define partitioning structure

        // create unique key for it and get its id
        std::string const pfxKey = _prefix + "/PARTITIONING/_";
        std::string const partKey = _kvI->create(pfxKey, "", true);
        partId = partKey.substr(pfxKey.size());

        // store striping structure
        // TODO: add uuid if we want to use it
        std::map<std::string, std::string> stripMap{
            std::make_pair("nStripes", std::to_string(striping.stripes)),
            std::make_pair("nSubStripes", std::to_string(striping.subStripes)),
            std::make_pair("overlap", std::to_string(striping.overlap))};
        _storePacked(partKey, stripMap);
    }

    // TODO: add uuid if we want to use it
    std::map<std::string, std::string> dbMap{
        std::make_pair("releaseStatus", releaseStatus),
        std::make_pair("storageClass", storageClass)};
    if (not partId.empty()) {
        dbMap.insert(std::make_pair("partitioningId", partId));
    }

    std::string const dbKey = _prefix + "/DBS/" + dbName;
    _storePacked(dbKey, dbMap);
    _kvI->set(dbKey, KEY_STATUS_READY);
}

void
CssAccess::createDbLike(std::string const& dbName,
                        std::string const& templateDbName) {
    LOGS(_log, LOG_LVL_DEBUG, "createDbLike(" << dbName << ")");
    _checkVersion();

    std::vector<std::string> subKeys{"partitioningId", "releaseStatus", "storageClass"};
    auto dbMap = _getSubkeys(_prefix + "/DBS/" + templateDbName, subKeys);
    if (dbMap.empty()) {
        // nothing is found, check whether db exists
        _assertDbExists(templateDbName);
    }

    // make new database with the copy of all parameters
    std::string const dbKey = _prefix + "/DBS/" + dbName;
    _storePacked(dbKey, dbMap);
    _kvI->set(dbKey, KEY_STATUS_READY);
}

void
CssAccess::dropDb(std::string const& dbName) {
    LOGS(_log, LOG_LVL_DEBUG, "dropDb(" << dbName << ")");
    _checkVersion();

    std::string key = _prefix + "/DBS/" + dbName;

    // key is supposed to exist
    try {
        LOGS(_log, LOG_LVL_DEBUG, "dropDb: try to delete key: " << key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "dropDb: key is not found: " << key);
        throw NoSuchDb(dbName);
    }
}

std::vector<std::string>
CssAccess::getTableNames(std::string const& dbName, bool readyOnly) const {
    LOGS(_log, LOG_LVL_DEBUG, "getTableNames(" << dbName << ")");
    _checkVersion();

    std::string key = _prefix + "/DBS/" + dbName + "/TABLES";
    std::vector<std::string> names;
    try {
        names = _kvI->getChildren(key);
    } catch (NoSuchKey const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "getTableNames: key is not found: " << key);
        _assertDbExists(dbName);
    }

    // tables cannot be packed, but just in case remove packed key if any
    auto it = std::remove(names.begin(), names.end(), ::_packedKeyName);
    names.erase(it, names.end());

    if (readyOnly and not names.empty()) {
        // filter out names with status other than READY
        auto const tableStatuses = _getSubkeys(key, names);
        names.clear();
        for (auto& kv: tableStatuses) {
            if (kv.second == "READY") {
                names.push_back(kv.first);
            }
        }
    }
    return names;
}

std::map<std::string, std::string>
CssAccess::getTableStatus(std::string const& dbName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getTableStatus(" << dbName << ")");
    _checkVersion();

    std::string key = _prefix + "/DBS/" + dbName + "/TABLES";
    std::map<std::string, std::string> kvs;
    try {
        kvs = _kvI->getChildrenValues(key);
    } catch (NoSuchKey const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "getTableNames: key is not found: " << key);
        _assertDbExists(dbName);
    }

    // tables cannot be packed, but just in case remove packed key if any
    kvs.erase(::_packedKeyName);

    return kvs;

}

void
CssAccess::setTableStatus(std::string const& dbName,
                          std::string const& tableName,
                          std::string const& status) {
    LOGS(_log, LOG_LVL_DEBUG, "setTableStatus(" << dbName << ", "
         << tableName << ", " << status << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
    _kvI->set(tableKey, status);
}

bool
CssAccess::containsTable(std::string const& dbName, std::string const& tableName, bool readyOnly) const {
    LOGS(_log, LOG_LVL_DEBUG, "containsTable(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const key = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    // If key is not there pretend that its value is not "READY"
    std::string const val = _kvI->get(key, "DOES_NOT_EXIST");
    if (val == "DOES_NOT_EXIST") {
        // table key is not there at all, throw if database name is not good
        _assertDbExists(dbName);
        LOGS(_log, LOG_LVL_DEBUG, "containsTable: key not found: " << key);
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, "containsTable: key value: " << val);
    // if key value is not "READY" it likely means table is in the process
    // of being deleted, which is the same as if it does not exist
    if (readyOnly) return val == "READY";
    return true;
}

std::string
CssAccess::getTableSchema(std::string const& dbName, std::string const& tableName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getTableSchema(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    auto kvMap = _getSubkeys(tableKey, {"schema"});
    auto const schema = kvMap["schema"];
    if (schema.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
    }
    return schema;
}

MatchTableParams
CssAccess::getMatchTableParams(std::string const& dbName,
                               std::string const& tableName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getMatchTableParams(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    MatchTableParams params;

    std::vector<std::string> subKeys{"match/dirTable1", "match/dirColName1", "match/dirTable2",
            "match/dirColName2", "match/flagColName"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    _fillMatchTableParams(paramMap, params);
    return params;
}

PartTableParams
CssAccess::getPartTableParams(std::string const& dbName,
                              std::string const& tableName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getPartTableParams(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    PartTableParams params;

    std::vector<std::string> subKeys{"partitioning", "partitioning/subChunks",
        "partitioning/dirDb", "partitioning/dirTable", "partitioning/dirColName", "partitioning/latColName",
        "partitioning/lonColName", "partitioning/overlap", "partitioning/secIndexColName"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    _fillPartTableParams(paramMap, params, tableKey);
    return params;
}

ScanTableParams
CssAccess::getScanTableParams(std::string const& dbName,
                              std::string const& tableName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getScanTableParams(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    ScanTableParams params;

    std::vector<std::string> subKeys{"sharedScan/lockInMem", "sharedScan/scanRating"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    _fillScanTableParams(paramMap, params, tableKey);
    return params;
}

TableParams
CssAccess::getTableParams(std::string const& dbName, std::string const& tableName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getTableParams(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    TableParams params;

    std::vector<std::string> subKeys{"partitioning/subChunks", "partitioning/dirDb",
        "partitioning/dirTable", "partitioning/dirColName", "partitioning/latColName",
        "partitioning/lonColName", "partitioning/overlap", "partitioning/secIndexColName",
        "sharedScann/lockInMem", "sharedScan/scanRating",
        "match/dirTable1", "match/dirColName1", "match/dirTable2", "match/dirColName2",
        "match/flagColName", "partitioning"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    // fill the structure
    _fillMatchTableParams(paramMap, params.match);
    _fillPartTableParams(paramMap, params.partitioning, tableKey);
    _fillScanTableParams(paramMap, params.sharedScan, tableKey);

    return params;
}

void
CssAccess::createTable(std::string const& dbName,
                       std::string const& tableName,
                       std::string const& schema,
                       PartTableParams const& partParams,
                       ScanTableParams const& scanParams) {
    LOGS(_log, LOG_LVL_DEBUG, "createTable(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    try {
        _kvI->create(tableKey, KEY_STATUS_IGNORE);
    } catch (KeyExistsError const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "createTable: key already exists: " << tableKey);
        throw TableExists(dbName, tableName);
    }

    // add schema
    _kvI->create(tableKey + "/schema", schema);

    // save partitioning info
    if (partParams.isPartitioned()) {
        std::map<std::string, std::string> partMap{
            std::make_pair("dirDb", partParams.dirDb),
            std::make_pair("dirTable", partParams.dirTable),
            std::make_pair("dirColName", partParams.dirColName),
            std::make_pair("latColName", partParams.latColName),
            std::make_pair("lonColName", partParams.lonColName),
            std::make_pair("subChunks", std::to_string(int(partParams.subChunks))),
        };
        // only store overlap if non-zero
        if (partParams.overlap != 0.0) {
            partMap.insert(std::make_pair("overlap", std::to_string(partParams.overlap)));
        }
        _storePacked(tableKey + "/partitioning", partMap);

        // save shared scan info. Only store values different from default
        if (scanParams.lockInMem or scanParams.scanRating != 0) {
            std::map<std::string, std::string> scanMap;
            if (scanParams.lockInMem) {
                scanMap.insert(std::make_pair("lockInMem", "1"));
            };
            if (scanParams.scanRating != 0) {
                scanMap.insert(std::make_pair("scanRating",
                                              std::to_string(scanParams.scanRating)));
            }
            _storePacked(tableKey + "/sharedScan", scanMap);
        }
    }

    // done
    _kvI->set(tableKey, KEY_STATUS_READY);
}

void
CssAccess::createMatchTable(std::string const& dbName,
                            std::string const& tableName,
                            std::string const& schema,
                            MatchTableParams const& matchParams) {
    LOGS(_log, LOG_LVL_DEBUG, "createMatchTable(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    try {
        _kvI->create(tableKey, KEY_STATUS_IGNORE);
    } catch (KeyExistsError const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "createMatchTable: key already exists: " << tableKey);
        throw TableExists(dbName, tableName);
    }

    // add schema
    _kvI->create(tableKey + "/schema", schema);

    // save partitioning info
    if (matchParams.isMatchTable()) {
        // It looks like older code checks "match" key value
        _kvI->create(tableKey + "/match", "1");
        std::map<std::string, std::string> partMap{
            std::make_pair("dirTable1", matchParams.dirTable1),
            std::make_pair("dirColName1", matchParams.dirColName1),
            std::make_pair("dirTable2", matchParams.dirTable2),
            std::make_pair("dirColName2", matchParams.dirColName2),
            std::make_pair("flagColName", matchParams.flagColName),
        };
        _storePacked(tableKey + "/match", partMap);
        // match table is always partitioned and needs corresponding key
        _kvI->create(tableKey + "/partitioning", "");
    }

    // done, can mark table as ready
    _kvI->set(tableKey, KEY_STATUS_READY);
}

void
CssAccess::dropTable(std::string const& dbName, std::string const& tableName) {
    LOGS(_log, LOG_LVL_DEBUG, "dropTable(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const key = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    // key is supposed to exist
    try {
        LOGS(_log, LOG_LVL_DEBUG, "dropTable: try to delete key: " << key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "dropTable: key is not found: " << key);
        throw NoSuchTable(dbName, tableName);
    }
}

std::vector<std::string>
CssAccess::getNodeNames() const {
    std::string const key = _prefix + "/NODES";
    auto nodes = _kvI->getChildren(key);
    _checkVersion();

    // /NODES cannot have packed keys, but just in case remove packed key if any
    auto it = std::remove(nodes.begin(), nodes.end(), ::_packedKeyName);
    nodes.erase(it, nodes.end());

    return nodes;
}

NodeParams
CssAccess::getNodeParams(std::string const& nodeName) const {
    LOGS(_log, LOG_LVL_DEBUG, "getNodeParams(" << nodeName << ")");
    _checkVersion();

    std::string const key = _prefix + "/NODES";

    NodeParams params;

    std::vector<std::string> subKeys{nodeName, nodeName + "/type", nodeName + "/host", nodeName + "/port"};
    auto paramMap = _getSubkeys(key, subKeys);
    if (paramMap.empty()) {
        // check node key
        if (not _kvI->exists(key + "/" + nodeName)) throw NoSuchNode(nodeName);
        return params;
    }

    // fill the structure
    params.state = paramMap[nodeName];
    params.type = paramMap[nodeName + "/type"];
    params.host = paramMap[nodeName + "/host"];
    try {
        auto iter = paramMap.find(nodeName + "/port");
        if (iter != paramMap.end()) {
            params.port = std::stoi(iter->second);
        }
    } catch (std::exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "one of the sub-keys is not numeric: "
             << util::printable(paramMap));
        throw KeyValueError(key + "/" + nodeName,
                            "one of the sub-keys is not numeric: " + std::string(exc.what()));
    }

    return params;
}

std::map<std::string, NodeParams>
CssAccess::getAllNodeParams() const {
    LOGS(_log, LOG_LVL_DEBUG, "getAllParams()");
    _checkVersion();

    std::string const key = _prefix + "/NODES";

    // we do not really care much about consistency here and
    // are prepared to deal with nodes disappearing.
    std::map<std::string, NodeParams> result;
    for (auto& node: getNodeNames()) {
        try {
            result.insert(std::make_pair(node, getNodeParams(node)));
        } catch (NoSuchNode const& exc) {
            LOGS(_log, LOG_LVL_DEBUG, "node disappeared");
        }
    }

    return result;
}

void
CssAccess::addNode(std::string const& nodeName, NodeParams const& nodeParams) {
    LOGS(_log, LOG_LVL_DEBUG, "addNode(" << nodeName << ")");
    _checkVersion(false);

    std::string const key = _prefix + "/NODES/" + nodeName;

    try {
        _kvI->create(key, "CREATING");
    } catch (KeyExistsError const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "addNode: key already exists: " << key);
        throw NodeExists(nodeName);
    }

    std::map<std::string, std::string> parMap{
        std::make_pair("type", nodeParams.type),
        std::make_pair("host", nodeParams.host),
        std::make_pair("port", std::to_string(nodeParams.port)),
    };
    _storePacked(key, parMap);

    // done
    _kvI->set(key, nodeParams.state);
}

void CssAccess::setNodeState(std::string const& nodeName, std::string const& newState) {
    LOGS(_log, LOG_LVL_DEBUG, "setNodeState(" << nodeName << ", " << newState << ")");
    _checkVersion();

    std::string const key = _prefix + "/NODES/" + nodeName;

    if (not _kvI->exists(key)) {
        LOGS(_log, LOG_LVL_DEBUG, "setNodeState: key does not exist: " << key);
        throw NoSuchNode(nodeName);
    }

    _kvI->set(key, newState);
}

void
CssAccess::deleteNode(std::string const& nodeName) {
    LOGS(_log, LOG_LVL_DEBUG, "deleteNode(" << nodeName << ")");
    _checkVersion();

    // check if the node is used by any chunk
    for (auto& dbName: getDbNames()) {
        for (auto& tblName: getTableNames(dbName, false)) {
            for(auto& chunkPair: getChunks(dbName, tblName)) {
                for (auto& node: chunkPair.second) {
                    if (node == nodeName) {
                        throw NodeInUse(nodeName);
                    }
                }
            }
        }
    }

    std::string const key = _prefix + "/NODES/" + nodeName;

    // key is supposed to exist
    try {
        LOGS(_log, LOG_LVL_DEBUG, "deleteNode: try to delete key: " << key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGS(_log, LOG_LVL_DEBUG, "deleteNode: key is not found: " << key);
        throw NoSuchNode(nodeName);
    }
}

void
CssAccess::addChunk(std::string const& dbName,
                    std::string const& tableName,
                    int chunk,
                    std::vector<std::string> const& nodeNames) {
    LOGS(_log, LOG_LVL_DEBUG, "addChunk(" << dbName << ", "
         << tableName << ", " << chunk << ")");
    _checkVersion();

    std::string const key = _prefix + (boost::format("/DBS/%s/TABLES/%s/CHUNKS/%s/REPLICAS") %
            dbName % tableName % chunk).str();

    for (auto& node: nodeNames) {
        auto path = _kvI->create(key + "/", "", true);
        LOGS(_log, LOG_LVL_DEBUG, "addChunk: New chunk replica key: " << path);
        std::map<std::string, std::string> chunkMap{std::make_pair("nodeName", node)};
        _storePacked(path, chunkMap);
    }
}

std::map<int, std::vector<std::string>>
CssAccess::getChunks(std::string const& dbName, std::string const& tableName) {
    LOGS(_log, LOG_LVL_DEBUG, "getChunks(" << dbName << ", " << tableName << ")");
    _checkVersion();

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    std::string const chunksKey = tableKey + "/CHUNKS";

    std::map<int, std::vector<std::string>> result;

    std::vector<std::string> chunks;
    try {
        chunks = _kvI->getChildren(chunksKey);
    } catch (NoSuchKey const& exc) {
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        LOGS(_log, LOG_LVL_DEBUG, "getChunks: No CHUNKS sub-key for: " << tableKey);
        return result;
    }

    for (auto& chunk: chunks) {
        int chunkId;
        try {
            chunkId = std::stoi(chunk);
        } catch (std::exception const& exc) {
            LOGS(_log, LOG_LVL_DEBUG, "getChunks: non-numeric chunk key: " << chunk);
            continue;
        }

        std::string const replicasKey = chunksKey + "/" + chunk + "/REPLICAS";
        std::vector<std::string> replicas;
        try {
            replicas = _kvI->getChildren(replicasKey);

            // replicas cannot be packed, but just in case remove packed key if any
            auto it = std::remove(replicas.begin(), replicas.end(), ::_packedKeyName);
            replicas.erase(it, replicas.end());
        } catch (std::exception const& exc) {
            LOGS(_log, LOG_LVL_DEBUG, "getChunks: replica key is missing: " << replicasKey);
            continue;
        }

        auto& nodes = result[chunkId];
        for (auto& replica: replicas) {
            auto nodeMap = _getSubkeys(replicasKey + "/" + replica, {"nodeName"});
            auto iter = nodeMap.find("nodeName");
            if (iter != nodeMap.end()) {
                nodes.push_back(iter->second);
            }
        }
    }

    return result;
}

void
CssAccess::_assertDbExists(std::string const& dbName) const {
    if (!containsDb(dbName)) {
        LOGS(_log, LOG_LVL_DEBUG, "Db '" << dbName << "' not found.");
        throw NoSuchDb(dbName);
    }
}

std::map<std::string, std::string>
CssAccess::_getSubkeys(std::string const& key, std::vector<std::string> const& subKeys) const {
    LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys(" << key << ", " << util::printable(subKeys) << ")");

    std::set<std::string> parentKeys;

    // construct full set of keys to look at, this includes a packed key
    // plus all explicit key names
    std::vector<std::string> allKeys;
    for (auto& subKey: subKeys) {

        // find actual parent of the key (everything before last slash)
        std::string::size_type p = subKey.rfind('/');
        std::string parentKey = key;
        if (p != std::string::npos) {
            parentKey += "/";
            parentKey += subKey.substr(0, p);
        }

        if (parentKeys.count(parentKey) == 0) {
            parentKeys.insert(parentKey);
            allKeys.push_back(parentKey + "/" + ::_packedKeyName);
        }

        allKeys.push_back(key + "/" + subKey);
    }
    LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys: parent keys: " << util::printable(parentKeys));
    LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys: looking for keys: " << util::printable(allKeys));

    // get everything in one call from KV store, this is
    // supposed to be consistent set of values
    auto keyMap = _kvI->getMany(allKeys);
    LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys: kvI returned: " << util::printable(keyMap));

    // unpack packed guys, and add unpacked keys to a key map, this does not overwrite
    // existing keys (meaning that regular key overrides same packed key)
    for (auto& parentKey: parentKeys) {
        std::string const packedKey = parentKey + "/" + ::_packedKeyName;
        auto iter = keyMap.find(packedKey);
        if (iter != keyMap.end()) {
            auto const packedMap = _unpackJson(iter->first, iter->second);
            LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys: packed keys: "
                 << packedKey << " -> " << util::printable(packedMap));
            for (auto& packed: packedMap) {
                keyMap.insert(std::make_pair(parentKey + "/" + packed.first, packed.second));
            }
        }
    }

    // copy the keys that we care about
    std::map<std::string, std::string> result;
    for (auto& subKey: subKeys) {
        std::string const fullKey = key + "/" + subKey;
        auto iter = keyMap.find(fullKey);
        if (iter != keyMap.end()) {
            result.insert(std::make_pair(subKey, iter->second));
        }
    }

    LOGS(_log, LOG_LVL_DEBUG, "_getSubkeys: result: " << util::printable(result));
    return result;
}

std::map<std::string, std::string>
CssAccess::_unpackJson(std::string const& key, std::string const& data) {
    namespace ptree = boost::property_tree;

    std::istringstream input(data);
    ptree::ptree pt;
    if (not data.empty()) {
        try {
            ptree::read_json(input, pt);
        } catch (ptree::ptree_error const& exc) {
            LOGS(_log, LOG_LVL_ERROR, "unpackJson error: " << exc.what() << " data=\"" << data << "\"");
            throw lsst::qserv::css::KeyValueError(key, "json unpacking failed: " + std::string(exc.what()));
        }
    }

    // convert to map (only top-level, not children)
    std::map<std::string, std::string> result;
    for (auto& pair: pt) {
        // only take keys that do not have children, this is a bit complicated
        // with ptree parser, basically {"c": {}} or {"c": []} will result in
        // key "c" having both empty data and empty child list which makes it
        // indistinguishable from {"c": ""}
        if (pair.second.empty()) {
            result.insert(std::make_pair(pair.first, pair.second.data()));
        }
    }
    return result;
}

void
CssAccess::_storePacked(std::string const& key, std::map<std::string, std::string> const& data) {
    namespace ptree = boost::property_tree;

    // make json string out of data
    ptree::ptree pt;
    for (auto& kv: data) {
        pt.add(kv.first, kv.second);
    }

    std::ostringstream output;
    try {
        ptree::write_json(output, pt, false);
    } catch (ptree::ptree_error const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "storePacked error: " << exc.what()
             << " data=\"" << util::printable(data) << "\"");
        throw lsst::qserv::css::KeyValueError(key, "json packing failed: " + std::string(exc.what()));
    }

    // ptree inserts newlines in json output and we prefer not to have newlines.
    // note that json value are not supposed to have newlines, only separators can be.
    std::string packed = output.str();
    std::replace(packed.begin(), packed.end(), '\n', ' ');

    // store it
    _kvI->set(key + "/" + ::_packedKeyName, packed);
}

void
CssAccess::_fillPartTableParams(std::map<std::string, std::string>& paramMap,
                                PartTableParams& params,
                                std::string const& tableKey) const {
    params.dirDb = paramMap["partitioning/dirDb"];
    params.dirTable = paramMap["partitioning/dirTable"];
    params.dirColName = paramMap["partitioning/dirColName"];
    params.latColName = paramMap["partitioning/latColName"];
    params.lonColName = paramMap["partitioning/lonColName"];
    params.partitioned = paramMap.count("partitioning") > 0;
    try {
        auto iter = paramMap.find("partitioning/subChunks");
        if (iter != paramMap.end()) {
            params.subChunks = std::stoi(iter->second);
        }
        iter = paramMap.find("partitioning/overlap");
        if (iter != paramMap.end()) {
            params.overlap = std::stod(iter->second);
        }
    } catch (std::exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "One of the sub-keys is not numeric: "
             << util::printable(paramMap));
        throw KeyValueError(tableKey + "/partitioning", "one of the sub-keys is not numeric: " +
                            std::string(exc.what()));
    }
}

void
CssAccess::_fillMatchTableParams(std::map<std::string, std::string>& paramMap,
                                 MatchTableParams& params) const {
    params.dirTable1 = paramMap["match/dirTable1"];
    params.dirColName1 = paramMap["match/dirColName1"];
    params.dirTable2 = paramMap["match/dirTable2"];
    params.dirColName2 = paramMap["match/dirColName2"];
    params.flagColName = paramMap["match/flagColName"];
}

void
CssAccess::_fillScanTableParams(std::map<std::string, std::string>& paramMap,
                                ScanTableParams& params,
                                std::string const& tableKey) const {
    try {
        auto iter = paramMap.find("sharedScan/lockInMem");
        if (iter != paramMap.end()) {
            params.lockInMem = std::stoi(paramMap["sharedScan/lockInMem"]);
        }
        iter = paramMap.find("sharedScan/scanRating");
        if (iter != paramMap.end()) {
            params.scanRating = std::stoi(paramMap["sharedScan/scanRating"]);
        }
    } catch (std::exception const& exc) {
        LOGS(_log, LOG_LVL_ERROR, "One of the sub-keys is not numeric: "
             << util::printable(paramMap));
        throw KeyValueError(tableKey + "/sharedScan", "one of the sub-keys is not numeric: " +
                            std::string(exc.what()));
    }
}

}}} // namespace lsst::qserv::css
