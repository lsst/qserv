/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
#include <fstream>
#include <sstream>

// Third-party headers
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/constants.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "css/KvInterface.h"
#include "css/KvInterfaceImplMem.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.css.CssAccess");

}

namespace lsst {
namespace qserv {
namespace css {

std::shared_ptr<CssAccess>
CssAccess::makeMemCss(std::string const& mapPath, std::string const& emptyChunkPath) {
    std::ifstream f(mapPath);
    if(f.fail()) {
        throw ConnError();
    }
    return CssAccess::makeMemCss(f, emptyChunkPath);
}

std::shared_ptr<CssAccess>
CssAccess::makeMemCss(std::istream& mapStream, std::string const& emptyChunkPath) {
    return CssAccess::makeKvCss(std::make_shared<KvInterfaceImplMem>(mapStream),
                                emptyChunkPath);
}

std::shared_ptr<CssAccess>
CssAccess::makeKvCss(std::shared_ptr<KvInterface> const& kv, std::string const& emptyChunkPath) {
    return std::shared_ptr<CssAccess>(new CssAccess(kv, std::make_shared<EmptyChunks>(emptyChunkPath)));
}


// Construct from KvInterface instance and empty chunk list instance
CssAccess::CssAccess(std::shared_ptr<KvInterface> const& kvInterface,
                     std::shared_ptr<EmptyChunks> const& emptyChunks,
                     std::string const& prefix)
    : _kvI(kvInterface), _emptyChunks(emptyChunks), _prefix(prefix) {
}

int
CssAccess::cssVersion() {
    return VERSION;
}

std::vector<std::string>
CssAccess::getDbNames() const {
    std::string p = _prefix + "/DBS";
    return _kvI->getChildren(p);
}

bool
CssAccess::containsDb(std::string const& dbName) const {
    if (dbName.empty()) {
        LOGF(_log, LOG_LVL_DEBUG, "Empty database name passed.");
        return false;
    }
    std::string p = _prefix + "/DBS/" + dbName;
    bool ret = _kvI->exists(p);
    LOGF(_log, LOG_LVL_DEBUG, "containsDb(%1%): %2%" % dbName % ret);
    return ret;
}

StripingParams
CssAccess::getDbStriping(std::string const& dbName) const {
    LOGF(_log, LOG_LVL_DEBUG, "getDbStriping(%1%)" % dbName);
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
        LOGF(_log, LOG_LVL_ERROR, "one of the keys is not numeric: %s" % util::printable(keyMap));
        throw KeyValueError(pKey, "one of the keys is not numeric: " + std::string(exc.what()));
    }

    return striping;
}

void
CssAccess::createDb(std::string const& dbName,
                    StripingParams const& striping,
                    std::string const& storageClass,
                    std::string const& releaseStatus) {
    LOGF(_log, LOG_LVL_DEBUG, "createDb(%1%)" % dbName);

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
    _kvI->set(dbKey, "READY");
}

void
CssAccess::createDbLike(std::string const& dbName,
                        std::string const& templateDbName) {
    LOGF(_log, LOG_LVL_DEBUG, "createDbLike(%1%)" % dbName);

    std::vector<std::string> subKeys{"partitioningId", "releaseStatus", "storageClass"};
    auto dbMap = _getSubkeys(_prefix + "/DBS/" + templateDbName, subKeys);
    if (dbMap.empty()) {
        // nothing is found, check whether db exists
        _assertDbExists(templateDbName);
    }

    // make new database with the copy of all parameters
    std::string const dbKey = _prefix + "/DBS/" + dbName;
    _storePacked(dbKey, dbMap);
    _kvI->set(dbKey, "READY");
}

void
CssAccess::dropDb(std::string const& dbName) {
    LOGF(_log, LOG_LVL_DEBUG, "dropDb(%1%)" % dbName);

    std::string key = _prefix + "/DBS/" + dbName;

    // key is supposed to exist and key.json is optional
    try {
        LOGF(_log, LOG_LVL_DEBUG, "dropDb: try to delete packed key: %s.json" % key);
        _kvI->deleteKey(key + ".json");
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "dropDb: packed key is not found");
    }
    try {
        LOGF(_log, LOG_LVL_DEBUG, "dropDb: try to delete regular key: %s" % key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "dropDb: regular key is not found");
        throw NoSuchDb(dbName);
    }
}

std::vector<std::string>
CssAccess::getTableNames(std::string const& dbName, bool readyOnly) const {
    LOGF(_log, LOG_LVL_DEBUG, "getTableNames(%1%)" % dbName);

    std::string key = _prefix + "/DBS/" + dbName + "/TABLES";
    std::vector<std::string> names;
    try {
        names = _kvI->getChildren(key);
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "getTableNames: key is not found: %s" % key);
        throw NoSuchDb(dbName);
    }

    // remove names from packed keys
    auto it = std::remove_if(names.begin(), names.end(),
                             [](const std::string& name) {
                                return name.compare(name.size()-5, name.size(), ".json") == 0; });
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

bool
CssAccess::containsTable(std::string const& dbName, std::string const& tableName, bool readyOnly) const {
    LOGF(_log, LOG_LVL_DEBUG, "containsTable(%1%, %2%)" % dbName % tableName);

    std::string const key = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    // If key is not there pretend that its value is not "READY"
    std::string const val = _kvI->get(key, "DOES_NOT_EXIST");
    if (val == "DOES_NOT_EXIST") {
        // table key is not there at all, throw if database name is not good
        _assertDbExists(dbName);
        LOGF(_log, LOG_LVL_DEBUG, "containsTable: key is not found: %1%" % key);
        return false;
    }
    LOGF(_log, LOG_LVL_DEBUG, "containsTable: key value: %1%" % val);
    // if key value is not "READY" it likely means table is in the process
    // of being deleted, which is the same as if it does not exist
    if (readyOnly) return val == "READY";
    return true;
}

std::string
CssAccess::getTableSchema(std::string const& dbName, std::string const& tableName) const {
    LOGF(_log, LOG_LVL_DEBUG, "getTableSchema(%1%, %2%)" % dbName % tableName);

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
    LOGF(_log, LOG_LVL_DEBUG, "getMatchTableParams(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    MatchTableParams params;

    std::vector<std::string> subKeys{"dirTable1", "dirColName1", "dirTable2", "dirColName2", "flagColName"};
    auto paramMap = _getSubkeys(tableKey + "/match", subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    // fill the structure
    params.dirTable1 = paramMap["dirTable1"];
    params.dirColName1 = paramMap["dirColName1"];
    params.dirTable2 = paramMap["dirTable2"];
    params.dirColName2 = paramMap["dirColName2"];
    params.flagColName = paramMap["flagColName"];

    return params;
}

PartTableParams
CssAccess::getPartTableParams(std::string const& dbName,
                              std::string const& tableName) const {
    LOGF(_log, LOG_LVL_DEBUG, "getPartTableParams(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    PartTableParams params;

    std::vector<std::string> subKeys{"partitioning", "partitioning.json", "partitioning/subChunks",
        "partitioning/dirDb", "partitioning/dirTable", "partitioning/dirColName", "partitioning/latColName",
        "partitioning/lonColName", "partitioning/overlap", "partitioning/secIndexColName"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    // fill the structure
    params.dirDb = paramMap["partitioning/dirDb"];
    params.dirTable = paramMap["partitioning/dirTable"];
    params.dirColName = paramMap["partitioning/dirColName"];
    params.latColName = paramMap["partitioning/latColName"];
    params.lonColName = paramMap["partitioning/lonColName"];
    params.partitioned = paramMap.count("partitioning") + paramMap.count("partitioning.json") > 0;
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
        LOGF(_log, LOG_LVL_ERROR, "one of the sub-keys is not numeric: %s" % util::printable(paramMap));
        throw KeyValueError(tableKey + "/partitioning", "one of the sub-keys is not numeric: " +
                            std::string(exc.what()));
    }

    return params;
}

TableParams
CssAccess::getTableParams(std::string const& dbName, std::string const& tableName) const {
    LOGF(_log, LOG_LVL_DEBUG, "getTableParams(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    TableParams params;

    std::vector<std::string> subKeys{"partitioning/subChunks", "partitioning/dirDb",
        "partitioning/dirTable", "partitioning/dirColName", "partitioning/latColName",
        "partitioning/lonColName", "partitioning/overlap", "partitioning/secIndexColName",
        "match/dirTable1", "match/dirColName1", "match/dirTable2", "match/dirColName2",
        "match/flagColName", "partitioning", "partitioning.json"};
    auto paramMap = _getSubkeys(tableKey, subKeys);
    if (paramMap.empty()) {
        // check table key
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        return params;
    }

    // fill the structure
    params.match.dirTable1 = paramMap["match/dirTable1"];
    params.match.dirColName1 = paramMap["match/dirColName1"];
    params.match.dirTable2 = paramMap["match/dirTable2"];
    params.match.dirColName2 = paramMap["match/dirColName2"];
    params.match.flagColName = paramMap["match/flagColName"];
    params.partitioning.dirDb = paramMap["partitioning/dirDb"];
    params.partitioning.dirTable = paramMap["partitioning/dirTable"];
    params.partitioning.dirColName = paramMap["partitioning/dirColName"];
    params.partitioning.latColName = paramMap["partitioning/latColName"];
    params.partitioning.lonColName = paramMap["partitioning/lonColName"];
    params.partitioning.partitioned =
            paramMap.count("partitioning") + paramMap.count("partitioning.json") > 0;
    try {
        auto iter = paramMap.find("partitioning/subChunks");
        if (iter != paramMap.end()) {
            params.partitioning.subChunks = std::stoi(iter->second);
        }
        iter = paramMap.find("partitioning/overlap");
        if (iter != paramMap.end()) {
            params.partitioning.overlap = std::stod(iter->second);
        }
    } catch (std::exception const& exc) {
        LOGF(_log, LOG_LVL_ERROR, "one of the sub-keys is not numeric: %s" % util::printable(paramMap));
        throw KeyValueError(tableKey + "/partitioning", "one of the sub-keys is not numeric: " +
                            std::string(exc.what()));
    }

    return params;
}

void
CssAccess::createTable(std::string const& dbName,
                       std::string const& tableName,
                       std::string const& schema,
                       PartTableParams const& partParams) {
    LOGF(_log, LOG_LVL_DEBUG, "createTable(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    try {
        _kvI->create(tableKey, "CREATING");
    } catch (KeyExistsError const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "createTable: key already exists: %s" % tableKey);
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
    }

    // done
    _kvI->set(tableKey, "READY");
}

void
CssAccess::createMatchTable(std::string const& dbName,
                            std::string const& tableName,
                            std::string const& schema,
                            MatchTableParams const& matchParams,
                            int chunkLevel) {
    LOGF(_log, LOG_LVL_DEBUG, "createMatchTable(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    try {
        _kvI->create(tableKey, "CREATING");
    } catch (KeyExistsError const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "createMatchTable: key already exists: %s" % tableKey);
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
        // partitioning options
        if (chunkLevel > 1) {
            std::map<std::string, std::string> partMap{std::make_pair("subChunks", "1")};
            _storePacked(tableKey + "/partitioning", partMap);
        } else  if (chunkLevel == 1) {
            _kvI->create(tableKey + "/partitioning", "");
        }
    }

    // done, can mark table as ready
    _kvI->set(tableKey, "READY");
}

void
CssAccess::dropTable(std::string const& dbName, std::string const& tableName) {
    LOGF(_log, LOG_LVL_DEBUG, "dropTable(%1%, %2%)" % dbName % tableName);

    std::string const key = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;

    // key is supposed to exist and key.json is optional
    try {
        LOGF(_log, LOG_LVL_DEBUG, "dropTable: try to delete packed key: %s.json" % key);
        _kvI->deleteKey(key + ".json");
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "dropTable: packed key is not found");
    }
    try {
        LOGF(_log, LOG_LVL_DEBUG, "dropTable: try to delete regular key: %s" % key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "dropTable: regular key is not found");
        throw NoSuchTable(dbName, tableName);
    }
}

std::vector<std::string>
CssAccess::getNodeNames() const {
    std::string const key = _prefix + "/NODES";
    auto nodes = _kvI->getChildren(key);

    // Node name keys can be both packed or unpacked (meaning that one
    // or both of /NODES/node and /NODES/node.json can exist)
    for (auto& node: nodes) {
        if (node.compare(node.size()-5, node.size(), ".json") == 0) {
            node.erase(node.size()-5, node.size());
        }
    }

    // remove duplicates
    std::sort(nodes.begin(), nodes.end());
    auto it = std::unique(nodes.begin(), nodes.end());
    nodes.erase(it, nodes.end());

    return nodes;
}

NodeParams
CssAccess::getNodeParams(std::string const& nodeName) const {
    LOGF(_log, LOG_LVL_DEBUG, "getNodeParams(%1%)" % nodeName);

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
    params.status = paramMap[nodeName];
    params.type = paramMap[nodeName + "/type"];
    params.host = paramMap[nodeName + "/host"];
    try {
        auto iter = paramMap.find(nodeName + "/port");
        if (iter != paramMap.end()) {
            params.port = std::stoi(iter->second);
        }
    } catch (std::exception const& exc) {
        LOGF(_log, LOG_LVL_ERROR, "one of the sub-keys is not numeric: %s" % util::printable(paramMap));
        throw KeyValueError(key + "/" + nodeName,
                            "one of the sub-keys is not numeric: " + std::string(exc.what()));
    }

    return params;
}

std::map<std::string, NodeParams>
CssAccess::getAllNodeParams() const {
    LOGF(_log, LOG_LVL_DEBUG, "getAllParams()");

    std::string const key = _prefix + "/NODES";

    // we do not really care much about consistency here and
    // are prepared to deal with nodes disappearing.
    std::map<std::string, NodeParams> result;
    for (auto& node: getNodeNames()) {
        try {
            result.insert(std::make_pair(node, getNodeParams(node)));
        } catch (NoSuchNode const& exc) {
            LOGF(_log, LOG_LVL_DEBUG, "node disappeared");
        }
    }

    return result;
}

void
CssAccess::addNode(std::string const& nodeName, NodeParams const& nodeParams) {
    LOGF(_log, LOG_LVL_DEBUG, "addNode(%1%)" % nodeName);

    std::string const key = _prefix + "/NODES/" + nodeName;

    try {
        _kvI->create(key, "CREATING");
    } catch (KeyExistsError const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "addNode: key already exists: %s" % key);
        throw NodeExists(nodeName);
    }

    std::map<std::string, std::string> parMap{
        std::make_pair("type", nodeParams.type),
        std::make_pair("host", nodeParams.host),
        std::make_pair("port", std::to_string(nodeParams.port)),
    };
    _storePacked(key, parMap);

    // done
    _kvI->set(key, nodeParams.status);
}

void CssAccess::setNodeStatus(std::string const& nodeName, std::string const& newStatus) {
    LOGF(_log, LOG_LVL_DEBUG, "setNodeStatus(%1%, %2%)" % nodeName % newStatus);

    std::string const key = _prefix + "/NODES/" + nodeName;

    if (not _kvI->exists(key)) {
        LOGF(_log, LOG_LVL_DEBUG, "addNode: key does not exist: %s" % key);
        throw NoSuchNode(nodeName);
    }

    _kvI->set(key, newStatus);
}

void
CssAccess::deleteNode(std::string const& nodeName) {
    LOGF(_log, LOG_LVL_DEBUG, "deleteNode(%1%)" % nodeName);

    std::string const key = _prefix + "/NODES/" + nodeName;

    // key is supposed to exist and key.json is optional
    try {
        LOGF(_log, LOG_LVL_DEBUG, "deleteNode: try to delete packed key: %s.json" % key);
        _kvI->deleteKey(key + ".json");
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "deleteNode: packed key is not found");
    }
    try {
        LOGF(_log, LOG_LVL_DEBUG, "deleteNode: try to delete regular key: %s" % key);
        _kvI->deleteKey(key);
    } catch (NoSuchKey const& exc) {
        LOGF(_log, LOG_LVL_DEBUG, "deleteNode: regular key is not found");
        throw NoSuchNode(nodeName);
    }
}

void
CssAccess::addChunk(std::string const& dbName,
                    std::string const& tableName,
                    int chunk,
                    std::vector<std::string> const& nodeNames) {
    LOGF(_log, LOG_LVL_DEBUG, "addChunk(%1%, %2%, %3%)" % dbName % tableName % chunk);

    std::string const key = _prefix + (boost::format("/DBS/%s/TABLES/%s/CHUNKS/%s/REPLICAS") %
            dbName % tableName % chunk).str();

    for (auto& node: nodeNames) {
        auto path = _kvI->create(key + "/", "", true);
        LOGF(_log, LOG_LVL_DEBUG, "addChunk: New chunk replica key: %s" % path);
        std::map<std::string, std::string> chunkMap{std::make_pair("nodeName", node)};
        _storePacked(path, chunkMap);
    }
}

std::map<int, std::vector<std::string>>
CssAccess::getChunks(std::string const& dbName, std::string const& tableName) {
    LOGF(_log, LOG_LVL_DEBUG, "getChunks(%1%, %2%)" % dbName % tableName);

    std::string const tableKey = _prefix + "/DBS/" + dbName + "/TABLES/" + tableName;
    std::string const chunksKey = tableKey + "/CHUNKS";

    std::map<int, std::vector<std::string>> result;

    std::vector<std::string> chunks;
    try {
        chunks = _kvI->getChildren(chunksKey);
    } catch (NoSuchKey const& exc) {
        if (not _kvI->exists(tableKey)) throw NoSuchTable(dbName, tableName);
        LOGF(_log, LOG_LVL_DEBUG, "getChunks: No CHUNKS sub-key for: %s" % tableKey);
        return result;
    }

    for (auto& chunk: chunks) {
        int chunkId;
        try {
            chunkId = std::stoi(chunk);
        } catch (std::exception const& exc) {
            LOGF(_log, LOG_LVL_DEBUG, "getChunks: non-numeric chunk key: %s" % chunk);
            continue;
        }

        std::string const replicasKey = chunksKey + "/" + chunk + "/REPLICAS";
        std::vector<std::string> replicas;
        try {
            replicas = _kvI->getChildren(replicasKey);
            // strip .json and remove duplicates
            for (auto& replica: replicas) {
                if (replica.compare(replica.size()-5, replica.size(), ".json") == 0) {
                    replica.erase(replica.size()-5, replica.size());
                }
            }
            std::sort(replicas.begin(), replicas.end());
            auto it = std::unique(replicas.begin(), replicas.end());
            replicas.erase(it, replicas.end());
        } catch (std::exception const& exc) {
            LOGF(_log, LOG_LVL_DEBUG, "getChunks: replica key is missing: %s" % replicasKey);
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
        LOGF(_log, LOG_LVL_DEBUG, "Db '%1%' not found." % dbName);
        throw NoSuchDb(dbName);
    }
}

std::map<std::string, std::string>
CssAccess::_getSubkeys(std::string const& key, std::vector<std::string> const& subKeys) const {
    LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys(%s, %s)" % key % util::printable(subKeys));

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
            allKeys.push_back(parentKey + ".json");
        }

        allKeys.push_back(key + "/" + subKey);
    }
    LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys: parent keys: %s" % util::printable(parentKeys));
    LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys: looking for keys: %s" % util::printable(allKeys));

    // get everything in one call from KV store, this is
    // supposed to be consistent set of values
    auto keyMap = _kvI->getMany(allKeys);
    LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys: kvI returned: %s" % util::printable(keyMap));

    // unpack packed guys, and add unpacked keys to a key map, this does not overwrite
    // existing keys (meaning that regular key overrides same packed key)
    for (auto& parentKey: parentKeys) {
        std::string const packedKey = parentKey + ".json";
        auto iter = keyMap.find(packedKey);
        if (iter != keyMap.end()) {
            auto const packedMap = _unpackJson(iter->first, iter->second);
            LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys: packed keys: %s -> %s" %
                 packedKey % util::printable(packedMap));
            for (auto& packed: packedMap) {
                keyMap.insert(std::make_pair(parentKey + "/" + packed.first, packed.second));
            }
            keyMap.erase(iter);
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

    LOGF(_log, LOG_LVL_DEBUG, "_getSubkeys: result: %s" % util::printable(result));
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
            LOGF(_log, LOG_LVL_ERROR, "unpackJson error: %s data=\"%s\"" % exc.what() % data);
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
        LOGF(_log, LOG_LVL_ERROR, "storePacked error: %s data=\"%s\"" % exc.what() % util::printable(data));
        throw lsst::qserv::css::KeyValueError(key, "json packing failed: " + std::string(exc.what()));
    }

    // store it
    _kvI->set(key+".json", output.str());
}

}}} // namespace lsst::qserv::css
