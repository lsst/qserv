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
#ifndef LSST_QSERV_CSS_CSSACCESS_H
#define LSST_QSERV_CSS_CSSACCESS_H

// System headers
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third-party headers

// LSST headers

// Qserv headers
#include "css/MatchTableParams.h"
#include "css/NodeParams.h"
#include "css/PartTableParams.h"
#include "css/ScanTableParams.h"
#include "css/StripingParams.h"
#include "css/TableParams.h"

namespace lsst {
namespace qserv {
namespace css {

class EmptyChunks;
class KvInterface;

/// @addtogroup css

/**
 *  @ingroup css
 *
 *  @brief C++ source file code template.
 *
 *  Class which provides access to information in Qserv Central State System.
 *
 *  This class is a wrapper around key-value storage (KvInterface) instance
 *  with knowledge about structure of CSS keys, packing/unpacking mechanism,
 *  etc.
 *
 *  This class  also provides information about empty chunk list. This will
 *  likely migrate to some different interface (e.g. become a part of
 *  secondary index) so it should be considered as temporary here.
 *
 *  This is a concrete class, instances can be copied around and all copies
 *  share the same KvInterface instance (and empty chunk list).
 */

class CssAccess {
public:

    /**
     *  Create CssAccess instance from existing key-value data in a stream.
     *
     *  Stream should contain a set of key-value pairs, pairs are separated
     *  from each other by newline character, key is separated from value by TAB
     *  character. Empty value can be represented by \N sequence (backslash-N).
     *  Neither keys nor values can contain newline or TAB.
     *
     *  @param stream:  stream with initial data
     *  @param emptyChunkPath:  path to empty chunk list file
     *  @param readOnly:  if true then KV storage will be set read-only
     *                    after loading initial data.
     */
    static std::shared_ptr<CssAccess> createFromStream(std::istream& stream,
                                                       std::string const& emptyChunkPath,
                                                       bool readOnly = false);

    /**
     *  Create CssAccess instance from existing key-value data.
     *
     *  Data is a string containing a set of key-value pairs, pairs are separated
     *  from each other by newline character, key is separated from value by TAB
     *  character. Empty value can be represented by \N sequence (backslash-N).
     *  Neither keys nor values can contain newline or TAB.
     *
     *  @param data:  initial data to load into KV store
     *  @param emptyChunkPath:  path to empty chunk list file
     *  @param readOnly:  if true then KV storage will be set read-only
     *                    after loading initial data.
     */
    static std::shared_ptr<CssAccess> createFromData(std::string const& data,
                                                     std::string const& emptyChunkPath,
                                                     bool readOnly = false);

    /**
     *  Create CssAccess instance from configuration dictionary.
     *
     *  Accepts dictionary containing all needed parameters, there is one
     *  required key "technology" in the dictionary, all other keys depend
     *  on the value of "technology" key. Here are possible values:
     *   'mem': other keys (all optional, file and data keys are exclusive):
     *       'file': name of the file containing initial data
     *       'data': string containing initial data (like in createFromData())
     *   'mysql': other keys (all optional):
     *       'hostname': string with mysql server host name or IP address
     *       'port': port number of mysql server (encoded as string)
     *       'socket': unix socket name
     *       'username': mysql user name
     *       'password': user password
     *       'database': database name
     *
     *  @param config:  configuration map
     *  @param emptyChunkPath:  path to empty chunk list file
     *  @param readOnly:  if true then KV storage will be set read-only
     *                    after loading initial data.
     *
     * @throws ConfigError: if config map is invalid
     * @throws CssError: for all CSS errors
     */
    static std::shared_ptr<CssAccess> createFromConfig(std::map<std::string, std::string> const& config,
                                                       std::string const& emptyChunkPath,
                                                       bool readOnly = false);

    /**
     *  Returns current compiled-in version number of CSS data structures.
     *  This is not normally useful for clients but can be used by various tests.
     */
    static int cssVersion();

    /**
     * @brief Returns the list of known databases.
     */
    std::vector<std::string> getDbNames() const;

    /**
     * @brief Returns status information for all databases.
     *
     * Returns mapping object with database name as a key and
     * database status as as a value.
     *
     * @throws CssError: for all CSS errors
     */
    std::map<std::string, std::string> getDbStatus() const;

    /**
     * @brief Change database status.
     *
     * @param dbName:  Database name
     * @param status:  Database status
     * @throws NoSuchDb: if database does not exist
     * @throws CssError: for all CSS errors
     */
    void setDbStatus(std::string const& dbName, std::string const& status);

    /**
     * @brief Returns true if database name is defined in CSS.
     *
     * @param dbName: database name
     * @throws CssError: for all CSS errors
     */
    bool containsDb(std::string const& dbName) const;

    /**
     * @brief Returns database's striping parameters.
     *
     * If database is defined but no partitioning information found it
     * returns default-constructed StripingParams instance.
     *
     * @param dbName: database name
     * @throws NoSuchDb: if database does not exist
     * @throws CssError: for all other errors
     */
    StripingParams getDbStriping(std::string const& dbName) const;

    /**
     * @brief Create new database in CSS.
     *
     * @param dbName: new database name
     * @param striping: striping parameters for new database, if
     *        striping.stripes is 0 (as in default-constructed
     *        StripingParameters) then database is not partitioned
     * @param storageClass: one of "L1", "L2", "L3"
     * @param releaseStatus: string, one of "RELEASED", "UNRELEASED"
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void createDb(std::string const& dbName,
                  StripingParams const& striping,
                  std::string const& storageClass,
                  std::string const& releaseStatus);

    /**
     * @brief Create new database in CSS base on existing database.
     *
     * @param dbName: new database name
     * @param templateDbName: name of the existing database
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void createDbLike(std::string const& dbName,
                      std::string const& templateDbName);

    /**
     * @brief Deletes database from CSS.
     *
     * @param dbName: database name
     * @throws NoSuchDb: if database does not exist
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void dropDb(std::string const& dbName);

    /**
     * @brief Returns the list of tables in a database.
     *
     * @param dbName: database name
     * @param readyOnly: if true then return tables which have "READY" status,
     *                   otherwise all table names are returned
     * @throws NoSuchDb: if database does not exist
     * @throws CssError: for all other errors
     */
    std::vector<std::string> getTableNames(std::string const& dbName, bool readyOnly=true) const;

    /**
     * @brief Returns status information for all table in a database.
     *
     * Returns mapping object with table name as a key and
     * table status as as a value.
     *
     * @throws NoSuchDb: if database does not exist
     * @throws CssError: for all other errors
     */
    std::map<std::string, std::string> getTableStatus(std::string const& dbName) const;

    /**
     * @brief Change table status.
     *
     * @param dbName:  Database name
     * @param tableName:  Table name
     * @param status:  Database status
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all CSS errors
     */
    void setTableStatus(std::string const& dbName, std::string const& tableName, std::string const& status);

    /**
     * @brief Returns true if table name is defined in CSS.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @param readyOnly: if true then table must have "READY" status, otherwise any status
     *                   is OK
     * @throws NoSuchDb: if database does not exist
     * @throws CssError: for all other errors
     */
    bool containsTable(std::string const& dbName, std::string const& tableName, bool readyOnly=true) const;

    /**
     * @brief Returns table schema.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all other errors
     */
    std::string getTableSchema(std::string const& dbName, std::string const& tableName) const;

    /**
     * @brief Returns match table metadata.
     *
     * Deprecated: use getTableParams() instead to get a consistent set of all table parameters.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all other errors
     */
    MatchTableParams getMatchTableParams(std::string const& dbName,
                                         std::string const& tableName) const;

    /**
     * @brief Returns partitioning table metadata.
     *
     * Deprecated: use getTableParams() instead to get a consistent set of all table parameters.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all other errors
     */
    PartTableParams getPartTableParams(std::string const& dbName,
                                       std::string const& tableName) const;

    /**
     * @brief Returns shared scan table metadata.
     *
     * Deprecated: use getTableParams() instead to get a consistent set of all table parameters.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all other errors
     */
    ScanTableParams getScanTableParams(std::string const& dbName,
                                       std::string const& tableName) const;

    /**
     * @brief Returns complete table metadata.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws CssError: for all other errors
     */
    TableParams getTableParams(std::string const& dbName, std::string const& tableName) const;

    /**
     * @brief Create new table in a database.
     *
     * This method is used to create non-match tables only (partitioned or not).
     *
     * @param dbName: database name
     * @param tableName: table name
     * @param schema: table schema
     * @param partParams: table partitioning parameters, if this is
     *        default-constructed object then table is not partitioned.
     * @param scanParams: table shared can parameters.
     * @throws TableExists: if CSS already has this table
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void createTable(std::string const& dbName,
                     std::string const& tableName,
                     std::string const& schema,
                     PartTableParams const& partParams,
                     ScanTableParams const& scanParams);

    /**
     * @brief Create new table in a database.
     *
     * This method is used to create non-match tables only (partitioned or not).
     *
     * @param dbName: database name
     * @param tableName: table name
     * @param schema: table schema
     * @param matchParams: match table parameters
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void createMatchTable(std::string const& dbName,
                          std::string const& tableName,
                          std::string const& schema,
                          MatchTableParams const& matchParams);

    /**
     * @brief Delete table from CSS.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws NoSuchTable: if table (or database) does not exist
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all other errors
     */
    void dropTable(std::string const& dbName, std::string const& tableName);

    /**
     * @brief Returns the list of nodes defined in CSS.
     *
     * @throws CssError: for all CSS errors
     */
    std::vector<std::string> getNodeNames() const;

    /**
     * @brief Returns node metadata for specific node.
     *
     * @param nodeName: name of the node
     * @throws NoSuchNode: if node is not defined in CSS
     * @throws CssError: for all CSS errors
     */
    NodeParams getNodeParams(std::string const& nodeName) const;

    /**
     * @brief Returns node metadata for all nodes.
     *
     * Returns map which has node names as keys and node parameters as values.
     *
     * @throws CssError: for all CSS errors
     */
    std::map<std::string, NodeParams> getAllNodeParams() const;

    /**
     * @brief Adds new node.
     *
     * @param nodeName: name of the new node
     * @param nodeParams: parameters for the new node
     * @throws NodeExists: if node is already defined in CSS
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all CSS errors
     */
    void addNode(std::string const& nodeName, NodeParams const& nodeParams);

    /**
     * @brief Updates node state.
     *
     * @param nodeName: name of the new node
     * @param newState: new node state
     * @throws NoSuchNode: if node is not defined in CSS
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all CSS errors
     */
    void setNodeState(std::string const& nodeName, std::string const& newState);

    /**
     * @brief Deletes node from CSS.
     *
     * @param nodeName: name of the new node
     * @throws NoSuchNode: if node is not defined in CSS
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all CSS errors
     */
    void deleteNode(std::string const& nodeName);

    /**
     * @brief Add one more chunk to CSS.
     *
     * Note: this method will likely be removed when we have new
     * dynamic data replication system.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @param chunk: chunk number or ID
     * @param nodeNames: list of node names
     * @throws ReadonlyCss: if CSS is using read-only storage
     * @throws CssError: for all CSS errors
     */
    void addChunk(std::string const& dbName,
                  std::string const& tableName,
                  int chunk,
                  std::vector<std::string> const& nodeNames);

    /**
     * @brief Returns metadata for all chunks of given table.
     *
     * Returned object is a mapping where key is the chunk number and value
     * is the list of node names where chunk is replicated.
     *
     * Note: this method will likely be removed when we have new
     * dynamic data replication system.
     *
     * @param dbName: database name
     * @param tableName: table name
     * @throws CssError: for all CSS errors
     */
    std::map<int, std::vector<std::string>> getChunks(std::string const& dbName,
                                                      std::string const& tableName);

    /**
     * @brief Access empty chunk list.
     */
    EmptyChunks const& getEmptyChunks() const { return *_emptyChunks; }

    /**
     *  Return underlying KvInterface instance.
     *
     *  This may be useful for testing, not much for regular clients.
     */
    std::shared_ptr<KvInterface> getKvI() { return _kvI; }

protected:

    // Construct from KvInterface instance and empty chunk list instance
    CssAccess(std::shared_ptr<KvInterface> const& kvInterface,
              std::shared_ptr<EmptyChunks> const& emptyChunks,
              std::string const& prefix = std::string());

    // Methods below are protected only for testing purposes so that one can
    // subclass CssAccess and expose these methods for testing

    /**
     * Throws NoSuchDb exception if the given database does not exist.
     */
    void _assertDbExists(std::string const& dbName) const;

    /**
     * Get values of specified sub-keys of a given key. This method knows how to unpack
     * packed keys. Returned map has sub-key names as keys, if sub-key is missing then
     * its key is not present in returned map.
     */
    std::map<std::string, std::string> _getSubkeys(std::string const& key,
                                                   std::vector<std::string> const& subKeys) const;

    /**
     * Unpack json string into key-value map, only one-level nesting
     * is supported, keys with more complex values are ignored. For empty
     * data string it returns empty map.
     */
    static std::map<std::string, std::string> _unpackJson(std::string const& key,
                                                          std::string const& data);

    /**
     *  Store data as a single packed key.
     */
    void _storePacked(std::string const& key, std::map<std::string, std::string> const& data);

    /**
     *  Validates version stored in KV. If version key exists but has
     *  unexpected value it throws VersionMismatchError. If version key
     *  is missing and mustExist is true it throws VersionMissingError.
     */
    void _checkVersion(bool mustExist=true) const;

private:

    void _fillPartTableParams(std::map<std::string, std::string>& paramMap,
                              PartTableParams& params,
                              std::string const& tableKey) const;
    void _fillMatchTableParams(std::map<std::string, std::string>& paramMap,
                               MatchTableParams& params) const;
    void _fillScanTableParams(std::map<std::string, std::string>& paramMap,
                              ScanTableParams& params,
                              std::string const& tableKey) const;

private:

    std::shared_ptr<KvInterface> _kvI;
    std::shared_ptr<EmptyChunks> _emptyChunks;
    std::string _prefix;    // optional prefix, for isolating tests from production
    mutable bool _versionOk;   // True if version is checked (and is OK)
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_CSSACCESS_H
