/* 
 * LSST Data Management System
 * Copyright 2008 - 2012 LSST Corporation.
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
 
#ifndef LSST_QSERV_WORKER_METADATA_H
#define LSST_QSERV_WORKER_METADATA_H

#include <string>
#include <vector>

#include "lsst/qserv/SqlConnection.hh"

namespace lsst {
namespace qserv {
    // Forward
    class SqlConfig;
}}

namespace lsst {
namespace qserv {
namespace worker {

class Metadata {
public:
    Metadata();
    ~Metadata();
    bool init(SqlConfig const&, // qmsConnCfg, 
              SqlConfig const&); // qmwConnCfg
    std::string getLastError() const;
    bool installMeta(std::string const&);
    bool destroyMeta();
    bool registerQservedDb(std::string const&);
    bool unregisterQservedDb(std::string const&);
    bool createExportPaths(std::string const&);
    bool getDbList(std::vector<std::string>&);
    bool showMetadata();

    struct TableChunks {
        std::string _tableName;
        std::vector<std::string> _chunksInDb;
    };

private:
    bool _unregisterQservedDb(std::string const&);
    bool _destroyExportPathWithPrefix();
    bool _destroyExportPath4Db(std::string const&);
    bool _generateExportPaths(std::vector<std::string>& exportPaths);
    static int _extractChunkNo(std::string const&);
    bool _isRegistered(std::string const&);
    void _addChunk(int, std::string const&, std::string const&,
                   std::vector<std::string>&);
    bool _generateExportPathsForDb(std::string const&,    // exportBaseDir
                                   std::string const&,    // dbName
                                   std::vector<std::string>&);
    bool _getTableChunksForDb(std::string const&,         // dbName
                              std::vector<TableChunks>&);
    bool _getExportBaseDir(std::string&);
    bool _getExportPathWithPrefix(std::string&);          // the path to be set
    bool _getInfoAboutAllDbs(std::vector<std::string>&,   // dbIds
                             std::vector<std::string>&,   // dbNames
                             std::vector<std::string>&);  // dbUuids
    bool _getDbInfoFromQms(std::string const&,            // dbName
                           int&,                          // dbId
                           std::string&);                 // dbUuid
    bool _getPartTablesFromQms(std::string const&,        // dbName
                               std::vector<std::string>&);// partTables

private:
    std::string _workerMetadataDbName;
    SqlConnection _sqlConn;
    SqlErrorObject _errObj;
    SqlConfig* _qmsConnCfg; // host, port, user, pass for qms
};

}}} // namespace lsst.qserv.worker

#endif // LSST_QSERV_WORKER_METADATA_H
