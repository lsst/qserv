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

namespace lsst {
namespace qserv {
    // Forward
    class SqlConnection;
    class SqlConfig;
    class SqlErrorObject;
}}

namespace lsst {
namespace qserv {
namespace worker {

class Metadata {
public:
    Metadata(SqlConfig const& qmsConnCfg);
    ~Metadata();
    bool registerQservedDb(std::string const& dbName, 
                           std::string const& baseDir,
                           SqlConnection&, SqlErrorObject&);
    bool unregisterQservedDb(std::string const& dbName,
                             std::string& dbPathToDestroy,
                             SqlConnection&, SqlErrorObject&);
    bool destroyWorkerMetadata(SqlConnection&, SqlErrorObject&);
    bool showMetadata(SqlConnection&, SqlErrorObject&);
    bool generateExportPaths(std::string const& baseDir,
                             SqlConnection&, SqlErrorObject&,
                             std::vector<std::string>& exportPaths);
    bool generateExportPathsForDb(std::string const& baseDir,
                                  std::string const& dbName,
                                  SqlConnection&, SqlErrorObject&,
                                  std::vector<std::string>& exportPaths);

private:
    bool generateExportPathsForDb(std::string const&,
                                  std::string const&,
                                  std::vector<std::string const> const&,
                                  SqlConnection&,
                                  SqlErrorObject&,
                                  std::vector<std::string>&);
    static int extractChunkNo(std::string const&);
    bool isRegistered(std::string const& dbName,
                      SqlConnection& sqlConn,
                      SqlErrorObject& errObj);
    void addChunk(int chunkNo,
                  std::string const& baseDir,
                  std::string const& dbName,
                  std::vector<std::string>& exportPaths);

    bool getDbInfoFromQms(std::string const&, int& dbId, 
                          std::string& dbUuid, SqlErrorObject& errObj);

private:
    std::string _workerMetadataDbName;
    SqlConfig* _qmsConnCfg; // host, port, user, pass for qms
};

}}} // namespace lsst.qserv.worker

#endif // LSST_QSERV_WORKER_METADATA_H
