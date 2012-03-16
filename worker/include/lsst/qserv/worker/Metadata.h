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
    class SqlErrorObject;
}}

namespace lsst {
namespace qserv {
namespace worker {

class Metadata {
public:
    Metadata(std::string const& workerId);
    bool registerQservedDb(std::string const& dbName,
                           std::string const& partitionedTables,
                           SqlConnection&,
                           SqlErrorObject&);
    bool unregisterQservedDb(std::string const& dbName,
                             SqlConnection&,
                             SqlErrorObject&);
    bool generateExportPaths(std::string const& baseDir,
                             SqlConnection&,
                             SqlErrorObject&,
                             std::vector<std::string>& exportPaths);
    bool generateExportPathsForDb(std::string const& baseDir,
                                  std::string const& dbName,
                                  std::string const& tableList,
                                  SqlConnection&,
                                  SqlErrorObject&,
                                  std::vector<std::string>& exportPaths);

private:
    static bool prepPartitionedTables(std::string&, SqlErrorObject&);
    static std::vector<std::string> tokenizeString(std::string const&);
    static int extractChunkNo(std::string const&);
    bool isRegistered(std::string const& dbName,
                      SqlConnection& sqlConn,
                      SqlErrorObject& errObj);

private:
    const std::string _metadataDbName;
};

}}} // namespace lsst.qserv.worker

#endif // LSST_QSERV_WORKER_METADATA_H
