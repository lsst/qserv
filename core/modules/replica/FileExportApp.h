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
#ifndef LSST_QSERV_REPLICA_FILEEXPORTAPP_H
#define LSST_QSERV_REPLICA_FILEEXPORTAPP_H

// System headers
#include <list>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Application.h"
#include "replica/Common.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class FileExportApp implements a tool which acts as a client of
 * the Replication system's table exporting server.
 */
class FileExportApp: public Application {
public:

    typedef std::shared_ptr<FileExportApp> Ptr;

    /**
     * Structure FileExportSpec represents a specification for a single file to
     * be exported.
     */
    struct FileExportSpec {
        std::string   workerHost;           /// The host name or an IP address of a worker
        uint16_t      workerPort = 0;       /// The port number of the Export Service
        std::string   databaseName;         /// The name of a database which has the desired table
        std::string   tableName;            /// The base name of a table to be exported
        unsigned int  chunk;                /// The chunk number (partitioned tables only)
        bool          overlap;              /// The flag is set to 'true' for the 'overlap
                                            /// tables (partitioned tables only)
        std::string   outFileName;          /// The name of a local file to be created
    };

    /**
     * Read file export specifications from a JSON object. Here is a schema of
     * the object:
     * @code
     * [
     *   {"worker-host":<string>,
     *    "worker-port":<number>,
     *    "database":<string>,
     *    "table":<string>,
     *    "chunk":<number>,
     *    "overlap":{0|1},
     *    "path":<string>
     *   },
     *   ...
     * ]
     * @code
     *
     * Notes on the values of the parameters :
     * - "worker-host" is a DNS name or an IP address
     * - "worker-port" is a 16-bit unsigned integer number
     * - "chunk" is a 32-bit unsigned integer  number of a chunk (ignored for the regular tables)
     * - "overlap" is either "1" for the "overlap" table, or "0" for other tables (ignored
     *   for the regular tables).
     * - "path" is a path to the file to be created. A folder where the file will
     *   be created has to be writeable by the application.
     *
     * Recommended names for the output files of the partitioned are (depending
     * if it's an 'overlap' table or not):
     * @code
     *   chunk_<number>.txt
     *   chunk_<number>_overlap.txt
     * @code
     * 
     * @param jsonObj specifications packaged into a JSON object
     * @return a collection of specifications
     * @throws std::invalid_argument if the string can't be parsed
     */
    static std::list<FileExportSpec> parseFileList(nlohmann::json const& jsonObj);

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    FileExportApp() = delete;
    FileExportApp(FileExportApp const&) = delete;
    FileExportApp& operator=(FileExportApp const&) = delete;

    ~FileExportApp() override = default;

protected:
    int runImpl() final;

private:
    FileExportApp(int argc, char* argv[]);

    /**
     * Read export specifications from a file supplied via the corresponding
     * command line parameter with command 'FILE-LIST'.
     * @return a list of file specifications
     */
    std::list<FileExportSpec> _readFileList() const;

    /**
     * Export a single file as per the export specification
     * @param file a specification of the file
     * @throws invalid_argument for non existing files or incorrect file names
     */
    void _export(FileExportSpec const& file) const;

    std::string _command;       /// 'FILE' or 'FILE-LIST' export scenarios
    std::string _fileListName;  /// The name of a file to read info for 'FILE-LIST' scenario

    /// An authorization key which should also be known to servers
    std::string _authKey;

    FileExportSpec _file;       /// File specification for the single file export ('FILE'))

    bool _verbose = false;      /// Print various stats upon a completion of the export
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_FILEEXPORTAPP_H */
