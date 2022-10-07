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
#ifndef LSST_QSERV_REPLICA_FILEINGESTAPP_H
#define LSST_QSERV_REPLICA_FILEINGESTAPP_H

// System headers
#include <list>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Application.h"
#include "replica/Csv.h"
#include "replica/IngestClient.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class FileIngestApp implements a tool which acts as a catalog data loading
 * client of the Replication system's catalog data ingest server.
 */
class FileIngestApp : public Application {
public:
    typedef std::shared_ptr<FileIngestApp> Ptr;

    /**
     * Structure FileIngestSpec represents a specification for a single file to
     * be ingested.
     */
    struct FileIngestSpec {
        std::string workerHost;           /// The host name or an IP address of a worker
        uint16_t workerPort = 0;          /// The port number of the Ingest Service
        TransactionId transactionId = 0;  /// An identifier of the super-transaction
        std::string tableName;            /// The base name of a table to be ingested
        std::string tableType;            /// The type of the table. Allowed options: 'P' or 'R'
        std::string inFileName;           /// The name of a local file to be ingested
    };

    /**
     * Read file ingest specifications from a JSON object. The schema of an input file
     * depends on a value of the 'shortFormat' parameter passed into the method.
     * If the parameter is set to 'true' then the following schema will be expected
     * for each object found in the file:
     * @code
     * [
     *   {"worker-host":<string>,
     *    "worker-port":<number>,
     *    "path":<string>
     *   },
     *   ...
     * ]
     * @code
     * Otherwise, the following schema will be expected:
     * @code
     * [
     *   {"worker-host":<string>,
     *    "worker-port":<number>,
     *    "transaction-id":<number>,
     *    "table":<string>,
     *    "type":<string>,
     *    "path":<string>
     *   },
     *   ...
     * ]
     * @code
     *
     * Notes on the values of the parameters :
     * - "worker-host" is a DNS name or an IP address
     * - "worker-port" is a 16-bit unsigned integer number
     * - "transaction-id" is a 32-bit unsigned integer number
     * - "type" is either "R" for the regular table, or "P" for the partitioned one
     * - "path" is a path to the file to be read. The file has to be readable by the application
     *
     * @param jsonObj specifications packaged into a JSON object
     * @param shortFormat (optional) flag which is set to 'true' would ommit reading
     *   transaction identifiers and table specifications (name and type) from
     *   an input object. In this case values of the remaining optional parameters will
     *   be used to set values of the missing fields. If the flag is set to 'false' then
     *   the optional parameters will be ignored and the corresponding specifications
     *   will be pulled from the input JSON object.
     * @param transactionId (optional) identifier of a transaction (if shortFormat=false)
     * @param tableName (optional) the name of a table (if shortFormat=false)
     * @param tableType (optional) the type of a table (if shortFormat=false)
     * @return a collection of specifications
     * @throws std::invalid_argument if the string can't be parsed, or if strings
     *   don't have a valid payload in the shortFormat=false mode.
     */
    static std::list<FileIngestSpec> parseFileList(nlohmann::json const& jsonObj, bool shortFormat = false,
                                                   TransactionId transactionId = 0,
                                                   std::string const& tableName = std::string(),
                                                   std::string const& tableType = std::string());

    /**
     * Structure ChunkContribution represents attributes of a chunk contribution
     * file.
     */
    struct ChunkContribution {
        unsigned int chunk = 0;
        bool isOverlap = false;
    };

    /**
     * Parse the file name (no folder allowed) and extract chunk attributes.
     * Allowed file names:
     * @code
     *   chunk_<number>.txt
     *   chunk_<number>_overlap.txt
     * @code
     * @param filename the name of the chunk contribution files
     * @return an object encapsulating the attributes
     */
    static ChunkContribution parseChunkContribution(std::string const& filename);

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    FileIngestApp() = delete;
    FileIngestApp(FileIngestApp const&) = delete;
    FileIngestApp& operator=(FileIngestApp const&) = delete;

    ~FileIngestApp() override = default;

protected:
    int runImpl() final;

private:
    FileIngestApp(int argc, char* argv[]);

    /**
     * Parse the input file t locate rows as per the specifications.
     */
    void _parseFile() const;

    /**
     * Read ingest specifications from a file supplied via the corresponding
     * command line parameter with command 'FILE-LIST'.
     * @param shortFormat the flag which is 'true' would omit reading transaction
     *   identifiers and table specifications (name and type) from a file.
     * @return a list of file specifications
     */
    std::list<FileIngestSpec> _readFileList(bool shortFormat) const;

    /**
     * Ingest a single file as per the ingest specification
     * @param file a specification of the file
     * @throws invalid_argument for non existing files or incorrect file names
     */
    void _ingest(FileIngestSpec const& file) const;

    std::string _command;       /// 'FILE' or 'FILE-LIST' ingest scenarios
    std::string _fileListName;  /// The name of a file to read info for 'FILE-LIST' scenario

    csv::DialectInput _dialectInput;

    unsigned int _maxNumWarnings = 0;
    size_t _recordSizeBytes = IngestClient::defaultRecordSizeBytes;

    FileIngestSpec _file;  /// File specification for the single file ingest ('FILE'))

    std::string _inFileName;   /// The name of a file to read from.
    std::string _outFileName;  /// The name of a file to write into.

    bool _verbose = false;  /// Print various stats upon a completion of the ingest
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_FILEINGESTAPP_H */
