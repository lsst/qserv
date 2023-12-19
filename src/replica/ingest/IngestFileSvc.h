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
#ifndef LSST_QSERV_REPLICA_INGESTFILESVC_H
#define LSST_QSERV_REPLICA_INGESTFILESVC_H

// System headers
#include <fstream>
#include <list>
#include <memory>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/ingest/TransactionContrib.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Csv.h"

// This header declarations
namespace lsst::qserv::replica {

// Forward declarations
namespace database::mysql {
class Warning;
}  // namespace database::mysql

/**
 * Class IngestFileSvc is used in the server-side implementations of
 * the point-to-point catalog data ingest services of the Replication system.
 * The class handles file upload into MySQL.
 * One instance of the class serves one file from one client at a time.
 */
class IngestFileSvc {
public:
    IngestFileSvc() = delete;
    IngestFileSvc(IngestFileSvc const&) = delete;
    IngestFileSvc& operator=(IngestFileSvc const&) = delete;

    /// Destructor (non-trivial because some resources need to be properly released)
    virtual ~IngestFileSvc();

protected:
    /// @see IngestFileSvc::create()
    IngestFileSvc(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName);

    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }
    std::string const& workerName() const { return _workerName; }
    unsigned int numWarnings() const { return _numWarnings; }
    std::list<database::mysql::Warning> const& warnings() const { return _warnings; }
    uint64_t numRowsLoaded() const { return _numRowsLoaded; }

    /**
     * Open a file.
     *
     * @param transactionId  An identifier of a "super-transaction" defining a context of the operation.
     * @param tableName  The base (or the final) name of a table where to upload the file.
     * @param dialect  The CSV dialect configured for interpreting the input stream, post-processing
     *   the data, and uploading the data into MySQL.
     * @param charsetName  The desired character set to be used when ingsting the contribution
     *   data into the destination table.
     * @param chunk  The number of a chunk (applies to partitioned tables only).
     * @param isOverlap  A kind of the table (applies to partitioned tables only).
     * @return The name of the open file.
     * @throw logic_error  For calling method in a wrong context (non-active trandaction, etc.)
     * @throw invalid_argument  For incorrect parameters on the input.
     * @throw runtime_error  Error wile creating, or opening a file.
     */
    std::string const& openFile(TransactionId transactionId, std::string const& tableName,
                                csv::Dialect const& dialect, std::string const& charsetName,
                                unsigned int chunk = 0, bool isOverlap = false);

    /**
     * @note Each row will be prepended with an identifier of a transaction before being written.
     * @note Rows are supposed to be terminated according to the csv::Dialect specified when
     *   opening the file.
     * @param buf A pointer to a row to be written.
     * @param size The length of the row, including the line terminator.
     */
    void writeRowIntoFile(char const* buf, size_t size);

    /// Load the content of the current file into a database table
    /// @param maxNumWarnings The optional limit for the number of MySQL warnings
    ///   to be captured when ingesting the contribution. If the default
    ///   value of 0 will be used then the implementation will use
    ///   the corresponding parameter of the configuration.
    void loadDataIntoTable(unsigned int maxNumWarnings = 0);

    /// Make sure the currently open/created file gets closed and deleted
    void closeFile();

    /// @return The status of the file
    bool isOpen() const { return _file.is_open(); }

private:
    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string const _workerName;

    // Parameters defining a scope of the operation are set/computed when opening a file.

    std::string _fileName;
    TransactionId _transactionId = 0;
    std::string _charsetName;
    csv::Dialect _dialect;
    unsigned int _chunk = 0;
    bool _isOverlap = false;
    DatabaseInfo _database;
    TableInfo _table;

    std::string _transactionIdField;  ///< The terminated field to be prepend at each row

    std::ofstream _file;

    size_t _totalNumRows = 0;  ///< The number of rows received and recorded

    // MySQL warnings (if any) captured after loading the contribution into the table.
    unsigned int _numWarnings = 0;
    std::list<database::mysql::Warning> _warnings;

    uint64_t _numRowsLoaded = 0;  ///< Th enumber of rows actually ingested into Qserv.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTFILESVC_H
