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
#include <memory>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Csv.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

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
    IngestFileSvc(ServiceProvider::Ptr const& serviceProvider,
                  std::string const& workerName);

    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }
    std::string const& workerName() const { return _workerName; }

    /**
     * Open a file.
     *
     * @param transactionId  An identifier of a "super-transaction" defining a context of the operation.
     * @param table  The base (or the final) name of a table where to upload the file.
     * @param dialect  The CSV dialect configured for interpreting the input stream, post-processing
     *   the data, and uploading the data into MySQL.
     * @param chunk  The number of a chunk (applies to partitioned tables only).
     * @param isOverlap  A kind of the table (applies to partitioned tables only).
     * @return The name of the open file.
     * @throw logic_error  For calling method in a wrong context (non-active trandaction, etc.)
     * @throw invalid_argument  For incorrect parameters on the input.
     * @throw runtime_error  Error wile creating, or opening a file.
     */
    std::string const& openFile(TransactionId transactionId,
                                std::string const& table,
                                csv::Dialect const& dialect,
                                unsigned int chunk=0,
                                bool isOverlap=false);

    /**
     * @note Each row will be prepended with an identifier of a transaction before being written.
     * @note Rows are supposed to be terminated according to the csv::Dialect specified when
     *   opening the file.
     * @param buf A pointer to a row to be written.
     * @param size The length of the row, including the line terminator.
     */
    void writeRowIntoFile(char const* buf, size_t size);

    /// Load the content of the current file into a database table
    void loadDataIntoTable();

    /// Make sure the currently open/created file gets closed and deleted
    void closeFile();

    /// @return The status of the file
    bool isOpen() const { return _file.is_open(); }

private:
    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string          const _workerName;

    // Parameters defining a scope of the operation are set/computed when opening a file.

    std::string   _fileName;
    TransactionId _transactionId = 0;
    std::string   _table;
    csv::Dialect  _dialect;
    bool          _isPartitioned = false;
    unsigned int  _chunk = 0;
    bool          _isOverlap = false;
    DatabaseInfo  _databaseInfo;    ///< Derived from the transaction identifier

    std::string _transactionIdField;    ///< The terminated field to be prepend at each row

    std::ofstream _file;

    size_t _totalNumRows = 0;   ///< The number of rows received and recorded
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTFILESVC_H
