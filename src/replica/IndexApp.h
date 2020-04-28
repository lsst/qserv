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
#ifndef LSST_QSERV_REPLICA_INDEXAPP_H
#define LSST_QSERV_REPLICA_INDEXAPP_H

// System headers
#include <cstdint>
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class IndexApp implements a tool which launches a single job Controller in order
 * to harvest the "secondary index" data from the "director" tables of a select
 * database and aggregate these data at a specified destination.
 */
class IndexApp : public Application {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<IndexApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the base class' inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    IndexApp()=delete;
    IndexApp(IndexApp const&)=delete;
    IndexApp& operator=(IndexApp const&)=delete;

    ~IndexApp() final=default;

protected:
    int runImpl() final;

private:
    IndexApp(int argc, char* argv[]);

    /// The name of a database
    std::string _database;

    /// The name of the director table
    std::string _table;

    /// A unique identifier of a super-transaction (not used if its value stays default)
    TransactionId _transactionId = std::numeric_limits<TransactionId>::max();

    /// The destination type of the harvested data. Allowed values here
    /// are: "DISCARD", "FILE", "FOLDER", "TABLE.
    std::string _destination = "DISCARD";

    /// The optional parameter for a specific destination (depends
    /// the destination type).
    std::string _destinationPath;

    /// This flag is used together with the TABLE destination option to load
    /// contributions using "LOAD DATA LOCAL INFILE" protocol instead of
    /// just "LOAD DATA INFILE". See MySQL documentation for further details
    /// on this subject.
    bool _localFile = false;

    /// A connection URL to the MySQL service of the Qserv master database.
    std::string _qservCzarDbUrl;

    /// The flag which if set allows selecting all workers for the operation
    bool _allWorkers = false;

    /// The maximum timeout for the completion of requests sent to
    /// the Replication System workers. The default value (0)
    /// implies using the timeout found in the Configuration.
    unsigned int _timeoutSec = 0;

    /// Dump the detailed report on the requests if 'true'
    bool _detailedReport = false;

    /// The flag (if set) for "printing the vertical separator when displaying
    /// tabular data in reports
    bool _verticalSeparator = false;

    /// The number of rows in the table of chunks (0 means no pages)
    size_t _pageSize = 20;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_INDEXAPP_H */
