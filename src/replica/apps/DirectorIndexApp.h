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
#ifndef LSST_QSERV_REPLICA_DIRECTORINDEXAPP_H
#define LSST_QSERV_REPLICA_DIRECTORINDEXAPP_H

// System headers
#include <cstdint>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"
#include "replica/util/Common.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DirectorIndexApp implements a tool which launches a single job Controller in order
 * to harvest the "director" index data from the "director" table of a select
 * database and load the data into the corresponding "director" index table.
 */
class DirectorIndexApp : public Application {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<DirectorIndexApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the base class' inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    DirectorIndexApp() = delete;
    DirectorIndexApp(DirectorIndexApp const&) = delete;
    DirectorIndexApp& operator=(DirectorIndexApp const&) = delete;

    ~DirectorIndexApp() final = default;

protected:
    int runImpl() final;

private:
    DirectorIndexApp(int argc, char* argv[]);

    /// The name of a database
    std::string _database;

    /// The name of the director table
    std::string _table;

    /// A unique identifier of a super-transaction (not used if its value stays default)
    TransactionId _transactionId = std::numeric_limits<TransactionId>::max();

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

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_DIRECTORINDEXAPP_H */
