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
#ifndef LSST_QSERV_REPLICA_SQLAPP_H
#define LSST_QSERV_REPLICA_SQLAPP_H

// System headers
#include <cstdint>

// Qserv headers
#include "replica/Application.h"
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class SqlApp implements a tool which executes the same SQL
 * statement against worker databases of select workers. Result sets
 * will be reported upon a completion of the application.
 */
class SqlApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlApp> Ptr;

    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    SqlApp() = delete;
    SqlApp(SqlApp const&) = delete;
    SqlApp& operator=(SqlApp const&) = delete;

    ~SqlApp() final = default;

protected:

    int runImpl() final;

private:

    SqlApp(int argc, char* argv[]);

    std::string _command;
    std::string _mysqlUser;
    std::string _mysqlPassword;
    std::string _query;
    std::string _database;
    std::string _table;
    std::string _engine;
    std::string _schemaFile;
    std::string _partitionByColumn;     

    TransactionId _transactionId = 0;   /// An identifier of a super-transaction corresponding to
                                        /// to a MySQL partition.

    uint64_t _maxRows = 10000;  /// the "hard" limit for the result set extractor.
                                /// This is not the same as SQL's 'LIMIT <num-rows>'.
    bool _allWorkers = false;   /// send the query to all workers regardless of their status

    bool _ignoreNonPartitioned = false; /// To allow (if 'true') running the partitions removal
                                        /// job multiple times.

    unsigned int _timeoutSec = 300; /// When waiting for the completion of the queries

    size_t _pageSize = 100; /// Rows per page in the printout

    unsigned int _reportLevel = 0;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_SQLAPP_H */
