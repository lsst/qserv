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

// Class header
#include "replica/SqlApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/SqlCreateDbJob.h"
#include "replica/SqlCreateTableJob.h"
#include "replica/SqlDeleteDbJob.h"
#include "replica/SqlDeleteTableJob.h"
#include "replica/SqlDeleteTablePartitionJob.h"
#include "replica/SqlDisableDbJob.h"
#include "replica/SqlEnableDbJob.h"
#include "replica/SqlGrantAccessJob.h"
#include "replica/SqlJob.h"
#include "replica/SqlQueryJob.h"
#include "replica/SqlRemoveTablePartitionsJob.h"
#include "replica/SqlResultSet.h"
#include "replica/SqlSchemaUtils.h"

using namespace std;

namespace {

string const description =
    "This application executes the same SQL statement against worker databases of"
    " select workers. Result sets will be reported upon a completion of"
    " the application.";
} // namespace

namespace lsst {
namespace qserv {
namespace replica {

SqlApp::Ptr SqlApp::create(int argc, char* argv[]) {
    return Ptr(
        new SqlApp(argc, argv)
    );
}


SqlApp::SqlApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"QUERY",
         "CREATE_DATABASE", "DELETE_DATABASE", "ENABLE_DATABASE", "DISABLE_DATABASE",
         "GRANT_ACCESS",
         "CREATE_TABLE", "DELETE_TABLE", "REMOVE_TABLE_PARTITIONS", "DELETE_TABLE_PARTITION"},
        _command
    ).flag(
        "all-workers",
        "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY)."
        " If the flag was not specified then ENABLED workers in the READ-WRITE state will"
        " be assumed.",
        _allWorkers
    ).option(
        "worker-response-timeout",
        "Maximum timeout (seconds) to wait before queries would finish."
        " Setting this timeout to some reasonably low number would prevent the application from"
        " hanging for a substantial duration of time (which depends on the default Configuration)"
        " in case if some workers were down. The parameter applies to operations with"
        " the Replication workers.",
        _timeoutSec
    ).option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize
    );

    parser().command(
        "QUERY"
    ).required(
        "user",
        "Worker-side MySQL user account for executing the query.",
        _mysqlUser
    ).required(
        "password",
        "Password for the MySQL account.",
        _mysqlPassword
    ).required(
        "query",
        "The query to be executed on all select workers. If '-' is used instead of"
        " the query then the query will be read from the Standard Input stream."
        " NOTE: in the current implementation of the tool only a single query is"
        " expected in either form of the query input.",
        _query
    ).option(
        "max-rows",
        "The maximum number of rows to be pulled from result set at workers"
        " when processing queries. NOTE: This parameter has nothing to do with"
        " the SQL's 'LIMIT <num-rows>'. It serves as an additional fail safe"
        " mechanism preventing protocol buffers from being overloaded by huge"
        " result sets which might be accidentally initiated by users.",
        _maxRows
    );

    parser().command(
        "CREATE_DATABASE"
    ).required(
        "database",
        "The name of a database to be created.",
        _database
    );

    parser().command(
        "DELETE_DATABASE"
    ).required(
        "database",
        "The name of a database to be deleted.",
        _database
    );

    parser().command(
        "ENABLE_DATABASE"
    ).required(
        "database",
        "The name of a database to be enabled at Qserv workers.",
        _database
    );

    parser().command(
        "DISABLE_DATABASE"
    ).required(
        "database",
        "The name of a database to be disabled at Qserv workers.",
        _database
    );

    parser().command(
        "GRANT_ACCESS"
    ).required(
        "database",
        "The name of a database to be accessed.",
        _database
    ).required(
        "user",
        "The name of a user to be affected by the operation.",
        _mysqlUser
    );

    parser().command(
        "CREATE_TABLE"
    ).required(
        "database",
        "The name of an existing database where the table will be created.",
        _database
    ).required(
        "table",
        "The name of a table to be created.",
        _table
    ).required(
        "engine",
        "The name of a MySQL engine for the new table",
        _engine
    ).required(
        "schema-file",
        "The name of a file where column definitions of the table schema will be"
        " read from. If symbol '-' is passed instead of the file name then column"
        " definitions will be read from the Standard Input File. The file is required"
        " to have the following format: <column-name> <type>",
        _schemaFile
    ).option(
        "partition-by-column",
        "The name of a column which is used for creating the table based on"
        " the MySQL partitioning mechanism,",
        _partitionByColumn
    );

    parser().command(
        "DELETE_TABLE"
    ).required(
        "database",
        "The name of an existing database where the table is residing.",
        _database
    ).required(
        "table",
        "The name of an existing table to be deleted.",
        _table
    );

    parser().command(
        "REMOVE_TABLE_PARTITIONS"
    ).required(
        "database",
        "The name of an existing database where the table is residing.",
        _database
    ).required(
        "table",
        "The name of an existing table to be affected by the operation.",
        _table
    );

    parser().command(
        "DELETE_TABLE_PARTITION"
    ).required(
        "database",
        "The name of an existing database where the table is residing.",
        _database
    ).required(
        "table",
        "The name of an existing table to be affected by the operation.",
        _table
    ).required(
        "transaction",
        "An identifier of a super-transaction corresponding to a partition"
        " to be dropped from the table. The transaction must exist, and it"
        " should be in the ABORTED state.",
        _transactionId
    );
}


int SqlApp::runImpl() {

    // Limit request execution time if such limit was provided
    if (_timeoutSec != 0) {
        serviceProvider()->config()->setControllerRequestTimeoutSec(_timeoutSec);
    }

    auto const controller = Controller::create(serviceProvider());
    SqlJob::Ptr job;
    if (_command == "QUERY") {
        job = SqlQueryJob::create(_query, _mysqlUser, _mysqlPassword, _maxRows,
                                  _allWorkers, controller);
    } else if(_command == "CREATE_DATABASE") {
        job = SqlCreateDbJob::create(_database, _allWorkers, controller);
    } else if(_command == "DELETE_DATABASE") {
        job = SqlDeleteDbJob::create(_database, _allWorkers, controller);
    } else if(_command == "ENABLE_DATABASE") {
        job = SqlEnableDbJob::create(_database, _allWorkers, controller);
    } else if(_command == "DISABLE_DATABASE") {
        job = SqlDisableDbJob::create(_database, _allWorkers, controller);
    } else if(_command == "GRANT_ACCESS") {
        job = SqlGrantAccessJob::create(_database, _mysqlUser, _allWorkers, controller);
    } else if(_command == "CREATE_TABLE") {
        job = SqlCreateTableJob::create(_database, _table, _engine, _partitionByColumn,
                                        SqlSchemaUtils::readFromTextFile(_schemaFile),
                                        _allWorkers, controller);
    } else if(_command == "DELETE_TABLE") {
        job = SqlDeleteTableJob::create(_database, _table, _allWorkers, controller);
    } else if(_command == "REMOVE_TABLE_PARTITIONS") {
        job = SqlRemoveTablePartitionsJob::create(_database, _table, _allWorkers, controller);
    } else if(_command == "DELETE_TABLE_PARTITION") {
        job = SqlDeleteTablePartitionJob::create(_database, _table, _transactionId,
                                                 _allWorkers, controller);
    } else {
        throw logic_error(
                "SqlApp::" + string(__func__) + "  command='" + _command + "' is not supported");
    }
    job->start();
    job->wait();

    // Analyze and display results for each worker

    auto const& resultData = job->getResultData();
    for (auto&& itr : resultData.resultSets) {
        auto&& worker = itr.first;
        auto&& workerResultSet = itr.second;
        for (auto&& resultSet: workerResultSet) {
            if (resultSet.hasErrors()) {
                for (auto&& error: resultSet.allErrors()) {
                    cout << "worker: " << worker << ",  error: " << error << endl;
                }
                continue;
            }
            string const caption =
                    "worker: " + worker + ",  performance [sec]: "
                    + to_string(resultSet.performanceSec);

            if (_command == "QUERY") {
                auto const queryResultSetItr = resultSet.queryResultSet.find("");
                if (queryResultSetItr == resultSet.queryResultSet.end()) {
                    throw logic_error(
                            "SqlApp::" + string(__func__) + "  no result set found for the query");
                }
                string const indent = "";
                auto table = queryResultSetItr->second.toColumnTable(caption, indent);

                bool const topSeparator    = false;
                bool const bottomSeparator = false;
                bool const repeatedHeader  = false;

                table.print(cout, topSeparator, bottomSeparator, _pageSize, repeatedHeader);
                cout << "\n";
            } else {
                cout << caption << endl;
            }
        }
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
