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
#include "replica/SqlAlterTablesJob.h"
#include "replica/SqlCreateDbJob.h"
#include "replica/SqlCreateIndexesJob.h"
#include "replica/SqlCreateTableJob.h"
#include "replica/SqlCreateTablesJob.h"
#include "replica/SqlDeleteDbJob.h"
#include "replica/SqlDeleteTableJob.h"
#include "replica/SqlDeleteTablePartitionJob.h"
#include "replica/SqlDisableDbJob.h"
#include "replica/SqlDropIndexesJob.h"
#include "replica/SqlEnableDbJob.h"
#include "replica/SqlGetIndexesJob.h"
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

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;
bool const injectXrootdOptions = false;

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
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider,
            injectXrootdOptions
        ) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"ALTER_TABLES",
         "QUERY",
         "CREATE_DATABASE", "DELETE_DATABASE", "ENABLE_DATABASE", "DISABLE_DATABASE",
         "GRANT_ACCESS",
         "CREATE_TABLE", "CREATE_TABLES", "DELETE_TABLE", "REMOVE_TABLE_PARTITIONS", "DELETE_TABLE_PARTITION",
         "CREATE_INDEXES", "DROP_INDEXES", "GET_INDEXES"},
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
    ).option(
        "report-level",
        "The option which controls the verbosity of the job completion report."
        " Supported report levels:"
        " 0: no report, just return the completion status to the shell."
        " 1: report a summary, including the job completion status, the number"
        " of objects (tables/databases) failed to be processed, as well as the number"
        " of objects which have been successfully processed."
        " 2: report processing status of each object failed to be processed by the operation."
        " The result will include the name of the object (if any), the name of a worker on which"
        " the object was expected to be residing, the completion status of"
        " the operation, and an error message (if any) reported by the remote"
        " worker service. Results will be presented in a tabular format with a row"
        " per each object involved into the operation."
        " 3: also include into the report all objects which were successfully"
        " processed by the operation. This level will also trigger printing result sets"
        " for a query if command QUERY was requested.",
        _reportLevel
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
        " the MySQL partitioning mechanism.",
        _partitionByColumn
    );

    parser().command(
        "CREATE_TABLES"
    ).required(
        "database",
        "The name of an existing database where the tables will be created.",
        _database
    ).required(
        "table",
        "The base name for tables to be created.",
        _table
    ).required(
        "engine",
        "The name of a MySQL engine for the new tables",
        _engine
    ).required(
        "schema-file",
        "The name of a file where column definitions of the table schema will be"
        " read from. If symbol '-' is passed instead of the file name then column"
        " definitions will be read from the standard input stream. The file is required"
        " to have the following format: <column-name> <type>",
        _schemaFile
    ).option(
        "partition-by-column",
        "The name of a column which is used for creating the tables based on"
        " the MySQL partitioning mechanism.",
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
    ).flag(
        "ignore-non-partitioned",
        "The flag allowing to run this job multiple times w/o considering tables"
        " which don't have MySQL partitions. The partitions may have already been"
        " removed at prior invocations of the job.",
        _ignoreNonPartitioned
    );

    parser().command(
        "DELETE_TABLE_PARTITION"
    ).required(
        "transaction",
        "An identifier of a super-transaction corresponding to a partition"
        " to be dropped from the table. The transaction must exist, and it"
        " should be in the ABORTED state. NOTE: the name of a database will be"
        " be deduced from an association between transactions and databases.",
        _transactionId
    ).required(
        "table",
        "The name of an existing table to be affected by the operation.",
        _table
    );

    _configureTableCommands();
}


void SqlApp::_configureTableCommands() {

    // Common parameters & flags. Note that two required positional parameters 
    // added in the loop below will be present in the same order at each command.
    // The additional positional (required and/or optional) parameters will be
    // appended for each command at the next step.

    for (auto&& command: {"ALTER_TABLES", "CREATE_INDEXES", "DROP_INDEXES", "GET_INDEXES"}) {
        parser().command(
            command
        ).required(
            "database",
            "The name of an existing database where the table is residing.",
            _database
        ).required(
            "table",
            "The name of an existing table to be affected by the operation.",
            _table
        );
    }

    // Note that "ALTER_TABLES" doesn't have this flag since it affects all tables
    // regardless their status.

    for (auto&& command: {"CREATE_INDEXES", "DROP_INDEXES", "GET_INDEXES"}) {
        parser().command(
            command
        ).flag(
            "overlap",
            "The optional selector for a subset of the partitioned tables to be affected by"
            " the operation. If the flag is provided then only the so called 'overalp' will be"
            " included into the operation. Otherwise, the chunk tables will be included. The flag"
            " is ignored for the regular tables.",
            _overlap
        );
    }

    // Additional parameters, options and flags for some commands

    parser().command(
        "ALTER_TABLES"
    ).required(
        "alter-spec",
        "The specification of what's to change in table definitions as it follows"
        " after 'ALTER TABLE <table> ' in the corresponding SQL statement.",
        _alterSpec
    );

    parser().command(
        "CREATE_INDEXES"
    ).required(
        "name",
        "The name of an index to be created.",
        _indexName
    ).required(
        "type-specification",
        "The type specification of an index.",
        _indexSpecStr,
        {"DEFAULT", "UNIQUE", "FULLTEXT", "SPATIAL"}
    ).required(
        "columns-file",
        "The name of a file where to read definitions of the index's columns.",
        _indexColumnsFile
    ).optional(
        "comment",
        "The optional comment explaining an index.",
        _indexComment
    );

    parser().command(
        "DROP_INDEXES"
    ).required(
        "name",
        "The name of an index to be dropped.",
        _indexName
    );
}


int SqlApp::runImpl() {

    // Limit request execution time if such limit was provided
    if (_timeoutSec != 0) {
        bool const updatePersistentState = false;
        serviceProvider()->config()->setControllerRequestTimeoutSec(_timeoutSec, updatePersistentState);
    }

    auto const controller = Controller::create(serviceProvider());
    SqlJob::Ptr job;
    if(_command == "ALTER_TABLES") {
        job = SqlAlterTablesJob::create(_database, _table, _alterSpec, _allWorkers, controller);
    } else if(_command == "QUERY") {
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
    } else if(_command == "CREATE_TABLES") {
        job = SqlCreateTablesJob::create(_database, _table, _engine, _partitionByColumn,
                                         SqlSchemaUtils::readFromTextFile(_schemaFile),
                                         _allWorkers, controller);
    } else if(_command == "DELETE_TABLE") {
        job = SqlDeleteTableJob::create(_database, _table, _allWorkers, controller);
    } else if(_command == "REMOVE_TABLE_PARTITIONS") {
        job = SqlRemoveTablePartitionsJob::create(_database, _table, _allWorkers, _ignoreNonPartitioned,
                                                  controller);
    } else if(_command == "DELETE_TABLE_PARTITION") {
        job = SqlDeleteTablePartitionJob::create(_transactionId, _table, _allWorkers, controller);
    } else if(_command == "CREATE_INDEXES") {
        job = SqlCreateIndexesJob::create(_database, _table, _overlap,
                                          SqlRequestParams::IndexSpec(_indexSpecStr),
                                          _indexName,
                                          _indexComment,
                                          SqlSchemaUtils::readIndexSpecFromTextFile(_indexColumnsFile),
                                         _allWorkers, controller);
    } else if(_command == "DROP_INDEXES") {
        job = SqlDropIndexesJob::create(_database, _table, _overlap,
                                        _indexName,
                                        _allWorkers, controller);
    } else if(_command == "GET_INDEXES") {
        job = SqlGetIndexesJob::create(_database, _table, _overlap,
                                       _allWorkers, controller);
    } else {
        throw logic_error(
                "SqlApp::" + string(__func__) + "  command='" + _command + "' is not supported");
    }
    job->start();
    job->wait();

    if (_reportLevel > 0) {
        cout << endl;
        cout << "Job completion status: " << Job::state2string(job->extendedState()) << endl;

        auto&& resultData = job->getResultData();
        size_t numSucceeded = 0;
        map<ExtendedCompletionStatus,size_t> numFailed;
        resultData.iterate(
            [&numFailed, &numSucceeded](SqlJobResult::Worker const& worker,
                                        SqlJobResult::Scope const& object,
                                        SqlResultSet::ResultSet const& resultSet) {
                if (resultSet.extendedStatus == ExtendedCompletionStatus::EXT_STATUS_NONE) {
                    numSucceeded++;
                } else {
                    numFailed[resultSet.extendedStatus]++;
                }
            }
        );
        cout << "Object processing summary:\n"
             << "  succeeded: " << numSucceeded << "\n";
        if (numFailed.size() == 0) {
            cout << "  failed: " << 0 << endl;
        } else {
            cout << "  failed:\n";
            for (auto&& entry: numFailed) {
                auto const extendedStatus = entry.first;
                auto const counter = entry.second;
                cout << "    " << status2string(extendedStatus) << ": " << counter << endl;
            }
        }
        cout << endl;

        string const indent = "";
        bool const topSeparator = false;
        bool const bottomSeparator = false;
        bool const repeatedHeader = false;
        bool const verticalSeparator = true;

        auto const workerSummaryTable = resultData.summaryToColumnTable(
                "Worker requests statistics:", indent
        );
        workerSummaryTable.print(cout, topSeparator, bottomSeparator, _pageSize,
                                 repeatedHeader);
        cout << endl;

        if (_reportLevel > 1) {
            if (_command == "QUERY") {
                resultData.iterate([&](SqlJobResult::Worker const& worker,
                                       SqlJobResult::Scope const& scope,
                                       SqlResultSet::ResultSet const& resultSet) {
                    string const caption =
                        worker + ":" + scope + ":" + status2string(resultSet.extendedStatus)
                        + ":" + resultSet.error;
                    if (resultSet.extendedStatus != ExtendedCompletionStatus::EXT_STATUS_NONE) {
                        cout << caption << endl;
                    } else {
                        auto const table = resultSet.toColumnTable(
                                caption, indent, verticalSeparator
                        );
                        table.print(cout, topSeparator, bottomSeparator, _pageSize,
                                    repeatedHeader);
                    }
                    cout << endl;
                });
            } else {
                string const caption = "Result sets completion status:";
                bool const reportAll = _reportLevel > 2;
                auto const table = resultData.toColumnTable(
                        caption, indent, verticalSeparator, reportAll
                );
                table.print(cout, topSeparator, bottomSeparator, _pageSize,
                            repeatedHeader);
            }
        }
    }
    return job->extendedState() == Job::SUCCESS ? 0 : 1;
}

}}} // namespace lsst::qserv::replica
