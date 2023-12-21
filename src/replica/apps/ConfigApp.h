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
#ifndef LSST_QSERV_REPLICA_CONFIGAPP_H
#define LSST_QSERV_REPLICA_CONFIGAPP_H

// Qserv headers
#include <map>
#include <string>

// Qserv headers
#include "replica/apps/ConfigAppBase.h"

// Forward declarations
namespace lsst::qserv::replica::detail {
class Command;
}  // namespace lsst::qserv::replica::detail

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ConfigApp implements a tool for inspecting/modifying configuration
 * records stored in the MySQL/MariaDB database.
 */
class ConfigApp : public ConfigAppBase {
public:
    typedef std::shared_ptr<ConfigApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    ConfigApp() = delete;
    ConfigApp(ConfigApp const&) = delete;
    ConfigApp& operator=(ConfigApp const&) = delete;

    ~ConfigApp() override = default;

protected:
    /// @see ConfigAppBase::runSubclassImpl()
    virtual int runSubclassImpl() final;

private:
    /// @see ConfigApp::create()
    ConfigApp(int argc, char* argv[]);

    /**
     * Configure command-line options for the worker management commands
     * 'ADD_WORKER' or 'UPDATE_WORKER'.
     * @param command The parser's command to be configured.
     */
    void _configureWorkerOptions(detail::Command& command);

    /**
     * Dump the Configuration into the standard output stream
     * @return A status code to be returned to the shell.
     */
    int _dump() const;

    /**
     * Dump the Configuration into the standard output stream in  format which could
     * be used for initializing the Configuration, either directly from the INI file,
     * or indirectly via a database.
     * @return A status code to be returned to the shell.
     */
    int _configInitFile() const;

    /**
     * Update parameters of a worker
     * @return A status code to be returned to the shell.
     */
    int _updateWorker() const;

    /**
     * Add a new worker
     * @return A status code to be returned to the shell.
     */
    int _addWorker() const;

    /**
     * Delete an existing worker and all metadata associated with it
     * @return A status code to be returned to the shell.
     */
    int _deleteWorker() const;

    /**
     * Add a new database family
     * @return A status code to be returned to the shell.
     */
    int _addFamily();

    /**
     * Delete an existing database family
     * @return A status code to be returned to the shell.
     */
    int _deleteFamily();

    /**
     * Add a new database
     * @return A status code to be returned to the shell.
     */
    int _addDatabase();

    /**
     * Publish an existing database
     * @param publish Set 'true' if the database is being published. Set 'false' otherwise.
     * @return A status code to be returned to the shell.
     */
    int _publishDatabase(bool publish);

    /**
     * Delete an existing database
     * @return A status code to be returned to the shell.
     */
    int _deleteDatabase();

    /**
     * Add a new table
     * @return A status code to be returned to the shell.
     */
    int _addTable();

    /**
     * Delete an existing table
     * @return A status code to be returned to the shell.
     */
    int _deleteTable();

    /// The command
    std::string _command;

    /// An optional scope of the command "DUMP"
    std::string _dumpScope;

    /// Format of an initialization file
    std::string _format;

    /// Parameters of a worker to be updated
    ConfigWorker _worker;

    /// The flag for enabling a select worker. The default value of -1 or any
    /// negative number) means that the flag wasn't used. A value of 0 means - disable
    /// the worker, and a value of 1 (or any positive number) means - enable the worker.
    int _workerEnable = -1;

    /// The flag for turning a worker into the read-only mode. The default value
    /// of -1 (or any negative number) means that the flag wasn't used. A value
    /// of 0 means - turn the worker into the read-only state, and a value
    /// of 1 (or any positive number) means - enable the read-write mode for the worker .
    int _workerReadOnly = -1;

    /// For database families
    DatabaseFamilyInfo _family;

    /// For databases
    DatabaseInfo _database;

    /// For tables
    TableInfo _table;

    std::string _directorDatabaseTable;
    std::string _directorDatabaseTable2;
    std::string _primaryKeyColumn;
    std::string _primaryKeyColumn2;

    bool _nonUniquePrimaryKey = false;  ///< @see TableInfo::uniquePrimaryKey
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONFIGAPP_H */
