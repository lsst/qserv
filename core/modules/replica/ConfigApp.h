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

// System headers
#include <limits>
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/Configuration.h"
#include "replica/ConfigurationTypes.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ConfigApp implements a tool for inspecting/modifying configuration
 * records stored in the MySQL/MariaDB database.
 */
class ConfigApp : public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ConfigApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    // Default construction and copy semantics are prohibited

    ConfigApp() = delete;
    ConfigApp(ConfigApp const&) = delete;
    ConfigApp& operator=(ConfigApp const&) = delete;

    ~ConfigApp() override = default;

protected:

    /**
     * @see ConfigApp::create()
     */
    ConfigApp(int argc, char* argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /**
     * Dump the Configuration into the standard output stream
     * 
     * @return
     *   a status code to be returned to the shell
     */
    int _dump() const;

    /**
     * Dump general configuration parameters into the standard output
     * stream as a table.
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpGeneralAsTable(std::string const& indent) const;

    /**
     * Dump workers into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpWorkersAsTable(std::string const& indent) const;

    /**
     * Dump database families into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpFamiliesAsTable(std::string const& indent) const;

    /**
     * Dump databases into the standard output stream as a table 
     * 
     * @param indent
     *   the indentation for the table
     */
    void _dumpDatabasesAsTable(std::string const& indent) const;

    /**
     * Dump the Configuration into the standard output stream in  format which could
     * be used for initializing the Configuration, either directly from the INI file,
     * or indirectly via a database.
     * 
     * @return
     *   a status code to be returned to the shell
     */
    int _configInitFile() const;

    /**
     * Update the general configuration parameters
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _updateGeneral();

    /**
     * Update parameters of a worker
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _updateWorker() const;

    /**
     * Add a new worker
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addWorker() const;

    /**
     * Delete an existing worker and all metadata associated with it
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteWorker() const;

    /**
     * Add a new database family
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addFamily();

    /**
     * Delete an existing database family
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteFamily();

    /**
     * Add a new database
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addDatabase();

    /**
     * Delete an existing database
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteDatabase();

    /**
     * Add a new table
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _addTable();

    /**
     * Delete an existing table
     *
     * @return
     *   a status code to be returned to the shell
     */
    int _deleteTable();

private:

    /// Logger stream
    LOG_LOGGER _log;
    
    /// The input Configuration
    Configuration::Ptr _config;

    /// The command
    std::string _command;

    /// Configuration URL
    std::string _configUrl;

    /// An optional scope of the command "DUMP"
    std::string _dumpScope;

    /// Show the actual database password when dumping the Configuration
    bool _dumpDbShowPassword{false};

    /// Print vertical separator in tables
    bool _verticalSeparator = false;

    /// Format of an initialization file
    std::string _format;

    /// Parameters of a worker to be updated
    WorkerInfo _workerInfo;

    /// The flag for enabling a select worker
    bool _workerEnable;

    /// The flag for disabling a select worker
    bool _workerDisable;

    /// The flag for turning a worker into the read-only mode
    bool _workerReadOnly;

    /// The flag for turning a worker into the read-write mode
    bool _workerReadWrite;

    /// General parameters
    ConfigurationGeneralParams _general;

    /// For database families
    DatabaseFamilyInfo _familyInfo;

    /// For databases
    DatabaseInfo _databaseInfo;

    /// The name of a database"
    std::string _database;

    /// The name of a table"
    std::string _table;

    /// 'false' for the regular tables, 'true' for the partitioned ones
    bool _isPartitioned = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONFIGAPP_H */
