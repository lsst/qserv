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
#ifndef LSST_QSERV_REPLICA_CONFIGTESTAPP_H
#define LSST_QSERV_REPLICA_CONFIGTESTAPP_H

// System headers
#include <string>

// Qserv headers
#include "replica/apps/ConfigAppBase.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ConfigTestApp implements an application for testing the configuration
 * API against the MySQL/MariaDB database.
 */
class ConfigTestApp : public ConfigAppBase {
public:
    typedef std::shared_ptr<ConfigTestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    ConfigTestApp() = delete;
    ConfigTestApp(ConfigTestApp const&) = delete;
    ConfigTestApp& operator=(ConfigTestApp const&) = delete;

    ~ConfigTestApp() override = default;

protected:
    /// @see ConfigAppBase::runSubclassImpl()
    virtual int runSubclassImpl() final;

private:
    /// @see ConfigTestApp::create()
    ConfigTestApp(int argc, char* argv[]);

    /**
     * The complete integration test for the Configuration service.
     */
    int _test();
    bool _testWorkers();
    bool _testDatabasesAndFamilies();
    bool _testTables();

    /// An optional scope for the test
    std::string _testScope = "ALL";
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONFIGTESTAPP_H */
