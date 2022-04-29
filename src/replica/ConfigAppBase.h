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
#ifndef LSST_QSERV_REPLICA_CONFIGAPPBASE_H
#define LSST_QSERV_REPLICA_CONFIGAPPBASE_H

// System headers
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/Configuration.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class ConfigAppBase is a base class for applications working with
 * the configuration service of the system.
 */
class ConfigAppBase : public Application {
public:
    ConfigAppBase() = delete;
    ConfigAppBase(ConfigAppBase const&) = delete;
    ConfigAppBase& operator=(ConfigAppBase const&) = delete;

    ~ConfigAppBase() override = default;

protected:
    ConfigAppBase(int argc, char* argv[], std::string const& description);

    std::string const& configUrl() const { return _configUrl; }
    Configuration::Ptr const& config() const { return _config; }
    bool verticalSeparator() const { return _verticalSeparator; }

    /**
     * Setup the configuration and trigger subclass-specific processing
     * deined in method 'runSubclassImpl'.
     * @see ConfigAppBase::runSubclassImpl()
     * @see Application::runImpl()
     */
    virtual int runImpl() final;

    /// Implement subclass-specific processing of a user request.
    virtual int runSubclassImpl() = 0;

    /**
     * Dump the Configuration into the standard output stream
     * @return A status code to be returned to the shell.
     */
    void dumpWorkersAsTable(std::string const& indent, std::string const& caption = "WORKERS:") const;
    void dumpFamiliesAsTable(std::string const& indent,
                             std::string const& caption = "DATABASE FAMILIES:") const;
    void dumpDatabasesAsTable(std::string const& indent, std::string const& caption = "DATABASES:") const;

private:
    std::string _configUrl;
    Configuration::Ptr _config;
    bool _verticalSeparator = false;  ///< print vertical separator in tables
};

}}}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_CONFIGAPPBASE_H */
