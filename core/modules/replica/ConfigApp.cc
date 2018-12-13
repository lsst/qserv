/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/ConfigApp.h"

// Qserv headers
#include "replica/Configuration.h"

namespace {

std::string const description {
    "This application is the Master Replication Controller which has"
    " a built-in Cluster Health Monitor and a linear Replication loop."
    " The Monitor would track a status of both Qserv and Replication workers"
    " and trigger the worker exclusion sequence if both services were found"
    " non-responsive within a configured interval."
    " The interval is specified via the corresponding command-line option."
    " And it also has some built-in default value."
    " Also, note that only a single node failure can trigger the worker"
    " exclusion sequence."
    " The controller has the built-in REST API which accepts external commands"
    " or request for information."
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ConfigApp::Ptr ConfigApp::create(int argc,
                                 const char* const argv[]) {
    return Ptr(
        new ConfigApp(
            argc,
            argv
        )
    );
}


ConfigApp::ConfigApp(int argc,
                     const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true  /* injectDatabaseOptions */,
            false /* boostProtobufVersionCheck */,
            false /* enableServiceProvider */
        ),
        _config("file:replication.cfg"),
        _log(LOG_GET("lsst.qserv.replica.ConfigApp")) {

    // Configure the command line parser

    parser().option(
        "config",
        "Configuration URL (a configuration file or a set of database connection parameters).",
        _config
    );
}


int ConfigApp::runImpl() {

    char const* context = "CONFIGURATION-APP  ";

    LOGS(_log, LOG_LVL_INFO, context << parser().serializeArguments());

    auto const config = Configuration::load(_config);
    if (config->prefix() != "mysql") {
        LOGS(_log, LOG_LVL_ERROR, context << "file-base configuration is not allowed by this application");
        return 1;
    }
    
    return 0;
}

}}} // namespace lsst::qserv::replica
