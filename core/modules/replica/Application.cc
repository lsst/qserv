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
#include "replica/Application.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/protocol.pb.h"
#include "util/Issue.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Application");
}

namespace lsst {
namespace qserv {
namespace replica {

Application::Application(int argc,
                         const char* const argv[],
                         string const& description,
                         bool const injectDatabaseOptions,
                         bool const boostProtobufVersionCheck,
                         bool const enableServiceProvider,
                         bool const injectXrootdOptions)
    :   _injectDatabaseOptions    (injectDatabaseOptions),
        _boostProtobufVersionCheck(boostProtobufVersionCheck),
        _enableServiceProvider    (enableServiceProvider),
        _injectXrootdOptions      (injectXrootdOptions),
        _parser   (argc,argv, description),
        _debugFlag(false),
        _config   ("mysql://qsreplica@localhost:3306/qservReplica"),
        _databaseAllowReconnect       (Configuration::databaseAllowReconnect() ? 1 : 0),
        _databaseConnectTimeoutSec    (Configuration::databaseConnectTimeoutSec()),
        _databaseMaxReconnects        (Configuration::databaseMaxReconnects()),
        _databaseTransactionTimeoutSec(Configuration::databaseTransactionTimeoutSec()),
        _xrootdAllowReconnect         (Configuration::xrootdAllowReconnect() ? 1 : 0),
        _xrootdConnectTimeoutSec      (Configuration::xrootdConnectTimeoutSec()) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    if (_boostProtobufVersionCheck) {
        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }
}


int Application::run() {

    // Add extra options to the parser configuration
    parser().flag(
        "debug",
        "Change the minimum logging level from ERROR to DEBUG. Note that the Logger"
        " is configured via a configuration file (if any) presented to the application via"
        " environment variable LSST_LOG_CONFIG. If this variable is not set then some"
        " default configuration of the Logger will be assumed.",
        _debugFlag
    );
    if (_injectDatabaseOptions) {
        parser().option(
            "db-allow-reconnect",
            "Change the default database connection handling node. Set 0 to disable"
            " automatic reconnects. Any other number would allow reconnects.",
            _databaseAllowReconnect
        ).option(
            "db-reconnect-timeout",
            "Change the default value limiting a duration of time for making automatic"
            " reconnects to a database server before failing and reporting error"
            " (if the server is not up, or if it's not reachable for some reason)",
            _databaseConnectTimeoutSec
        ).option(
            "db-max-reconnects",
            "Change the default value limiting a number of attempts to repeat a sequence"
            " of queries due to connection losses and subsequent reconnects before to fail.",
            _databaseMaxReconnects
        ).option(
            "db-transaction-timeout",
            "Change the default value limiting a duration of each attempt to execute"
            " a database transaction before to fail.",
            _databaseTransactionTimeoutSec
        );
    }
    if (_enableServiceProvider) {
        parser().option(
            "config",
            "Configuration URL (a database connection string).",
            _config
        ).option(
            "instance-id",
            " A unique identifier of a Qserv instance served by the Replication System."
            " Its value will be passed along various internal communication lines of"
            " the system to ensure that all services are related to the same instance."
            " This mechanism also prevents 'cross-talks' between two (or many) Replication"
            " System's setups in case of an accidental mis-configuration.",
            _instanceId
        );
    }
    if (_injectXrootdOptions) {
        parser().option(
            "xrootd-allow-reconnect",
            "Change the default XROOTD connection handling node. Set 0 to disable"
            " automatic reconnects. Any other number would allow reconnects.",
            _xrootdAllowReconnect
        ).option(
            "xrootd-reconnect-timeout",
            "Change the default value limiting a duration of time for making automatic"
            " reconnects to the XROOTD servers before failing and reporting error"
            " (if the server is not up, or if it's not reachable for some reason)",
            _xrootdConnectTimeoutSec
        );
    }
    try {
        int const code = parser().parse();
        if (Parser::SUCCESS != code) return code;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "Application::" + string(__func__) + "  command-line parser error: " << ex.what());
        return Parser::PARSING_FAILED;
    }

    // Change the default logging level if requested
    if (not _debugFlag) {
        LOG_CONFIG_PROP(
            "log4j.rootLogger=INFO, CONSOLE\n"
            "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n"
            "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n"
            "log4j.appender.CONSOLE.layout.ConversionPattern=%d{yyyy-MM-ddTHH:mm:ss.SSSZ}  LWP %-5X{LWP} %-5p  %m%n\n"
            "log4j.logger.lsst.qserv=INFO"
        );
    }

    // Change default parameters of the database connectors
    if (_injectDatabaseOptions) {
        Configuration::setDatabaseAllowReconnect(_databaseAllowReconnect != 0);
        Configuration::setDatabaseConnectTimeoutSec(_databaseConnectTimeoutSec);
        Configuration::setDatabaseMaxReconnects(_databaseMaxReconnects);
        Configuration::setDatabaseTransactionTimeoutSec(_databaseTransactionTimeoutSec);
    }
    if (_enableServiceProvider) {

        // Create and then start the provider in its own thread pool before
        // performing any asynchronous operations via BOOST ASIO.
        //
        // Note that onFinish callbacks which are activated upon the completion of
        // the asynchronous activities will be run by a thread from the pool.
        _serviceProvider = ServiceProvider::create(_config, _instanceId);
        _serviceProvider->run();
    }
    // Change default parameters of the XROOTD connectors
    if (_injectXrootdOptions) {
        Configuration::setXrootdAllowReconnect(_xrootdAllowReconnect != 0);
        Configuration::setXrootdConnectTimeoutSec(_xrootdConnectTimeoutSec);
    }

    // Let the user's code to do its job
    int const exitCode = runImpl();

    // Shutdown the provider and join with its threads
    if (_enableServiceProvider) {
        _serviceProvider->stop();
    }
    return exitCode;
}


ServiceProvider::Ptr const& Application::serviceProvider() const {
    if (nullptr == _serviceProvider) {
        throw logic_error(
                "Application::" + string(__func__) + "  this application was not configured to initialize"
                " the ServiceProvider.");
    }
    if (_serviceProvider == nullptr) {
        throw logic_error(
            "Application::" + string(__func__) + "  calling this method isn't allowed before invoking"
            " the command-line parser.");
    }
    return _serviceProvider;
}


string const& Application::configUrl() const {
    if (!_enableServiceProvider) {
        throw logic_error(
            "Application::" + string(__func__) + "  this application was not configured to initialize"
            " the ServiceProvider.");
    }
    if (_serviceProvider == nullptr) {
        throw logic_error(
            "Application::" + string(__func__) + "  calling this method isn't allowed before invoking"
            " the command-line parser.");
    }
    return _config;
}


}}} // namespace lsst::qserv::replica
