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
#include "replica/apps/Application.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/config/ConfigParserMySQL.h"
#include "replica/config/ConfigurationSchema.h"
#include "replica/proto/protocol.pb.h"
#include "util/Issue.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Application");
}

namespace lsst::qserv::replica {

Application::Application(int argc, const char* const argv[], string const& description,
                         bool const injectDatabaseOptions, bool const boostProtobufVersionCheck,
                         bool const enableServiceProvider)
        : _injectDatabaseOptions(injectDatabaseOptions),
          _boostProtobufVersionCheck(boostProtobufVersionCheck),
          _enableServiceProvider(enableServiceProvider),
          _parser(argc, argv, description),
          _debugFlag(false),
          _config("mysql://qsreplica@localhost:3306/qservReplica"),
          _databaseAllowReconnect(Configuration::databaseAllowReconnect() ? 1 : 0),
          _databaseConnectTimeoutSec(Configuration::databaseConnectTimeoutSec()),
          _databaseMaxReconnects(Configuration::databaseMaxReconnects()),
          _databaseTransactionTimeoutSec(Configuration::databaseTransactionTimeoutSec()),
          _schemaUpgradeWait(Configuration::schemaUpgradeWait() ? 1 : 0),
          _schemaUpgradeWaitTimeoutSec(Configuration::schemaUpgradeWaitTimeoutSec()) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    if (_boostProtobufVersionCheck) {
        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }
}

int Application::run() {
    // Add extra options to the parser configuration
    parser().option("instance-id",
                    " A unique identifier of a Qserv instance served by the Replication System."
                    " Its value will be passed along various internal communication lines of"
                    " the system to ensure that all services are related to the same instance."
                    " This mechanism also prevents 'cross-talks' between two (or many) Replication"
                    " System's setups in case of an accidental mis-configuration.",
                    _instanceId)
            .option("http-user", "The login name of a user for connecting to the Replication service.",
                    _httpAuthContext.user)
            .option("http-password",
                    "The login password of a user for connecting to the Replication service. The value of "
                    "the password"
                    " will be ignored if the user is not specified. The password will be used for"
                    " authenticating the user. The password can't be empty if the user is specified.",
                    _httpAuthContext.password)
            .option("auth-key",
                    "An authorization key for operations affecting the state of Qserv or"
                    " the Replication/Ingest system.",
                    _httpAuthContext.authKey)
            .option("admin-auth-key",
                    "An administrator-level authorization key for critical operations affecting"
                    " the state of Qserv of the Replication/Ingest system.",
                    _httpAuthContext.adminAuthKey)
            .flag("debug",
                  "Change the minimum logging level from ERROR to DEBUG. Note that the Logger"
                  " is configured via a configuration file (if any) presented to the application via"
                  " environment variable LSST_LOG_CONFIG. If this variable is not set then some"
                  " default configuration of the Logger will be assumed.",
                  _debugFlag);
    if (_injectDatabaseOptions) {
        parser().option("db-allow-reconnect",
                        "Change the default database connection handling node. Set 0 to disable"
                        " automatic reconnects. Any other number would allow reconnects.",
                        _databaseAllowReconnect)
                .option("db-reconnect-timeout",
                        "Change the default value limiting a duration of time for making automatic"
                        " reconnects to a database server before failing and reporting error"
                        " (if the server is not up, or if it's not reachable for some reason)",
                        _databaseConnectTimeoutSec)
                .option("db-max-reconnects",
                        "Change the default value limiting a number of attempts to repeat a sequence"
                        " of queries due to connection losses and subsequent reconnects before to fail.",
                        _databaseMaxReconnects)
                .option("db-transaction-timeout",
                        "Change the default value limiting a duration of each attempt to execute"
                        " a database transaction before to fail.",
                        _databaseTransactionTimeoutSec)
                .option("schema-upgrade-wait",
                        "If the value of the option is 0 and the schema version of the Replication/Ingest "
                        "system's"
                        " database is either not available or is less than " +
                                to_string(ConfigParserMySQL::expectedSchemaVersion) +
                                " then the application will fail right away. Otherwise, the application will "
                                "keep"
                                " tracking schema version for a duration specified by the option "
                                "--schema-upgrade-wait-timeout."
                                " Note that if the schema version found in the database is higher than the "
                                "expected one"
                                " then the application will fail right away regardless of a value of either "
                                "options.",
                        _schemaUpgradeWait)
                .option("schema-upgrade-wait-timeout",
                        "This option specifies a duration of time to wait for the schema upgrade in case"
                        " if this feature is enabled in the option --schema-upgrade-wait.",
                        _schemaUpgradeWaitTimeoutSec);
    }
    if (_enableServiceProvider) {
        parser().option("config", "Configuration URL (a database connection string).", _config);
        // Inject options for th egeneral configuration parameters.
        for (auto&& itr : ConfigurationSchema::parameters()) {
            string const& category = itr.first;
            for (auto&& param : itr.second) {
                // The read-only parameters can't be updated programmatically.
                if (ConfigurationSchema::readOnly(category, param)) continue;
                _generalParams[category][param] = ConfigurationSchema::defaultValueAsString(category, param);
                parser().option(category + "-" + param, ConfigurationSchema::description(category, param),
                                _generalParams[category][param]);
            }
        }
    }
    try {
        int const code = parser().parse();
        if (Parser::SUCCESS != code) return code;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             "Application::" + string(__func__) + "  command-line parser error: " << ex.what());
        return Parser::PARSING_FAILED;
    }

    // Change the default logging level if requested
    if (not _debugFlag) {
        LOG_CONFIG_PROP(
                "log4j.rootLogger=INFO, CONSOLE\n"
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n"
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n"
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{yyyy-MM-ddTHH:mm:ss.SSSZ}  LWP %-5X{LWP} "
                "%-5p  %m%n\n"
                "log4j.logger.lsst.qserv=INFO");
    }

    // Change default parameters of the database connectors
    if (_injectDatabaseOptions) {
        Configuration::setDatabaseAllowReconnect(_databaseAllowReconnect != 0);
        Configuration::setDatabaseConnectTimeoutSec(_databaseConnectTimeoutSec);
        Configuration::setDatabaseMaxReconnects(_databaseMaxReconnects);
        Configuration::setDatabaseTransactionTimeoutSec(_databaseTransactionTimeoutSec);
        Configuration::setSchemaUpgradeWait(_schemaUpgradeWait != 0);
        Configuration::setSchemaUpgradeWaitTimeoutSec(_schemaUpgradeWaitTimeoutSec);
    }
    if (_enableServiceProvider) {
        _serviceProvider = ServiceProvider::create(_config, _instanceId, _httpAuthContext);

        // Update general configuration parameters.
        // Note that options specified by a user will have non-empty values.
        for (auto&& categoryItr : _generalParams) {
            string const& category = categoryItr.first;
            for (auto&& paramItr : categoryItr.second) {
                string const& param = paramItr.first;
                string const& value = paramItr.second;
                if (!value.empty()) {
                    _serviceProvider->config()->setFromString(category, param, value);
                }
            }
        }

        // Start the provider in its own thread pool before performing any asynchronous
        // operations via BOOST ASIO.
        // Note that onFinish callbacks which are activated upon the completion of
        // the asynchronous activities will be run by a thread from the pool.
        _serviceProvider->run();
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
    _assertValidOption(__func__, _enableServiceProvider, " service provider options");
    return _serviceProvider;
}

string const& Application::configUrl() const {
    _assertValidOption(__func__, _enableServiceProvider, " service provider options");
    return _config;
}

void Application::_assertValidOption(string const& func, bool option, string const& context) const {
    string const context_ = "Application::" + func + " ";
    if (_parser.status() != Parser::SUCCESS) {
        throw logic_error(context_ +
                          "calling this method isn't allowed before invoking the command-line parser.");
    }
    if (!option) {
        throw logic_error(context_ + "this application was not configured with " + context + ".");
    }
}

}  // namespace lsst::qserv::replica
