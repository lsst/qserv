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
#include "replica/Application.h"

// System headers
#include <iostream>
#include <sstream>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "util/Issue.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Application");

}   // namespace

namespace lsst {
namespace qserv {
namespace replica {
namespace detail {

ParserError::ParserError(util::Issue::Context const& ctx,
                         std::string const& message)
    :   util::Issue(ctx, "ParserError: " + message) {
}

Parser::Parser(int argc,
               const char* const argv[],
               std::string const& description,
               bool injectHelpOption)
    :   _argc(argc),
        _argv(argv),
        _description(description),
        _injectHelpOption(injectHelpOption),
        _code(Status::UNDEFINED) {
}

void Parser::reset() {
    _allArguments.clear();
    _required.clear();
    _optional.clear();
    _options.clear();
    _flags.clear();
    _code = Status::UNDEFINED;
    _usage = "";
    _help = "";
}

Parser& Parser::flag(std::string const& name,
                     std::string const& description,
                     bool& var) {

    registerArgument(name);

    _flags.emplace(
        std::make_pair(
            name,
            std::move(
                std::make_unique<FlagParser>(
                    name,
                    description,
                    var
                )
            )
        )
    );
    return *this;
}

int Parser::parse() {

    // Check if the parser hasn't been used.
    if (Status::UNDEFINED != _code) return _code;

    try {

        // Inject standard options if requested

        bool helpFlag = false;
        if (_injectHelpOption) {
            flag("help",
                 "print this 'help'",
                 helpFlag);
        }

        // Split input arguments into the following 3 categories, assuming
        // the following syntax:
        //
        //   options:   --<option>=[<value>]
        //   flag:      --<flag>
        //   parameter: <value>

        std::map<std::string, std::string> options;    
        std::set<std::string> flags;
        std::vector<std::string> parameters;

        for (int i = 1; i < _argc; ++i) {

            std::string const arg = _argv[i];

            // Positional parameter?
            if ("--" != arg.substr(0, 2)) {
                parameters.push_back(arg);
                continue;
            }

            // An option with a value?
            std::string const nameVal = arg.substr(2);
            if (nameVal.empty()) {
                throw ParserError(ERR_LOC, "standalone '--' can't be used as a flag");
            }
            std::string::size_type const pos = nameVal.find('=');
            if (std::string::npos != pos) {

                std::string const name = nameVal.substr(0, pos);
                if (0 == _allArguments.count(name)) {
                    throw ParserError(ERR_LOC, "unknown option: '" + name+ "'");
                }
                options[name] = nameVal.substr(pos+1);
                continue;
            }

            // Then, it's a flag
            if (0 == _allArguments.count(nameVal)) {
                throw ParserError(ERR_LOC, "unknown option: '" + nameVal+ "'");
            }
            flags.insert(nameVal);
        }
 
        // Parse values of options
        for (auto&& entry: options) {
            auto const& name  = entry.first;
            auto const& value = entry.second;
            if (0 == _options.count(name)) {
                throw ParserError(ERR_LOC, "'" + name+ "' is not an option");
            }
            _options[name]->parse(value);
        }
        
        // Parse flags
        for (auto&& name: flags) {
            if (0 == _flags.count(name)) {
                throw ParserError(ERR_LOC, "'" + name+ "' is not a flag");
            }
            _flags[name]->parse();
        }

        // Intercept and respond to '--help' if found before parsing
        // positional parameters

        if (helpFlag) {
            std::cerr << help() << std::endl;
            _code = Status::HELP_REQUESTED;
        } else {

            // Verify that the number and a category (mandatory and optional)
            // of the positional parameters match expectations

            if (parameters.size() > _required.size() + _optional.size()) {
                throw ParserError(ERR_LOC, "too many positional parameters");
            }
            if (parameters.size() < _required.size()) {
                throw ParserError(ERR_LOC, "insufficient number of positional parameters");
            }

            // Then parse values of parameters
            
            for (size_t inIdx = 0, outIdx = 0;
                 inIdx < _required.size();
                 ++inIdx, ++outIdx) {
                _required[outIdx]->parse(parameters[inIdx]);
            }
            for (size_t inIdx = _required.size(), outIdx = 0;
                 inIdx < parameters.size();
                 ++inIdx, ++outIdx) {
                _optional[outIdx]->parse(parameters[inIdx]);
            }
            _code = Status::SUCCESS;
        }

    } catch (ParserError const& ex) {
        std::cerr << ex.what() << "\n" << usage() << std::endl;
        _code = Status::PARSING_FAILED;
    }
    return _code;
}

void Parser::registerArgument(std::string const& name) {

    if (name.empty()) {
        throw std::invalid_argument(
                "empty string passed where argument name was expected");
    }
    if (0 != _allArguments.count(name)) {
        throw std::invalid_argument(
                "argument '" + name + "' was already registered");
    }
    _allArguments.insert(name);
}

std::string const& Parser::usage() {

    std::string const indent = "  ";

    if (_usage.empty()) {
        _usage = "USAGE:\n";
        if (not (_required.empty() and _optional.empty())) {
            _usage += "\n" + indent;
            for (auto&& arg: _required) {
                _usage += "<" + arg->name() + "> ";
            }
            for (auto&& arg: _optional) {
                _usage += "[<" + arg->name() + ">] ";
            }
        }
        if (not _options.empty()) {
            for (auto&& arg: _options) {
                _usage += "\n" + indent + "--" + arg.first + "=[<value>]";
            }
        }
        if (not _flags.empty()) {
            for (auto&& arg: _flags) {
                _usage += "\n" + indent + "--" + arg.first;
            }
        }
        _usage += "\n";
    }
    return _usage;
}

std::string const& Parser::help() {

    if (_help.empty()) {

        _help += "DESCRIPTION:\n\n" + wrap(_description, "  ") + "\n\n" + usage();

        _help += "\nPARAMETERS:\n";
        for (auto&& arg: _required) {
            _help += "\n  <" + arg->name() + ">\n" + wrap(arg->description()) + "\n";
        }
        for (auto&& arg: _optional) {
            _help += "\n  <" + arg->name() + ">\n" + wrap(arg->description()) + "\n";
            _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }

        _help += "\nOPTIONS:\n";
        for (auto&& entry: _options) {
            auto&& arg = entry.second;
            _help += "\n  --" + arg->name() + "\n" + wrap(arg->description()) + "\n";
            _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }

        _help += "\nFLAGS:\n";
        for (auto&& entry: _flags) {
            auto&& arg = entry.second;
            _help += "\n  --" + arg->name() + "\n" + wrap(arg->description()) + "\n";
        }
    }
    return _help;
}

std::string Parser::wrap(std::string const& str,
                         std::string const& indent,
                         size_t width) {
    
    std::ostringstream os;
    size_t lineLength = 0;

    std::istringstream is(str);
    std::string word;

    while (is >> word) {

        if (0 == lineLength) {
            
            // Just starting the very first line.

            os << indent;
            lineLength = indent.size();

        } else if (lineLength +  word.size() + 1 > width) {

            // Wrap the current line if its total length would exceed
            // the allowed limit after adding the word.

            os << "\n" << indent;
            lineLength = indent.size();
        }
        os << word << " ";
        lineLength += word.size() + 1;
    }
    return os.str();
}

} // namespace detail

Application::Application(int argc,
                         const char* const argv[],
                         std::string const& description,
                         bool const injectHelpOption,
                         bool const injectDatabaseOptions,
                         bool const boostProtobufVersionCheck,
                         bool const enableServiceProvider)
    :    _parser(
            argc,
            argv,
            description,
            injectHelpOption
        ),
        _injectDatabaseOptions(injectDatabaseOptions),
        _boostProtobufVersionCheck(boostProtobufVersionCheck),
        _enableServiceProvider(enableServiceProvider),
        _config("file:replication.cfg") {
}

int Application::run() {

    // Add extra options to the parser's configuration

    bool debugFlag = false;
    parser().flag(
        "debug",
        "Change the logging lover of the internal LSST level to DEBUG. Note that the Logger"
        " is configured via a configuration file presented to the application via"
        " environment variable LSST_LOG_CONFIG. If this variable is not set then some"
        " default configuation of the Logger will be assumed.",
        debugFlag
    );

    unsigned int databaseAllowReconnect        = replica::Configuration::databaseAllowReconnect() ? 1 : 0;
    unsigned int databaseConnectTimeoutSec     = replica::Configuration::databaseConnectTimeoutSec();
    unsigned int databaseMaxReconnects         = replica::Configuration::databaseMaxReconnects();
    unsigned int databaseTransactionTimeoutSec = replica::Configuration::databaseTransactionTimeoutSec();

    if (_injectDatabaseOptions) {
        parser().option(
            "db-allow-reconnect",
            "Change the default database connecton handling node. Set 0 to disable"
            " automati reconnects. Any other number would allow reconnects.",
            databaseAllowReconnect
        ).option(
            "db-reconnect-timeout",
            "Change the default value limiting a duration of time for making automatic"
            " reconnects to a database server before failing and reporting error"
            " (if the server is not up, or if it's not reachable for some reason)",
            databaseConnectTimeoutSec
        ).option(
            "db-max-reconnects",
            "Change the default value limiting a number of attempts to repeat a sequence"
            " of queries due to connection losses and subsequent reconnects before to fail.",
            databaseMaxReconnects
        ).option(
            "db-transaction-timeout",
            "Change the default value limiting a duration of each attempt to execute"
            " a database transaction before to fail.",
            databaseTransactionTimeoutSec
        );
    }
    if (_enableServiceProvider) {
        parser().option(
            "config",
            "Configuration URL (a configuration file or a set of database connection parameters).",
            _config
        );
    }
    
    int const code = parser().parse();
    if (Parser::SUCCESS != code) return code;

    // Change the default logging level if requested

    if (not debugFlag) {
        LOG_CONFIG_PROP("log4j.rootLogger=ERROR");
    }

    // Change default parameters of the database connectors
    if (_injectDatabaseOptions) {

        Configuration::setDatabaseAllowReconnect(0 != databaseAllowReconnect);
        Configuration::setDatabaseConnectTimeoutSec(databaseConnectTimeoutSec);
        Configuration::setDatabaseMaxReconnects(databaseMaxReconnects);
        Configuration::setDatabaseTransactionTimeoutSec(databaseTransactionTimeoutSec);
    }
    if (_enableServiceProvider) {
        _serviceProvider = ServiceProvider::create(_config);
    }

    // Finally, let the user's code to do the job
    return runImpl();
}

ServiceProvider::Ptr const& Application::serviceProvider() const {
    if (nullptr == _serviceProvider) {
        throw std::logic_error(
                "Application::serviceProvider()  this application was not configured to enable this");
    }
    return _serviceProvider;
}

}}} // namespace lsst::qserv::replica
