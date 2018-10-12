/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_APPLICATION_H
#define LSST_QSERV_REPLICA_APPLICATION_H

// System headers
#include <string>

// Qserv headers
#include "replica/ApplicationTypes.h"
#include "replica/ServiceProvider.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class Application is a utility base class for building command-line
  * tools. The class is meant to take care of mudane tasks such as handling
  * command-line parameters, initializing application environment, etc.
  */
class Application {

public:

    /// To bring the Parser type into the class's scope
    using Parser = detail::Parser;

    /**
     * Construct and initialize an application.
     *
     * @param arc
     *   argument count
     * 
     * @parav argv
     *   vector of argument values
     *
     * @param description
     *   (optional) descripton of an application as it will appear in
     *   the documentation string reported with option "--help"
     *
     * @param injectHelpOption
     *   (optional) flag to inject option "--help"
     *
     * @param injectDatabaseOptions
     *   (optional) flag which will inject database options and use an input
     *   from a user to change the corresponding defaults in the Configuration
     *
     * @param boostProtobufVersionCheck
     *   (optional) flag which will force Google Protobuf version check. The check
     *   will ensure that a version of the Protobuf library linked to an application
     *   is consistent with hedear files.
     *
     * @param enableServiceProvider
     *   (optional) flag which will inject configuration option "--config=<url>",
     *   load the configuration into Configuration and initialize the ServiceProvider
     *   with the configuration.
     */
    Application(int argc,
                const char* const argv[],
                std::string const& description = "",
                bool const injectHelpOption = true,
                bool const injectDatabaseOptions = true,
                bool const boostProtobufVersionCheck = false,
                bool const enableServiceProvider = false);

    // Default construction and copy semantics are prohibited

    Application() = delete;
    Application(Application const&) = delete;
    Application& operator=(Application const&) = delete;

    virtual ~Application() = default;

    /**
     * Parse command line parameters, initialize the application's context
     * and run a user-supplied algorithm. A completion code obtained from
     * this method is supposed to be returned to a shell. These are some of
     * the predefined values returned by the method as defined by type
     * Parser::Status. Other values are determied by the user-supplied implementaton
     * of virtual method 'runImpl'.
     * 
     * @see Parser::Status
     * @see Application::runImpl()
     *
     * @return completion code
     */
    int run();

protected:

    /// @return reference to the parser
    Parser& parser() { return _parser; }

    /**
     * @return reference to the ServiceProvider object
     *
     * @throws std::logic_error
     *   if Configuration loading and ServiceProvider is not enabled
     *   in the constructor of teh class.
     */
    ServiceProvider::Ptr const& serviceProvider() const;

    /**
     * This method is requird to be implements by subclasses to run
     * the application's logic. The method is called after successfully
     * parsing the command-line paramters and initializing the aplication's
     * context.
     *
     * @see method Application::run()
     * 
     * @eturn completion code
     */
    virtual int runImpl() = 0;
    
private:

    /// For parsing command-line parameters, options and flags
    Parser _parser;

    /// The flag indicating if database options need to be captured and
    /// forwarded to the Configuration.
    bool const _injectDatabaseOptions;

    /// The flag indicating if Google Protobuf version check is forced
    bool const _boostProtobufVersionCheck;


    /// The flag indicating if the Configuration object and ServiceProvider
    /// have to be setup.
    bool const _enableServiceProvider;
    
    /// Configuration URL
    std::string _config;
    
    /// The provider of the Configuration and other services
    ServiceProvider::Ptr _serviceProvider;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_APPLICATION_H
