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
#ifndef LSST_QSERV_REPLICA_APPLICATION_H
#define LSST_QSERV_REPLICA_APPLICATION_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/ApplicationTypes.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class Application is a utility base class for building command-line
  * tools. The class is meant to take care of mundane tasks such as handling
  * command-line parameters, initializing application environment, etc.
  */
class Application : public std::enable_shared_from_this<Application> {
public:

    /// To bring the Parser type into the class's scope
    using Parser = detail::Parser;

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
     * Parser::Status. Other values are determined by the user-supplied
     * implementation of virtual method 'runImpl'.
     * 
     * @see Parser::Status
     * @see Application::runImpl()
     *
     * @return a completion code
     */
    int run();

protected:
    /**
     * Construct and initialize an application.
     *
     * @param arc  An argument count
     * @parav argv  A vector of argument values
     * @param description  An optional description of an application as it will appear
     *   in the documentation string reported with option "--help".
     * @param injectDatabaseOptions An optional flag which will inject database options
     *   and use an input from a user to change the corresponding defaults in the Configuration.
     * @param boostProtobufVersionCheck  An optional flag which will force Google Protobuf
     *   version check. The check will ensure that a version of the Protobuf library linked
     *   to an application is consistent with header files.
     * @param enableServiceProvider An optional flag which will inject configuration
     *   option "--config=<url>", load the configuration into Configuration and initialize
     *   the ServiceProvider with the configuration.
     */
    Application(int argc,
                const char* const argv[],
                std::string const& description="",
                bool const injectDatabaseOptions=true,
                bool const boostProtobufVersionCheck=false,
                bool const enableServiceProvider=false);

    /// @return a shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /// @return reference to the parser
    Parser& parser() { return _parser; }

    /**
     * @return reference to the ServiceProvider object
     *
     * @throws std::logic_error
     *   if Configuration loading and ServiceProvider is not enabled
     *   in the constructor of the class.
     */
    ServiceProvider::Ptr const& serviceProvider() const;

    /**
     * This method is required to be implements by subclasses to run
     * the application's logic. The method is called after successfully
     * parsing the command-line parameters and initializing the application's
     * context.
     *
     * @see method Application::run()
     * 
     * @eturn a completion code
     */
    virtual int runImpl() = 0;
    
private:
    // Input parameters

    bool const _injectDatabaseOptions;
    bool const _boostProtobufVersionCheck;
    bool const _enableServiceProvider;

    /// For parsing command-line parameters, options and flags
    Parser _parser;

    /// The standard flag which would turn on the debug output if requested
    bool _debugFlag;

    /// Configuration URL
    std::string _config;

    /// A unique identifier of a Qserv instance served by the Replication System
    std::string _instanceId;

    // Database connector options (if enabled)

    unsigned int _databaseAllowReconnect;
    unsigned int _databaseConnectTimeoutSec;
    unsigned int _databaseMaxReconnects;
    unsigned int _databaseTransactionTimeoutSec;

    /// The provider of the Configuration and other services
    ServiceProvider::Ptr _serviceProvider;

    /// Message logger
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_APPLICATION_H
