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
#ifndef LSST_QSERV_HTTPMODULE_H
#define LSST_QSERV_HTTPMODULE_H

// System headers
#include <memory>
#include <stdexcept>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "qhttp/Server.h"
#include "replica/EventLogger.h"
#include "replica/HttpProcessorConfig.h"
#include "replica/HttpRequestBody.h"
#include "replica/HttpRequestQuery.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpModule is a base class for requests processing modules
 * of an HTTP server built into the Master Replication Controller.
 */
class HttpModule:
        public EventLogger,
        public std::enable_shared_from_this<HttpModule> {
public:
    typedef std::shared_ptr<HttpModule> Ptr;

    /// The enumeration type which is used for configuring/enforcing
    /// module's authorization requirements.
    enum AuthType {
        AUTH_REQUIRED,
        AUTH_NONE
    };

    /**
     * Class AuthError represent exceptions thrown when the authorization
     * requirements aren't met.
     */
    class AuthError: public std::invalid_argument {
    public:
        using std::invalid_argument::invalid_argument;
    };

    HttpModule() = delete;
    HttpModule(HttpModule const&) = delete;
    HttpModule& operator=(HttpModule const&) = delete;

    virtual ~HttpModule() = default;

    /**
     * Invokes a subclass-specific request processing provided by implementations
     * of the pure virtual method HttpModule::executeImpl(). The current method
     * would also do an optional processing of exceptions thrown by the subclass-specific
     * implementations of method HttpModule::executeImpl(). These error conditions will
     * be reported to as errors to callers.
     * 
     * @param req  the HTTP request
     * @param resp  the HTTP response channel
     * @param subModuleName  this optional parameter allows modules to have
     *   multiple sub-modules. A value of this parameter will be forwarded to
     *   the subclass-specific implementation of the pure virtual method
     *   HttpModule::executeImpl().
     * @param authType  Authorization requirements of the module. If AUTH_REQUIRED is
     *   requested then the method will enforce the authorization.  would result in a error
     *   sent back to a client.
     */
    void execute(qhttp::Request::Ptr const& req,
                 qhttp::Response::Ptr const& resp,
                 std::string const& subModuleName=std::string(),
                 AuthType const authType=AUTH_NONE);

protected:
    HttpModule(Controller::Ptr const& controller,
               std::string const& taskName,
               HttpProcessorConfig const& processorConfig);

    unsigned int workerResponseTimeoutSec() const { return _processorConfig.workerResponseTimeoutSec; }
    unsigned int qservSyncTimeoutSec() const { return _processorConfig.qservSyncTimeoutSec; }
    unsigned int workerReconfigTimeoutSec() const { return _processorConfig.workerReconfigTimeoutSec; }

    std::string context() const;

    qhttp::Request::Ptr const& req() const { return _req; }
    qhttp::Response::Ptr const& resp() const { return _resp; }

    HttpRequestQuery const& query() const { return _query; }
    HttpRequestBody const& body() const { return _body; }

    // Message loggers for the corresponding log levels

    void info(std::string const& msg) const;
    void info(std::string const& context, std::string const& msg) const { info(context + "  " + msg); }

    void debug(std::string const& msg) const;
    void debug(std::string const& context, std::string const& msg) const { debug(context + "  " + msg); }

    void error(std::string const& msg) const;
    void error(std::string const& context, std::string const& msg) const { error(context + "  " + msg); }

    /**
     * Report a error condition and send a error message back to a requester
     * of a service.
     *
     * @param resp the HTTP response channel
     * @param func the name of a context from which the operation was initiated
     * @param errorMsg error condition to be reported
     */
    void sendError(std::string const& func,
                   std::string const& errorMsg) const;

    /**
     * Report a result back to a requester of a service upon its successful
     * completion.
     *
     * @param resp    the HTTP response channel
     * @param result  JSON object to be sent back
     * @param success (optional) flag indicating if the operation was successful.
     *                Note, that the method will still send a result regardless of
     *                a value of the flag. The result object may provide more specific
     *                info on a reason of a failure (if not success)
     */
    void sendData(nlohmann::json& result,
                  bool success=true);

    /**
     * To implement a subclass-specific request processing.
     * 
     * @note all exceptions thrown by the implementations will be intercepted and
     * reported as errors to callers.
     */
    virtual void executeImpl(std::string const& subModuleName) = 0 ;

private:
    /**
     * Inspect the body of a request or a presence of a user-supplied authorization key.
     * Its value will be compared against a value of the corresponding configuration
     * parameter of the service (processorConfig) passed into the constructor of the class.
     * In the absence of the message body, or in the absence of the key in the body, or
     * in case of any mismatch between the keys would result in an exception thrown.
     *
     * @throw AuthError  This exception is thrown if the authorization requirements weren't met.
     */
    void _enforceAuthorization() const;

    // Input parameters

    HttpProcessorConfig const _processorConfig;

    // Pointers to the request and response are set when a module is called for execution.

    qhttp::Request::Ptr _req;
    qhttp::Response::Ptr _resp;

    /// The parser for parameters passed into the Web services via the optional
    /// query part of a URL. The object gets initialized when a module is called
    /// for execution.
    HttpRequestQuery _query;

    /// The body of a request is initialized when a module is called for execution.
    HttpRequestBody _body;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPMODULE_H
