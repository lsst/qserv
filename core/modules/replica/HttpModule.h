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
#include <stdexcept>
#include <string>
#include <unordered_map>

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
class HttpModule: public EventLogger {
public:
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
     * @param subModuleName  this optional parameter allows modules to have
     *   multiple sub-modules. A value of this parameter will be forwarded to
     *   the subclass-specific implementation of the pure virtual method
     *   HttpModule::executeImpl().
     * @param authType  Authorization requirements of the module. If AUTH_REQUIRED is
     *   requested then the method will enforce the authorization.  would result in a error
     *   sent back to a client.
     */
    void execute(std::string const& subModuleName=std::string(),
                 AuthType const authType=AUTH_NONE);

protected:
    /**
     * 
     * @param controller       The Controller provides the network I/O services (BOOST ASIO)
     * @param taskName         The name of a task in a context of the Master Replication Controller
     * @param processorConfig  Shared parameters of the HTTP services
     * @param req              The HTTP request
     * @param resp             The HTTP response channel
     */
    HttpModule(Controller::Ptr const& controller,
               std::string const& taskName,
               HttpProcessorConfig const& processorConfig,
               qhttp::Request::Ptr const& req,
               qhttp::Response::Ptr const& resp);

    unsigned int workerResponseTimeoutSec() const { return _processorConfig.workerResponseTimeoutSec; }
    unsigned int qservSyncTimeoutSec() const { return _processorConfig.qservSyncTimeoutSec; }
    unsigned int workerReconfigTimeoutSec() const { return _processorConfig.workerReconfigTimeoutSec; }

    std::string context() const;

    qhttp::Request::Ptr const& req() const { return _req; }
    qhttp::Response::Ptr const& resp() const { return _resp; }

    std::unordered_map<std::string, std::string> const& params() const { return _req->params; }

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
     * To implement a subclass-specific request processing.
     * 
     * @note All exceptions thrown by the implementations will be intercepted and
     *   reported as errors to callers. Exceptions are now the only way to report
     *   errors from modules.
     * @return A result to be sent back to a service requester in case of a successful
     *   completion of the requested operation.
     * @throws HttpExceptions In case if a module needs to pass extra details
     *   on a error back to a service requester. 
     */
    virtual nlohmann::json executeImpl(std::string const& subModuleName) = 0 ;

private:

    /**
     * Inspect the body of a request or a presence of a user-supplied authorization key.
     * Its value will be compared against a value of the corresponding configuration
     * parameter of the service (processorConfig) passed into the constructor of the class.
     * In the absence of the message body, or in the absence of the key in the body, or
     * in case of any mismatch between the keys would result in an exception thrown.
     *
     * @throw AuthError This exception is thrown if the authorization requirements weren't met.
     */
    void _enforceAuthorization() const;

    /**
     * Report a error condition and send an error message back to a requester
     * of a service.
     *
     * @param func The name of a context from which the operation was initiated.
     * @param errorMsg An error condition to be reported.
     * @param errorExt (optional) The additional information on the error.
     */
    void _sendError(std::string const& func,
                    std::string const& errorMsg,
                    nlohmann::json const& errorExt=nlohmann::json::object()) const;

    /**
     * Report a result back to a requester of a service upon its successful
     * completion.
     * @param result A JSON object to be sent back.
     */
    void _sendData(nlohmann::json& result);

    // Input parameters

    HttpProcessorConfig const _processorConfig;

    qhttp::Request::Ptr const _req;
    qhttp::Response::Ptr const _resp;

    /// The parser for parameters passed into the Web services via the optional
    /// query part of a URL. The object gets initialized from the request.
    HttpRequestQuery const _query;

    /// The body of a request is initialized/parsed from the request before calling
    /// the overloaded method HttpModule::executeImpl.
    HttpRequestBody _body;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPMODULE_H
