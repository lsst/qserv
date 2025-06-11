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
#ifndef LSST_QSERV_HTTP_FILEUPLOADMODULE_H
#define LSST_QSERV_HTTP_FILEUPLOADMODULE_H

// System headers
#include <string>
#include <unordered_map>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/BaseModule.h"

// Forward declarations

namespace httplib {
class ContentReader;
class Request;
class Response;
}  // namespace httplib

namespace lsst::qserv::http {
class RequestQuery;
}  // namespace lsst::qserv::http

// This header declarations
namespace lsst::qserv::http {

/**
 * Class FileUploadModule is an extended base class specialized for constructing
 * the CPP-HTTPLIB file uploading/processing modules. The uploading is expected
 * to be done in a streaming mode. The class is abstract and is expected to be subclassed
 * to implement the actual file uploading/processing logic.
 *
 * The class defines the following protocol allowing to handle 0 or many files:
 * @code
 *   onStartOfFile  \
 *     onFileData    \
 *     ..             * <file-1>
 *     onFileData    /
 *   onEndOfFile    /
 *
 *   onStartOfFile  \
 *     onFileData    \
 *     ..             * <file-2>
 *     onFileData    /
 *   onEndOfFile    /
 *
 *   ..
 *
 *   onEndOfBody
 * @endcode
 * The call of the onEndOfBody() method is expected to prepare the JSON object
 * to be returned to the client. This is the only method that is guaranteed to be called
 * once for each request, even if no files were sent in the request.
 *
 * @note Note a role of the parameter "subModuleName". The parameter is used to specify
 *  a name of a sub-module to be executed. It's up to the subclass to interpret the parameter
 *  and to decide what to do with it.
 */
class FileUploadModule : public BaseModule {
public:
    FileUploadModule() = delete;
    FileUploadModule(FileUploadModule const&) = delete;
    FileUploadModule& operator=(FileUploadModule const&) = delete;

    virtual ~FileUploadModule() = default;
    virtual void execute(std::string const& subModuleName = std::string(),
                         http::AuthType const authType = http::AuthType::NONE);

protected:
    /**
     * @param authContext  An authorization context for operations which require extra security.
     * @param req  The HTTP request.
     * @param resp  The HTTP response channel.
     */
    FileUploadModule(http::AuthContext const& authContext, httplib::Request const& req,
                     httplib::Response& resp, httplib::ContentReader const& contentReader);

    httplib::Request const& req() { return _req; }
    httplib::Response& resp() { return _resp; }
    std::string const& subModuleName() const { return _subModuleName; }

    // These methods implemented the BaseModule's pure virtual methods.

    virtual std::string method() const;
    virtual std::unordered_map<std::string, std::string> params() const;
    virtual RequestQuery query() const;
    virtual std::string headerEntry(std::string const& key) const;
    virtual void sendResponse(std::string const& content, std::string const& contentType);

    // The following methods are required to be implemented by the subclasses
    // to handle the file uploading. The methods are expected to throw exceptions
    // for any problem encountered while evaluating a context of a request, or if
    // the corresponidng operations couldn't be accomplished.

    /**
     * Is called when a file is found in the requst.
     * @param name  The name of a parameter assocated with the file.
     * @param fileName  The name of the file to be opened.
     * @param contentType  The content type of the file.
     */
    virtual void onStartOfFile(std::string const& name, std::string const& fileName,
                               std::string const& contentType) = 0;

    /**
     * Is called when the next portion of the file data is available. The method may
     * be called 0 or multiple times for a single file while the data is being uploaded.
     * @param data  The data of the file.
     * @param length  The length of the data.
     */
    virtual void onFileData(char const* data, std::size_t length) = 0;

    /**
     * Is called when the file parsing is finished.
     */
    virtual void onEndOfFile() = 0;

    /**
     * Is called when the body parsing is finished. This is the last call of the
     * file uploading protocol.
     * @return The JSON object to be sent back to the client.
     */
    virtual nlohmann::json onEndOfBody() = 0;

private:
    // Input parameters
    httplib::Request const& _req;
    httplib::Response& _resp;
    httplib::ContentReader const& _contentReader;

    std::string _subModuleName;  ///< The name of the sub-module to be executed.
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_FILEUPLOADMODULE_H
