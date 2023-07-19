/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "qhttp/Response.h"

// System headers
#include <map>
#include <memory>
#include <string>
#include <sstream>
#include <string>
#include <utility>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/filesystem.hpp"
#include "boost/filesystem/fstream.hpp"

// Local headers
#include "lsst/log/Log.h"
#include "qhttp/LogHelpers.h"

namespace asio = boost::asio;
namespace ip = boost::asio::ip;
namespace fs = boost::filesystem;
using namespace lsst::qserv::qhttp;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qhttp");

std::map<Status, const std::string> responseStringsByCode = {
        {STATUS_CONTINUE, "Continue"},
        {STATUS_SWITCH_PROTOCOL, "Switching Protocols"},
        {STATUS_PROCESSING, "Processing"},
        {STATUS_OK, "OK"},
        {STATUS_CREATED, "Created"},
        {STATUS_ACCEPTED, "Accepted"},
        {STATUS_NON_AUTHORATIVE_INFO, "Non-Authoritative Information"},
        {STATUS_NO_CONTENT, "No Content"},
        {STATUS_RESET_CONTENT, "Reset Content"},
        {STATUS_PARTIAL_CONTENT, "Partial Content"},
        {STATUS_MULTI_STATUS, "Multi-Status"},
        {STATUS_ALREADY_REPORTED, "Already Reported"},
        {STATUS_IM_USED, "IM Used"},
        {STATUS_MULTIPLE_CHOICES, "Multiple Choices"},
        {STATUS_MOVED_PERM, "Moved Permanently"},
        {STATUS_FOUND, "Found"},
        {STATUS_SEE_OTHER, "See Other"},
        {STATUS_NOT_MODIFIED, "Not Modified"},
        {STATUS_USE_PROXY, "Use Proxy"},
        {STATUS_TEMP_REDIRECT, "Temporary Redirect"},
        {STATUS_PERM_REDIRECT, "Permanent Redirect"},
        {STATUS_BAD_REQ, "Bad Request"},
        {STATUS_UNAUTHORIZED, "Unauthorized"},
        {STATUS_PAYMENT_REQUIRED, "Payment Required"},
        {STATUS_FORBIDDEN, "Forbidden"},
        {STATUS_NOT_FOUND, "Not Found"},
        {STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed"},
        {STATUS_NON_ACCEPTABLE, "Not Acceptable"},
        {STATUS_PROXY_AUTH_REQUIRED, "Proxy Authentication Required"},
        {STATUS_REQ_TIMEOUT, "Request Timeout"},
        {STATUS_CONFLICT, "Conflict"},
        {STATUS_GONE, "Gone"},
        {STATUS_LENGTH_REQUIRED, "Length Required"},
        {STATUS_PRECOND_FAILED, "Precondition Failed"},
        {STATUS_PAYLOAD_TOO_LARGE, "Payload Too Large"},
        {STATUS_URI_TOO_LONG, "URI Too Long"},
        {STATUS_UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type"},
        {STATUS_RANGE_NOT_SATISFIABLE, "Range Not Satisfiable"},
        {STATUS_FAILED_EXPECT, "Expectation Failed"},
        {STATUS_MISREDIRECT_REQ, "Misdirected Request"},
        {STATUS_UNPROCESSIBLE, "Unprocessable Entity"},
        {STATUS_LOCKED, "Locked"},
        {STATUS_FAILED_DEP, "Failed Dependency"},
        {STATUS_UPGRADE_REQUIRED, "Upgrade Required"},
        {STATUS_PRECOND_REQUIRED, "Precondition Required"},
        {STATUS_TOO_MANY_REQS, "Too Many Requests"},
        {STATUS_REQ_HDR_FIELDS_TOO_LARGE, "Request Header Fields Too Large"},
        {STATUS_INTERNAL_SERVER_ERR, "Internal Server Error"},
        {STATUS_NOT_IMPL, "Not Implemented"},
        {STATUS_BAD_GATEWAY, "Bad Gateway"},
        {STATUS_SERVICE_UNAVAIL, "Service Unavailable"},
        {STATUS_GSATEWAY_TIMEOUT, "Gateway Timeout"},
        {STATUS_UNDSUPPORT_VERSION, "HTTP Version Not Supported"},
        {STATUS_VARIANT_NEGOTIATES, "Variant Also Negotiates"},
        {STATUS_NO_STORAGE, "Insufficient Storage"},
        {STATUS_LOOP, "Loop Detected"},
        {STATUS_NOT_EXTENDED, "Not Extended"},
        {STATUS_NET_AUTH_REQUIRED, "Network Authentication Required"}};

std::unordered_map<std::string, const std::string> contentTypesByExtension = {
        {".css", "text/css"},   {".gif", "image/gif"},  {".htm", "text/html"},
        {".html", "text/html"}, {".jpg", "image/jpeg"}, {".js", "application/javascript"},
        {".png", "image/png"},
};

}  // namespace

namespace lsst::qserv::qhttp {

Response::Response(std::shared_ptr<Server> const server, std::shared_ptr<ip::tcp::socket> const socket,
                   DoneCallback const& doneCallback)
        : _server(std::move(server)), _socket(std::move(socket)), _doneCallback(doneCallback) {
    _transmissionStarted.clear();
}

void Response::sendStatus(Status status) {
    this->status = status;
    std::string statusStr = responseStringsByCode[status];
    std::ostringstream entStr;
    entStr << "<html>" << std::endl;
    entStr << "<head><title>" << status << " " << statusStr << "</title></head>" << std::endl;
    entStr << "<body style=\"background-color:#E6E6FA\">" << std::endl;
    entStr << "<h1>" << status << " " << statusStr << "</h1>" << std::endl;
    entStr << "</body>" << std::endl;
    entStr << "</html>" << std::endl;
    send(entStr.str());
}

void Response::send(std::string const& content, std::string const& contentType) {
    headers["Content-Type"] = contentType;
    headers["Content-Length"] = std::to_string(content.length());

    std::ostream responseStream(&_responsebuf);
    responseStream << _headers() << "\r\n" << content;
    _write();
}

void Response::sendFile(fs::path const& path) {
    auto ct = contentTypesByExtension.find(path.extension().string());
    headers["Content-Type"] = (ct != contentTypesByExtension.end()) ? ct->second : "text/plain";
    headers["Content-Length"] = std::to_string(fs::file_size(path));

    // Try to open the file for streaming input. Throw if we hit a snag; exception expected to be caught by
    // top-level handler in Server::_dispatchRequest().
    fs::ifstream responseFile(path);
    if (!responseFile) {
        LOGLS_ERROR(_log, logger(_server) << logger(_socket) << "open failed for " << path << ": "
                                          << std::strerror(errno));
        throw(boost::system::system_error(errno, boost::system::generic_category()));
    }

    std::ostream responseStream(&_responsebuf);
    responseStream << _headers() << "\r\n" << responseFile.rdbuf();
    _write();
}

std::string Response::_headers() const {
    std::ostringstream headerst;
    headerst << "HTTP/1.1 ";

    auto r = responseStringsByCode.find(status);
    if (r == responseStringsByCode.end()) r = responseStringsByCode.find(STATUS_INTERNAL_SERVER_ERR);
    headerst << r->first << " " << r->second;

    auto ilength = headers.find("Content-Length");
    std::size_t length = (ilength == headers.end()) ? 0 : std::stoull(ilength->second);
    LOGLS_INFO(_log, logger(_server) << logger(_socket) << headerst.str() << " + " << length << " bytes");

    headerst << "\r\n";
    for (auto const& h : headers) {
        headerst << h.first << ": " << h.second << "\r\n";
    }

    return headerst.str();
}

void Response::_write() {
    if (_transmissionStarted.test_and_set()) {
        LOGLS_ERROR(_log, logger(_server)
                                  << logger(_socket) << "handler logic error: multiple responses sent");
        return;
    }

    auto self = shared_from_this();
    asio::async_write(*_socket, _responsebuf, [self](boost::system::error_code const& ec, std::size_t sent) {
        if (ec) {
            LOGLS_ERROR(_log, logger(self->_server)
                                      << logger(self->_socket) << "write failed: " << ec.message());
        }
        if (self->_doneCallback) {
            self->_doneCallback(ec, sent);
        }
    });
}

}  // namespace lsst::qserv::qhttp
