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

#ifndef LSST_QSERV_QHTTP_REQUEST_H
#define LSST_QSERV_QHTTP_REQUEST_H

// System headers
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"

// Local headers
#include "util/CIUtils.h"

namespace lsst::qserv::qhttp {

class Server;
class Response;

class Request : public std::enable_shared_from_this<Request> {
public:
    using Ptr = std::shared_ptr<Request>;
    using BodyReadCallback =
            std::function<void(std::shared_ptr<Request>, std::shared_ptr<Response>, bool, std::size_t)>;

    // ----- The default size of the body read requests in stream mode (when
    //       calling  method readPartialBodyAsync()).

    static std::size_t const defaultRecordSizeBytes;

    //----- The local address on which this request was accepted

    boost::asio::ip::tcp::endpoint localAddr;

    //----- The remote address from which this request was initiated

    boost::asio::ip::tcp::endpoint remoteAddr;

    //----- Elements of the HTTP header for this request

    std::string method;   // HTTP header method
    std::string target;   // HTTP header target
    std::string version;  // HTTP header version

    //----- Parsed query elements and headers for this request.  Note that parsed HTTP headers and
    //      URL parameters are stored in simple std::maps, so repeated headers or parameters are not
    //      supported (last parsed for any given header or parameter wins).  Headers are stored in a
    //      case-insensitive map, in accordance with HTTP standards.

    std::string path;                                    // path portion of URL
    std::unordered_map<std::string, std::string> query;  // parsed URL query parameters
    std::unordered_map<std::string, std::string, util::ci_hash, util::ci_pred> header;  // parsed HTTP headers
    std::unordered_map<std::string, std::string> params;  // captured URL path elements

    //----- Body content for this request

    std::istream content;  // unparsed body

    //----- Read the body of the request asynchronously.  The onFinished callback will be called
    //      with a boolean argument that is true if the body was read successfully.
    //      The length of the body is always available to the request handler via contentLengthBytes().
    //      The actual number of bytes read so far is available via contentReadBytes(). This parameter
    //      is set if the body is read automatically or manually by calling method readEntireBodyAsync().
    //      Any differene between contentLengthBytes() and contentReadBytes() indicates that the body
    //      is not fully read yet.

    void readEntireBodyAsync(BodyReadCallback onFinished);

    std::size_t contentLengthBytes() const { return _contentLengthBytes; }
    std::size_t contentReadBytes() const { return _contentReadBytes; }

    //----- Read the next chunk of the body of the request asynchronously.  The onFinished callback
    //      will be called with a boolean argument that is true if the body was read successfully.
    //      The caller is responsible for calling this method repeatedly until the body is fully read
    //      which is indicated by comparing contentLengthBytes() and contentReadBytes().
    //      The number of bytes read in each chunk is determined by the value of the optional
    //      parameter numBytes. If the value is 0, the size of the chunk is determined by the value
    //      of the recordSizeBytes() method.

    void readPartialBodyAsync(BodyReadCallback onFinished, std::size_t numBytes = 0);

    //----- Get/set the size of the body read requests in stream mode.
    //      The default value of 0 will reset the current setting to the implementation default.

    std::size_t recordSizeBytes() const { return _recordSizeBytes; }
    void setRecordSize(std::size_t bytes = 0) {
        _recordSizeBytes = bytes == 0 ? defaultRecordSizeBytes : bytes;
    }

private:
    friend class Server;

    Request(Request const&) = delete;
    Request& operator=(Request const&) = delete;

    explicit Request(std::shared_ptr<Server> const server, std::shared_ptr<Response> const response,
                     std::shared_ptr<boost::asio::ip::tcp::socket> const socket);

    bool _parseHeader(std::size_t headerSizeBytes);
    bool _parseUri();

    std::string _percentDecode(std::string const& encoded, bool exceptPathDelimeters, bool& hasNULs);

    //----- Read the specified number of bytes of the request's body asynchronously. The onFinished
    //      callback will be called with a boolean argument that is true if the body was read successfully.

    void _readBodyAsync(std::shared_ptr<boost::asio::steady_timer> timer_, std::size_t bytesToRead,
                        BodyReadCallback onFinished);

    std::shared_ptr<Server> const _server;
    std::shared_ptr<Response> const _response;
    std::shared_ptr<boost::asio::ip::tcp::socket> const _socket;
    boost::asio::streambuf _requestbuf;

    // ----- The current size of the body read requests in stream mode (when calling method
    //       readPartialBodyAsync()). The size can be changed by calling method setRecordSize().

    std::size_t _recordSizeBytes = defaultRecordSizeBytes;

    // ----- Content-Length header value, if present is set by _parseHeader() and used to
    //       read the request's body.

    std::size_t _contentLengthBytes = 0;

    // ----- The number of content bytes read so far. It is used to determine when the body
    //       has been fully read and to prevent reading beyond the end of the body.
    //       The value is set by _parseHeader() and considerd when reading the request's body.

    std::size_t _contentReadBytes = 0;
};

}  // namespace lsst::qserv::qhttp

#endif  // LSST_QSERV_QHTTP_REQUEST_H
