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

#ifndef LSST_QSERV_QHTTP_RESPONSE_H
#define LSST_QSERV_QHTTP_RESPONSE_H

// System headers
#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/filesystem.hpp"

// Local headers
#include "qhttp/Status.h"

namespace lsst::qserv::qhttp {

class Server;

class Response : public std::enable_shared_from_this<Response> {
public:
    using Ptr = std::shared_ptr<Response>;

    //----- These methods are used to send a response back to the HTTP client.  When using sendStatus,
    //      an appropriate HTML body will be automatically generated.  When using sendFile, contentType
    //      will be inferred based on file extension for a handful of known file types (see map defined
    //      near the top of Response.cc for specific extensions supported.)

    void send(std::string const& content, std::string const& contentType = "text/html");
    void sendStatus(Status status);
    void sendFile(boost::filesystem::path const& path);

    //----- Response status code and additional headers may also be set with these members, and will be
    //      included/observed by the send methods above (sendStatus and sendFile will override status set
    //      here, though; sendFile will override any Content-Type header set here.)

    Status status = STATUS_OK;
    std::unordered_map<std::string, std::string> headers;

private:
    friend class Server;

    Response(Response const&) = delete;
    Response& operator=(Response const&) = delete;

    using DoneCallback =
            std::function<void(boost::system::error_code const& ec, std::size_t bytesTransferred)>;

    Response(std::shared_ptr<Server> const server, std::shared_ptr<boost::asio::ip::tcp::socket> const socket,
             DoneCallback const& doneCallback, std::size_t const maxResponseBufSize);

    std::string _headers() const;

    //----- Mark a start of transmission. Return 'false' if multiple transmission attempts
    //      were detected. Duplicate transmission requests will be ignored. An error message
    //      may be logged. The ongoing transmission won't be affected.

    bool _startTransmit();
    void _finishTransmit(boost::system::error_code const& ec, std::size_t sent);
    void _sendFileRecord(std::string::size_type pos, std::size_t size);

    std::shared_ptr<Server> const _server;

    std::shared_ptr<boost::asio::ip::tcp::socket> const _socket;
    std::string _responseBuf;
    std::atomic_flag _transmissionStarted;

    std::string _fileName;
    std::ifstream _inFile;
    std::size_t _bytesRemaining = 0;  // initialized with the file size
    std::size_t _bytesSent = 0;       // including the header

    DoneCallback _doneCallback;
    std::size_t const _maxResponseBufSize;
};

}  // namespace lsst::qserv::qhttp

#endif  // LSST_QSERV_QHTTP_RESPONSE_H
