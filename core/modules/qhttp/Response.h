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
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/filesystem.hpp"

namespace lsst {
namespace qserv {
namespace qhttp {

class Response : public std::enable_shared_from_this<Response>
{
public:

    using Ptr = std::shared_ptr<Response>;

    //----- These methods are used to send a response back to the HTTP client.  When using sendStatus,
    //      an appropriate HTML body will be automatically generated.

    void send(std::string const& content, std::string const& contentType="text/html");
    void sendStatus(unsigned int status);
    void sendFile(boost::filesystem::path const& path);

    //----- Response status code and additional headers may also be set with these members, and will be
    //      included/observed by the send methods above (sendStatus and sendFile will override status set
    //      here, though.)

    unsigned int status = { 200 };
    std::unordered_map<std::string, std::string> headers;

private:

    friend class Server;

    Response(Response const&) = delete;
    Response& operator=(Response const&) = delete;

    using DoneCallback = std::function<void(
        boost::system::error_code const& ec,
        std::size_t bytesTransferred
    )>;

    Response(
        std::shared_ptr<boost::asio::ip::tcp::socket> socket,
        DoneCallback const& doneCallback
    );

    std::string _headers() const;

    std::shared_ptr<boost::asio::ip::tcp::socket> _socket;
    boost::asio::streambuf _responsebuf;

    DoneCallback _doneCallback;

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSERV_QHTTP_RESPONSE_H
