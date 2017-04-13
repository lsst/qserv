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
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"

// Local headers
#include "qhttp/CIUtils.h"

namespace lsst {
namespace qserv {
namespace qhttp {

class Request : public std::enable_shared_from_this<Request>
{
public:

    using Ptr = std::shared_ptr<Request>;

    boost::asio::ip::tcp::endpoint localAddr;

    std::string method;   // HTTP header method
    std::string target;   // HTTP header target
    std::string version;  // HTTP header version

    std::string path;                                                      // path portion of URL
    std::unordered_map<std::string, std::string> query;                    // parsed URL query parameters
    std::unordered_map<std::string, std::string, ci_hash, ci_pred> header; // parsed HTTP headers
    std::unordered_map<std::string, std::string> params;                   // captured URL path elements

    std::istream content;                                  // unparsed body
    std::unordered_map<std::string, std::string> body;     // parsed body, if x-www-form-urlencoded

private:

    friend class Server;

    Request(Request const&) = delete;
    Request& operator=(Request const&) = delete;

    explicit Request(std::shared_ptr<boost::asio::ip::tcp::socket> socket);

    void _parseHeader();
    void _parseUri();
    void _parseBody();

    std::shared_ptr<boost::asio::ip::tcp::socket> _socket;
    boost::asio::streambuf _requestbuf;

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSERV_QHTTP_REQUEST_H
