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
#include "qhttp/Request.h"

// Third-party headers
#include "boost/regex.hpp"

namespace ip = boost::asio::ip;

namespace lsst {
namespace qserv {
namespace qhttp {

Request::Request(std::shared_ptr<ip::tcp::socket> socket)
:
    content(&_requestbuf),
    _socket(socket)
{
    localAddr = _socket->local_endpoint();
}


void Request::_parseHeader()
{
    std::string line;
    static boost::regex reqRe{R"(^([^ \r]+) ([^ \r]+) ([^ \r]+)\r$)"}; // e.g. "method target version"
    boost::smatch reqMatch;
    if (getline(content, line) && boost::regex_match(line, reqMatch, reqRe)) {
        method = reqMatch[1];
        target = reqMatch[2];
        version = reqMatch[3];
        static boost::regex headerRe{R"(^([^:\r]+): ?([^\r]*)\r$)"}; // e.g. "header: value"
        boost::smatch headerMatch;
        while(getline(content, line) && boost::regex_match(line, headerMatch, headerRe)) {
            header[headerMatch[1]] = headerMatch[2];
        }
    }
}


void Request::_parseUri()
{
    static boost::regex targetRe{R"(([^\?#]*)(?:\?([^#]*))?)"}; // e.g. "path[?query]"
    boost::smatch targetMatch;
    if (boost::regex_match(target, targetMatch, targetRe)) {
        path = targetMatch[1];
        std::string squery = targetMatch[2];
        static boost::regex queryRe{R"(([^=&]+)(?:=([^&]*))?)"}; // e.g. "key[=value]"
        auto end = boost::sregex_iterator{};
        for(auto i=boost::make_regex_iterator(squery, queryRe); i!=end; ++i) {
            query[(*i)[1]] = (*i)[2];
        }
    }
}


void Request::_parseBody()
{
    // TODO: implement application/x-www-form-urlencoded body -> body
}

}}}  // namespace lsst::qserv::qhttp
