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

// System headers
#include <cstdlib>

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
        path = _percentDecode(targetMatch[1], true);
        std::string squery = targetMatch[2];
        static boost::regex queryRe{R"(([^=&]+)(?:=([^&]*))?)"}; // e.g. "key[=value]"
        auto end = boost::sregex_iterator{};
        for(auto i=boost::make_regex_iterator(squery, queryRe); i!=end; ++i) {
            query[_percentDecode((*i)[1])] = _percentDecode((*i)[2]);
        }
    }
}


void Request::_parseBody()
{
    // TODO: implement application/x-www-form-urlencoded body -> body
}


std::string Request::_percentDecode(std::string const& encoded, bool exceptPathDelimeters)
{
    std::string decoded;

    static boost::regex codepointRe(R"(%[0-7][0-7a-fA-F])");
    auto pbegin = boost::sregex_iterator(encoded.begin(), encoded.end(), codepointRe);
    auto pend = boost::sregex_iterator();

    std::string::const_iterator tail = encoded.cbegin();

    for(boost::sregex_iterator i=pbegin; i!=pend; ++i) {
        decoded.append(i->prefix().first, i->prefix().second);
        tail = i->suffix().first;
        char codepoint = strtol(i->str().c_str()+1, NULL, 16);

        // If decoding a path, leave any encoded slashes encoded (but ensure lower case), so they don't
        // become confused with path-element-delimiting slashes. We elsewhere make sure that intra-element
        // slashes within the matchers are lower-case percent-encoded as well (see Path.cc).
        if (exceptPathDelimeters && (codepoint == '/')) {
            decoded.append("%2f");
        }

        // Otherwise, decode.
        else {
            decoded.push_back(codepoint);
        }

    }

    decoded.append(tail, encoded.cend());

    return decoded;
}

}}}  // namespace lsst::qserv::qhttp
