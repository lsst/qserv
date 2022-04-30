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
#include <utility>

// Third-party headers
#include "boost/regex.hpp"

// Local headers
#include "lsst/log/Log.h"
#include "qhttp/LogHelpers.h"

namespace ip = boost::asio::ip;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qhttp");
}

namespace lsst::qserv::qhttp {

Request::Request(std::shared_ptr<Server> const server, std::shared_ptr<ip::tcp::socket> const socket)
        : content(&_requestbuf), _server(std::move(server)), _socket(std::move(socket)) {
    boost::system::error_code ignore;
    localAddr = _socket->local_endpoint(ignore);
    remoteAddr = _socket->remote_endpoint(ignore);
}

bool Request::_parseHeader() {
    std::string line;
    if (!getline(content, line)) return false;

    // Regexp to parse request line into "method target version".  Allowed character classes here are
    // extracted from ABNF in RFC7230 and RFC3986.

    static boost::regex reqRe{R"(^([!#$%&'*+-.^_`|~[:digit:][:alpha:]]+) )"
                              R"(([-._-~%!$&'()*+,;=:@?/[:digit:][:alpha:]]+) )"
                              R"((HTTP/[[:digit:]]\.[[:digit:]])\r$)"};

    boost::smatch reqMatch;
    if (!boost::regex_match(line, reqMatch, reqRe)) {
        LOGLS_WARN(_log, logger(_server) << logger(_socket) << "bad start line: " << ctrlquote(line));
        return false;
    }

    method = reqMatch[1];
    target = reqMatch[2];
    version = reqMatch[3];

    // Regexp to parse header field lines into "header: value".  Allowed character classes here are
    // extracted from ABNF in RFC7230.

    static boost::regex headerRe{R"(^([!#$%&'*+-.^_`|~[:digit:][:alpha:]]+))"
                                 R"(:[ \t]*)"
                                 R"(([\x20-\x7e \t]*?))"
                                 R"([ \t]*\r$)"};

    boost::smatch headerMatch;

    while (getline(content, line)) {
        if (line == "\r") break;  // empty line signals end of headers
        if (!boost::regex_match(line, headerMatch, headerRe)) {
            LOGLS_WARN(_log, logger(_server) << logger(_socket) << "bad header: " << ctrlquote(line));
            return false;
        }
        header[headerMatch[1]] = headerMatch[2];
    }

    return true;
}

bool Request::_parseUri() {
    static boost::regex targetRe{R"(([^\?#]*)(?:\?([^#]*))?)"};  // e.g. "path[?query]"

    boost::smatch targetMatch;
    if (!boost::regex_match(target, targetMatch, targetRe)) return false;

    bool hasNULs = false;
    path = _percentDecode(targetMatch[1], true, hasNULs);
    if (hasNULs) {
        LOGLS_WARN(_log, logger(_server)
                                 << logger(_socket) << "rejecting target with encoded NULs: " << target);
        return false;
    }

    std::string squery = targetMatch[2];
    static boost::regex queryRe{R"(([^=&]+)(?:=([^&]*))?)"};  // e.g. "key[=value]"
    auto end = boost::sregex_iterator{};
    for (auto i = boost::make_regex_iterator(squery, queryRe); i != end; ++i) {
        std::string key = _percentDecode((*i)[1], false, hasNULs);
        std::string value = _percentDecode((*i)[2], false, hasNULs);
        if (hasNULs) {
            LOGLS_WARN(_log, logger(_server)
                                     << logger(_socket) << "rejecting target with encoded NULs: " << target);
            return false;
        }
        query[key] = value;
    }

    return true;
}

bool Request::_parseBody() {
    // TODO: implement application/x-www-form-urlencoded body -> body
    return true;
}

std::string Request::_percentDecode(std::string const& encoded, bool exceptPathDelimeters, bool& hasNULs) {
    std::string decoded;
    hasNULs = false;

    static boost::regex codepointRe(R"(%[0-7][0-7a-fA-F])");
    auto pbegin = boost::sregex_iterator(encoded.begin(), encoded.end(), codepointRe);
    auto pend = boost::sregex_iterator();

    std::string::const_iterator tail = encoded.cbegin();

    for (boost::sregex_iterator i = pbegin; i != pend; ++i) {
        decoded.append(i->prefix().first, i->prefix().second);
        tail = i->suffix().first;
        char codepoint = strtol(i->str().c_str() + 1, NULL, 16);

        if (codepoint == '\0') hasNULs = true;

        // If decoding a path, leave any encoded slashes encoded (but ensure lower case), so they don't
        // become confused with path-element-delimiting slashes. We elsewhere make sure that intra-element
        // slashes within the matchers are lowercase percent-encoded as well (see Path.cc).
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

}  // namespace lsst::qserv::qhttp
