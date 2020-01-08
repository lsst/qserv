// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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

// Class header
#include "loader/WorkerConfig.h"

// System headers
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// Third party headers
#include "boost/asio.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.Util");
}

namespace lsst {
namespace qserv {
namespace loader {

std::vector<std::string> split(std::string const& in, std::function<bool(char)> func) {
    std::vector<std::string> result;
    // special case of empty string
    if (in.empty()) {
        result.push_back("");
        return result;
    }

    auto pos = in.begin();
    while (pos != in.end()) {
        std::string str("");
        while (pos != in.end() && !func(*pos)) {
            str += *pos;
            ++pos;
        }
        result.push_back(str);
        if (pos != in.end()) {
            ++pos;
            // Another special case. The last character was a match
            // for func so append an empty string. Basically ensure that
            // ".com" is distinguishable from ".com." in the input.
            if (pos == in.end())  {
                result.push_back("");
            }
        }
    }
    return result;
}


/// TODO Test to be put in unit tests
bool splitTest() {
    auto out = split("www.github.com", [](char c) {return c == '.';});
    auto test = (out[0] == "www" && out[1] == "github" && out[2] == "com");
    if (!test) return false;

    out = split("", [](char c) {return c == '.';});
    test = (out[0] == "" && out.size() == 1);
    if (!test) return false;

    out = split(".com.", [](char c) {return c == '.';});
    test = (out[0] == "" && out[1] == "com" && out[2] == "");
    if (!test) return false;
    return true;
}


std::string getOurHostName(unsigned int domains=0) {
    std::string out("");
    std::string const ourHost = boost::asio::ip::host_name();
    std::string ourHostIp;
    LOGS(_log, LOG_LVL_INFO, "ourHost=" << ourHost);
    boost::asio::io_service ioService;
    boost::asio::io_context ioContext;


    char *IPbuffer;
    struct hostent *host_entry;

    host_entry = gethostbyname(ourHost.c_str());

    // convert to ASCII
    IPbuffer = inet_ntoa(*((struct in_addr*)host_entry->h_addr_list[0]));
    LOGS(_log, LOG_LVL_DEBUG, "host_entry=" << host_entry << " IP=" << IPbuffer);
    ourHostIp = IPbuffer;

    hostent *he;
    in_addr ipv4addr;

    inet_pton(AF_INET, ourHostIp.c_str(), &ipv4addr);
    he = gethostbyaddr(&ipv4addr, sizeof ipv4addr, AF_INET);
    if (he == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "getOurHostName() no hostname found!");
        return out;
    } else {
        LOGS(_log, LOG_LVL_INFO, " host name=" << he->h_name); // full name
        if (domains == 0) {
            out = he->h_name;
            return out;
        } else {
            auto splitName = split(he->h_name, [](char c) {return c == '.';});
            out = splitName[0];
            for(unsigned int j=1; j < domains && j < splitName.size(); ++j) {
                out += "." + splitName.at(j);
            }
        }
    }
    return out;
}

}}} // namespace lsst::qserv::loader

