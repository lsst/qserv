
// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_NETWORKADDRESS_H_
#define LSST_QSERV_LOADER_NETWORKADDRESS_H_

// system headers
#include <chrono>
#include <list>

// Qserv headers
#include "util/ThreadPool.h"
#include "loader/BufferUdp.h"

namespace lsst {
namespace qserv {
namespace loader {

class StringElement;

/// Comparable network addresses.
struct NetworkAddress {
    using Ptr = std::shared_ptr<NetworkAddress>;
    using UPtr = std::unique_ptr<NetworkAddress>;

    NetworkAddress(std::string const& ip_, int port_) : ip(ip_), port(port_) {}
    NetworkAddress() = delete;
    NetworkAddress(NetworkAddress const&) = default;

    static UPtr create(BufferUdp::Ptr const& bufData, int& tcpPort, std::string const& note);

    const std::string ip;
    const int port; // Most of the workers will have the same port number.

    bool operator==(NetworkAddress const& other) const {
        return (port == other.port && ip == other.ip);
    }

    bool operator!=(NetworkAddress const& other) const {
        return !(*this == other);
    }

    bool operator<(NetworkAddress const& other) const {
        auto compRes = ip.compare(other.ip);
        if (compRes < 0) { return true; }
        if (compRes > 0) { return false; }
        return port < other.port;
    }

    bool operator>(NetworkAddress const& other) const {
        return (other < *this);
    }




    friend std::ostream& operator<<(std::ostream& os, NetworkAddress const& adr);
};


}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_NETWORKADDRESS_H_
