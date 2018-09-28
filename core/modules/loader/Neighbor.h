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
#ifndef LSST_QSERV_LOADER_NEIGHBOR_H_
#define LSST_QSERV_LOADER_NEIGHBOR_H_

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/Central.h"


namespace lsst {
namespace qserv {
namespace loader {


/// Class to describe one of a worker's neighbors.
class Neighbor {
public:
    enum Type {
        LEFT = 1,
        RIGHT = 2
    };

    Neighbor() = delete;
    explicit Neighbor(Type t) : _type(t) {}

    std::string getTypeStr() { return _type == LEFT ? "LEFT" : "RIGHT"; }

    void setAddress(std::string const& hostName, int port) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _address.reset(new NetworkAddress(hostName, port));
    }

    void setAddress(NetworkAddress const& addr) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _address.reset(new NetworkAddress(addr));
    }

    NetworkAddress getAddress() {
        std::lock_guard<std::mutex> lck(_nMtx);
        return *_address;
    }

    void setName(uint32_t name);
    uint32_t getName() const { return _name; }

    void setEstablished(bool val) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _established = val;
    }

    bool getEstablished() const { return _established; }

private:
    NetworkAddress::UPtr _address{new NetworkAddress("", -1)};
    uint32_t _name{0}; ///< Name of neighbor, 0 means no neighbor.
    bool _established{false};
    std::mutex _nMtx;
    Type _type;
};

}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_NEIGHBOR_H_
