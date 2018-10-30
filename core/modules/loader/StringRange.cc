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
#include "loader/StringRange.h"

// System headers
#include <iostream>

// qserv headers
#include "loader/BufferUdp.h"
#include "loader/LoaderMsg.h"
#include "proto/loader.pb.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.StringRange");
}

namespace lsst {
namespace qserv {
namespace loader {


std::ostream& operator<<(std::ostream& os, NeighborsInfo const& ni) {
    os << "NeighborsInfo";
    os << " neighborLeft=" << (ni.neighborLeft == nullptr) ? "nullptr" : std::to_string(ni.neighborLeft->get());
    os << " neighborRight=" << (ni.neighborRight == nullptr) ? "nullptr" : std::to_string(ni.neighborRight->get());
    os << " recentAdds=" << ni.recentAdds;
    os << " keyCount=" << ni.keyCount;
    return os;
}

std::ostream& operator<<(std::ostream& os, StringRange const& strRange) {
    os << "valid=" << strRange._valid
       << " min=" << strRange._min
       << " max=" << strRange._maxE
       << " unlimited=" << strRange._unlimited;
    return os;
}


std::string StringRange::incrementString(std::string const& str, char appendChar) {
    std::string output(str);
    size_t pos = output.size() - 1;
    char lastChar = output[pos];
    if (lastChar < 'z') {
        ++lastChar;
        output[pos] = lastChar;
    } else {
        output += appendChar;
    }
    return output;
}


std::string StringRange::decrementString(std::string const& str, char minChar) {
    if (str == "") {
        return "";
    }
    std::string output(str);
    size_t pos = output.size() - 1;
    char lastChar = output[pos];
    --lastChar;
    if (lastChar > minChar) {
        output[pos] = lastChar;
        return output;
    } else {
        output.erase(pos, 1);
    }
    return output;
}


void ProtoHelper::workerKeysInfoExtractor(BufferUdp& data, uint32_t& name, NeighborsInfo& nInfo, StringRange& strRange) {
    auto funcName = "CentralWorker::_workerKeysInfoExtractor";
    LOGS(_log, LOG_LVL_DEBUG, funcName);
    auto protoItem = StringElement::protoParse<proto::WorkerKeysInfo>(data);
    if (protoItem == nullptr) {
        throw LoaderMsgErr(funcName, __FILE__, __LINE__);
    }

    name = protoItem->name();
    nInfo.keyCount = protoItem->mapsize();
    nInfo.recentAdds = protoItem->recentadds();
    proto::WorkerRangeString protoRange = protoItem->range();
    bool valid = protoRange.valid();
    if (valid) {
        std::string min   = protoRange.min();
        std::string max   = protoRange.max();
        bool unlimited = protoRange.maxunlimited();
        strRange.setMinMax(min, max, unlimited);
    }
    proto::Neighbor protoLeftNeigh = protoItem->left();
    nInfo.neighborLeft->update(protoLeftNeigh.name());
    proto::Neighbor protoRightNeigh = protoItem->right();
    nInfo.neighborRight->update(protoRightNeigh.name());
}


}}} // namespace lsst::qserv::loader

