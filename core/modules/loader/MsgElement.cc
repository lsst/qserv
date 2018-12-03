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
#include "loader/MsgElement.h"

// System headers
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.MsgElement");
}

namespace lsst {
namespace qserv {
namespace loader {


bool MsgElement::retrieveType(BufferUdp &data, char& elemType) {
    return data.retrieve(&elemType, sizeof(elemType));
}


MsgElement::Ptr MsgElement::retrieve(BufferUdp& data) {
    char elemT;
    if (not retrieveType(data, elemT)) {
        LOGS(_log, LOG_LVL_INFO, "no type retrieved");
        return nullptr; // the data probably has not been read from the socket yet.
    }
    MsgElement::Ptr msgElem = create(elemT);
    if (msgElem != nullptr && not msgElem->retrieveFromData(data)) {
        // No good way to recover from missing data from a know type.
        throw LoaderMsgErr(ERR_LOC, "static retrieve, incomplete data for type=" +
                std::to_string((int)elemT) + " data:" + data.dumpStr());
    }
    return msgElem;
}


MsgElement::Ptr MsgElement::create(char elementType) {
    switch (elementType) {
        case STRING_ELEM:
            return std::make_shared<StringElement>();
        case UINT16_ELEM:
            return std::make_shared<UInt16Element>();
        case UINT32_ELEM:
            return std::make_shared<UInt32Element>();
        case UINT64_ELEM:
            return std::make_shared<UInt64Element>();
        case NOTHING:
            // Fallthrough
        default:
            throw LoaderMsgErr(ERR_LOC, "MsgElement::create Unexpected type " +
                               std::to_string(elementType));
    }
}


// Returns data pointer after inserted string.
bool StringElement::appendToData(BufferUdp& data) {
     auto len = element.length();
     auto sz = sizeof(uint16_t);
     auto totalLength = len + sz + 1; // string, length of string, data type.
     if (not data.isAppendSafe(totalLength)) {
         LOGS(_log, LOG_LVL_INFO, "StringElement append makes data too long total=" << totalLength <<
                                 " data.writeLen=" << data.getAvailableWriteLength() <<
                                 " max=" << data.getMaxLength());
         return false;
     }

     // Insert type
     _appendType(data);

     // Insert the length
     uint16_t lenU16 = len;
     uint16_t netLen = htons(lenU16);

     data.append(&netLen, sz);

     // Insert the string
     if (not data.append(element.data(), len)) {
         throw LoaderMsgErr(ERR_LOC, "StringElement append unexpectedly failed element=" + element +
                                " data=" + data.dumpStr());
     }
     return true;

}


bool StringElement::retrieveFromData(BufferUdp& data) {
    // Get the length.
    uint16_t netLen;
    if (not data.retrieve(&netLen, sizeof(uint16_t))) {
        LOGS(_log, LOG_LVL_WARN, "retrieveFromData failed to retrieve length");
        return false;
    }

    uint16_t len = ntohs(netLen);

    // Get the string.
    bool res =  data.retrieveString(element, len);
    return res;
}


}}} // namespace lsst::qserv::loader
