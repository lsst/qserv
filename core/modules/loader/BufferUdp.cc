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
#include "BufferUdp.h"

// system headers


// Third-party headers


// qserv headers
#include "loader/LoaderMsg.h"


// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.BufferUdp");
}

namespace lsst {
namespace qserv {
namespace loader {

MsgElement::Ptr BufferUdp::readFromSocket(boost::asio::ip::tcp::socket& socket, std::string const& note) {
    for (;;) {
        LOGS(_log, LOG_LVL_INFO, note << " &&& readFromSocket");
        boost::system::error_code error;

        size_t len = socket.read_some(boost::asio::buffer(_wCursor, getAvailableWriteLength()), error);
        _wCursor += len; /// must advance the cursor.

        LOGS(_log, LOG_LVL_INFO, note << " &&& readFromSocket len=" << len << " " << dump());

        // error is only supposed to be non-zero if len is zero.
        if (error == boost::asio::error::eof) {
            break; // Connection closed cleanly by peer.
        } else if (error) {
            throw boost::system::system_error(error); // Some other error.
        }

        /// Try to retrieve an element (there's no guarantee that an entire element got read in a single read.
        // Store original cursor positions so they can be restored if the read fails.
        MsgElement::Ptr msgElem = _safeRetrieve();
        if (msgElem != nullptr) {
            return msgElem;
        }
    }
    return nullptr;
}


std::shared_ptr<MsgElement> BufferUdp::_safeRetrieve() {
    auto wCursorOriginal = _wCursor;
    auto rCursorOriginal = _rCursor;
    MsgElement::Ptr msgElem = MsgElement::retrieve(*this);
    if (msgElem != nullptr) {
        return msgElem;
    } else {
        _wCursor = wCursorOriginal;
        _rCursor = rCursorOriginal;
    }
    return nullptr;
}


}}} // namespace lsst:qserv:loader
