// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_WBASE_SENDCHANNEL_H
#define LSST_QSERV_WBASE_SENDCHANNEL_H

// System headers
#include <string>
#include <stdexcept>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Qserv headers
#include "global/Bug.h"
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace wbase {

/// SendChannel objects abstract an byte-output mechanism. Provides a layer of
/// abstraction to reduce coupling to the XrdSsi API. SendChannel generally
/// accepts only one call to send bytes, unless the sendStream call is used.
class SendChannel {
public:
    typedef util::VoidCallable<void> ReleaseFunc;
    typedef boost::shared_ptr<ReleaseFunc> ReleaseFuncPtr;
    typedef long long Size;

    /// Send a buffer
    virtual bool send(char const* buf, int bufLen) = 0;

    /// Send an error
    virtual bool sendError(std::string const& msg, int code) = 0;

    /// Send the bytes from a POSIX file handle
    virtual bool sendFile(int fd, Size fSize) = 0;

    /// Send a bucket of bytes.
    /// @param last true if no more sendStream calls will be invoked.
    virtual bool sendStream(char const* buf, int bufLen, bool last) {
        throw Bug("Streaming is unimplemented, should not see this");
    }

    /// Set a function to be called when a resources from a deferred send*
    /// operation may be released. This allows a sendFile() caller to be
    /// notified when the file descriptor may be closed and perhaps reclaimed.
    void setReleaseFunc(ReleaseFuncPtr r) { _release = r; }
    void release() {
        if(_release) {
            (*_release)();
        }
    }
    static boost::shared_ptr<SendChannel> newNopChannel();
    static boost::shared_ptr<SendChannel> newStringChannel(std::string& dest);

protected:
    ReleaseFuncPtr _release;
};
}}} // lsst::qserv::wbase
#endif // LSST_QSERV_WBASE_SENDCHANNEL_H
