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
#ifndef LSST_QSERV_QDISP_RESPONSEREQUESTER_H
#define LSST_QSERV_QDISP_RESPONSEREQUESTER_H

// System headers
#include <string>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Qserv headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class ResponseRequester {
public:
    struct Error {
        Error() : code(0) {}
        std::string msg;
        int code;
    };

    typedef util::VoidCallable<void> CancelFunc;

    typedef boost::shared_ptr<ResponseRequester> Ptr;
    virtual ~ResponseRequester() {}

    /// @return a char vector to receive the next message. The vector
    /// should be sized to the request size. The buffer will be filled
    /// before flush(), unless the response is completed (no more
    /// bytes) or there is an error.
    virtual std::vector<char>& nextBuffer() = 0;

    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    virtual bool flush(int bLen, bool last) = 0;

    /// Signal an unrecoverable error condition. No further calls are expected.
    virtual void errorFlush(std::string const& msg, int code) = 0;

    /// @return true if the receiver has completed its duties.
    virtual bool finished() const = 0;
    virtual bool reset() = 0; ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    virtual std::ostream& print(std::ostream& os) const = 0;

    /// @return an error code and description
    virtual Error getError() const { return Error(); };

    /// Set a function to be called that forcibly cancels the ResponseRequester
    /// process. The buffer filler should call this function so that it can be
    /// notified when the receiver no longer cares about being filled.
    virtual void registerCancel(boost::shared_ptr<CancelFunc> cancelFunc) {
        _cancelFunc = cancelFunc;
    }

    /// Cancel operations on the Receiver. This calls _cancelFunc and propagates
    /// cancellation towards the buffer-filler.
    /// Default behavior invokes registered function.
    virtual void cancel() { _callCancel(); }

protected:
    /// Call _cancelFunc.
    void _callCancel() {
        if(_cancelFunc) {
            (*_cancelFunc)();
        }
    }
    boost::shared_ptr<CancelFunc> _cancelFunc;

};

inline std::ostream& operator<<(std::ostream& os, ResponseRequester const& r) {
    return r.print(os);
}


}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_RESPONSEREQUESTER_H
