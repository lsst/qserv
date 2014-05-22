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
#ifndef LSST_QSERV_QDISP_QUERYRECEIVER_H
#define LSST_QSERV_QDISP_QUERYRECEIVER_H

// Third-party headers
#include <boost/shared_ptr.hpp>

// Qserv headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class QueryReceiver {
public:
    struct Error {
        std::string msg;
        int code;
    };

    typedef util::VoidCallable<void> CancelFunc;

    typedef boost::shared_ptr<QueryReceiver> Ptr;
    virtual ~QueryReceiver() {}
    virtual int bufferSize() const = 0;
    virtual char* buffer() = 0;
    virtual bool flush(int bLen, bool last) = 0;
    virtual void errorFlush(std::string const& msg, int code) = 0;
    virtual bool finished() const = 0;
    virtual bool reset() = 0; //< Reset the state that a request can be retried.
    virtual std::ostream& print(std::ostream& os) const = 0;

    Error getError() const { return Error(); };

    virtual void registerCancel(boost::shared_ptr<CancelFunc> cancelFunc) {
        _cancelFunc = cancelFunc;
    }
    void cancel() {
        if(_cancelFunc) {
            (*_cancelFunc)();
        }
    }

protected:
    boost::shared_ptr<CancelFunc> _cancelFunc;

};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYRECEIVER_H
