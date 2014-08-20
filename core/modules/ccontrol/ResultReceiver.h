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

#ifndef LSST_QSERV_CCONTROL_RESULTRECEIVER_H
#define LSST_QSERV_CCONTROL_RESULTRECEIVER_H

// System headers
#include <string>
#include <vector>

// Third-party
#include <boost/make_shared.hpp>

// Qserv headers
#include "qdisp/QueryReceiver.h"
#include "util/Callable.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace rproc {
  class TableMerger;
}
namespace ccontrol {
  class MergeAdapter;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {

/// See QueryReceiver API for basic documentation
class ResultReceiver : public qdisp::QueryReceiver {
public:
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    ResultReceiver(boost::shared_ptr<rproc::TableMerger> merger,
                   std::string const& tableName);
    virtual ~ResultReceiver() {}

    virtual int bufferSize() const;
    virtual char* buffer();
    virtual bool flush(int bLen, bool last);
    virtual void errorFlush(std::string const& msg, int code);
    virtual bool finished() const;
    virtual bool reset();
    virtual std::ostream& print(std::ostream& os) const;

    /// Add a callback to be invoked when the receiver finishes processing
    /// a response from its request.
    void addFinishHook(util::UnaryCallable<void, bool>::Ptr f);

    Error getError() const;

private:
    /// (helper) merge buffer and shift contents depending on merge size.
    bool _appendAndMergeBuffer(int bLen);

    boost::shared_ptr<rproc::TableMerger> _merger; //< Target merging delegate
    std::string _tableName; //< Target table name

    /// Invoked at receiver completion
    util::UnaryCallable<void, bool>::Ptr _finishHook;

    int _bufferSize; //< Available size from _buffer
    int _actualSize; //< Allocated size
    std::vector<char> _actualBuffer; //< Entire allocated buffer
    char* _buffer; //< Current buffer insertion point
    bool _flushed; //< Has data ben flushed into this object?
    bool _dirty; //< Has data been flushed past this object?
    Error _error; //< Internal error state
};

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_RESULTRECEIVER_H
