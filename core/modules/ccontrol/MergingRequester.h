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
#ifndef LSST_QSERV_CCONTROL_MERGINGREQUESTER_H
#define LSST_QSERV_CCONTROL_MERGINGREQUESTER_H

// Third-party headers
#include "boost/shared_ptr.hpp"

// Qserv headers
#include "qdisp/ResponseRequester.h"
//#include "util/Callable.h"

// Forward decl
namespace lsst {
namespace qserv {
  class MsgReceiver;
namespace proto {
  class WorkerResponse;
}
namespace rproc {
  class InfileMerger;
//class TableMerger;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {

class MergingRequester : public qdisp::ResponseRequester {
public:
    enum MsgState { INVALID, HEADER_SIZE_WAIT, 
                    HEADER_WAIT, RESULT_WAIT, RESULT_RECV,
                    RESULT_EXTRA, RESULT_LAST,
                    HEADER_ERR, RESULT_ERR };

    typedef boost::shared_ptr<MergingRequester> Ptr;
    virtual ~MergingRequester() {}

    /// @param msgReceiver Message code receiver
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingRequester(boost::shared_ptr<MsgReceiver> msgReceiver,
                     boost::shared_ptr<rproc::InfileMerger> merger,
                     std::string const& tableName);

    /// @return a char vector to receive the next message. The vector
    /// should be sized to the request size. The buffer will be filled
    /// before flush(), unless the response is completed (no more
    /// bytes) or there is an error.
    virtual std::vector<char>& nextBuffer() { return _buffer; }
 
    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    virtual bool flush(int bLen, bool last);

    /// Signal an unrecoverable error condition. No further calls are expected.
    virtual void errorFlush(std::string const& msg, int code);

    /// @return true if the receiver has completed its duties.
    virtual bool finished() const;

    virtual bool reset(); ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    virtual std::ostream& print(std::ostream& os) const;

    /// @return an error code and description
    virtual Error getError() const { return _error; }

    using ResponseRequester::registerCancel;
    using ResponseRequester::cancel;

private:    
    void _initState();
    bool _merge();

    boost::shared_ptr<MsgReceiver> _msgReceiver; ///< Message code receiver
    boost::shared_ptr<rproc::InfileMerger> _infileMerger; ///< Merging delegate
    std::string _tableName; ///< Target table name
    std::vector<char> _buffer;
    Error _error;
    MsgState _state;
    boost::shared_ptr<proto::WorkerResponse> _response;
    bool _flushed;

};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_CCONTROL_MERGINGREQUESTER_H
