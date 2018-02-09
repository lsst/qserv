// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2018LSST Corporation.
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
#ifndef LSST_QSERV_XRDSVC_STREAMBUFFER_H
#define LSST_QSERV_XRDSVC_STREAMBUFFER_H

// System headers
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

// qserv headers
#include "util/InstanceCount.h" // &&&

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh" // required by XrdSsiStream
#include "XrdSsi/XrdSsiStream.hh"

namespace lsst {
namespace qserv {
namespace xrdsvc {


/// StreamBuffer is a single use buffer for transferring data packets
/// to XrdSsi.
/// Its notable feature is the Recycle() function, which XrdSsi will
/// promptly call when it no longer needs the buffer.
class StreamBuffer : public XrdSsiStream::Buffer {
public:
    using Ptr = std::shared_ptr<StreamBuffer>;

    // Copying this would be very confusing for something waiting for Recycle().
    StreamBuffer() = delete;
    StreamBuffer(StreamBuffer const&) = delete;
    StreamBuffer& operator=(StreamBuffer const&) = delete;

    // Factory function, because this should be able to delete itself when Recycle() is called.
    static StreamBuffer::Ptr create(std::string &input);

    size_t getSize() const { return _dataStr.size(); }

    /// @Return total number of bytes used by ALL StreamBuffer objects.
    static size_t getTotalBytes() { return _totalBytes; }

    //!> Call to recycle the buffer when finished
    void Recycle() override;

    // Wait until recycle is called.
    void waitForDoneWithThis();

    // Inherited from XrdSsiStream:
    // char  *data; //!> -> Buffer containing the data
    // Buffer *next; //!> For chaining by buffer receiver

    virtual ~StreamBuffer();

private:
    // This constructor will invalidate 'input'.
    explicit StreamBuffer(std::string &input);

    std::string _dataStr;
    std::mutex _mtx;
    std::condition_variable _cv;
    bool doneWithThis{false};
    Ptr _selfKeepAlive; ///< keep this object alive until after Recycle() is called.
    util::InstanceCount _ic{"&&&StreamBuffer"};

    static std::atomic<size_t> _totalBytes;
};


}}} // namespace lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_STREAMBUFFER_H
