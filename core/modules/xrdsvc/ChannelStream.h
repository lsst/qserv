// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2018 LSST Corporation.
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
#ifndef LSST_QSERV_XRDSVC_CHANNELSTREAM_H
#define LSST_QSERV_XRDSVC_CHANNELSTREAM_H

// System headers
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

// qserv headers
#include "xrdsvc/StreamBuffer.h"

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh" // required by XrdSsiStream
#include "XrdSsi/XrdSsiStream.hh"

namespace lsst {
namespace qserv {
namespace xrdsvc {

/* &&&
/// SimpleBuffer is a buffer for transferring data packets to XrdSsi.
class SimpleBuffer : public XrdSsiStream::Buffer {
public:
    using Ptr = std::shared_ptr<SimpleBuffer>;

    SimpleBuffer() = delete;
       SimpleBuffer(SimpleBuffer const&) = delete;
       SimpleBuffer& operator=(SimpleBuffer const&) = delete;

    // Factory function, because this should be able to delete itself when Recycle() is called.
    static SimpleBuffer::Ptr create(std::string &input) {
        Ptr ptr(new SimpleBuffer(input));
        ptr->_selfKeepAlive = ptr;
        return ptr;
    }


    size_t getSize() const {return _size;}


    //!> Call to recycle the buffer when finished
    void Recycle() override {
        {
            std::lock_guard<std::mutex> lg(_mtx);
            doneWithThis = true;
        }
        _cv.notify_all();

        // delete this;
        // Effectively reset _selfPointer, and if nobody else was
        // referencing this, it will delete itself when this call is done.
        Ptr keepAlive = std::move(_selfKeepAlive);
    }

    // Wait until recycle is called.
    void waitForDoneWithThis() {
        std::unique_lock<std::mutex> uLock(_mtx);
        _cv.wait(uLock, [this](){ return doneWithThis == true; });
    }

    // Inherited from XrdSsiStream:
    // char  *data; //!> -> Buffer containing the data
    // Buffer *next; //!> For chaining by buffer receiver

    virtual ~SimpleBuffer() {
        delete[] data;
    }

private:
    SimpleBuffer(std::string const& input) : _size(input.size()) {
        data = new char[_size];
        memcpy(data, input.data(), _size);
        next = 0;
    }


    size_t const  _size; ///< Size of *data, needs to be immutable.
    std::mutex _mtx;
    std::condition_variable _cv;
    bool doneWithThis{false};
    Ptr _selfKeepAlive; // keep this object alive
    util::InstanceCount _ic{"&&&SimpleBuffer"};
};
*/


/// ChannelStream is an implementation of an XrdSsiStream that accepts
/// SendChannel streamed data.
class ChannelStream : public XrdSsiStream {
public:
    ChannelStream();
    virtual ~ChannelStream();

    /// Push in a data packet
    //void append(char const* buf, int bufLen, bool last); // &&& delete
    void append(StreamBuffer::Ptr const& StreamBuffer, bool last);

    /// Pull out a data packet as a Buffer object (called by XrdSsi code)
    virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last);

    bool closed() const { return _closed; }

private:
    bool _closed; ///< Closed to new append() calls?
    // Can keep a deque of (buf, bufsize) to reduce copying, if needed.
    std::deque<StreamBuffer::Ptr> _msgs; ///< Message queue
    std::mutex _mutex; ///< _msgs protection
    std::condition_variable _hasDataCondition; ///< _msgs condition
};

}}} // namespace lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_CHANNELSTREAM_H
