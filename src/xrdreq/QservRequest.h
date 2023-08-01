// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
#ifndef LSST_QSERV_XRDREQ_QSERV_REQUEST_H
#define LSST_QSERV_XRDREQ_QSERV_REQUEST_H

// System headers
#include <atomic>
#include <memory>
#include <string>

// Third party headers
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "proto/FrameBuffer.h"
#include "proto/worker.pb.h"

namespace lsst::qserv::xrdreq {

/**
 * Class QservRequest is a base class for a family of the client-side requests
 * (classes) to Qserv workers.
 */
class QservRequest : public XrdSsiRequest {
public:
    QservRequest(QservRequest const&) = delete;
    QservRequest& operator=(QservRequest const&) = delete;
    virtual ~QservRequest() override;

    /**
     * Do a proper request cancellation to ensure a pointer to the request gets deleted
     * after calling XrdSsiRequest::Finished(true).
     */
    void cancel();

protected:
    QservRequest();

    /**
     * Setting a pointer to the object would guarantee that the life expectancy
     * of the request be preserved before it's finished/failed and the corresponding
     * notifications are sent to a subclass via the virtual methods QservRequest::onResponse()
     * or QservRequest::onError(). The pointer will be reset after calling either of
     * these methods, or the method QservRequest::cancel().
     * @param ptr The pointer to be set.
     * @throws std::invalid_argument if the pointer is empty or pointing to a different
     *   request object.
     */
    void setRefToSelf4keepAlive(std::shared_ptr<QservRequest> ptr);

    /**
     * Serialize a request into the provided buffer. The method is required to be
     * provided by a subclass.
     * @param buf A request buffer for serializing a request.
     */
    virtual void onRequest(proto::FrameBuffer& buf) = 0;

    /**
     * Process response from Qserv. The method is required to be provided by a subclass.
     * @param view The buffer view for parsing results.
     */
    virtual void onResponse(proto::FrameBufferView& view) = 0;

    /**
     * Notify a base class about a failure occurred when sending a request data
     * or receiving a response.
     * @param error A message explaining a reason of the failure.
     */
    virtual void onError(std::string const& msg) = 0;

    char* GetRequest(int& dlen) override;
    bool ProcessResponse(const XrdSsiErrInfo& eInfo, const XrdSsiRespInfo& rInfo) override;
    void ProcessResponseData(const XrdSsiErrInfo& eInfo, char* buff, int blen, bool last) override;

private:
    /// The global counter for the number of instances of any subclasses
    static std::atomic<size_t> _numClassInstances;

    /// Request buffer is prepared by subclasses before sending a request to a worker.
    proto::FrameBuffer _frameBuf;

    // Response buffer is updated when receiving a response stream of data from a worker.

    /// The (very first and the) last increment of the capacity of the incoming
    /// buffer is used to limit the amount of bytes to be received from a server.
    int _bufIncrementSize;

    int _bufSize;      ///< actual (meaningful) number of bytes in the incoming buffer
    int _bufCapacity;  ///< total capacity of the incoming buffer

    char* _buf;  ///< buffer for incomming data

    /// The reference to the object is needed to guarantee the life expectency of
    /// the request object while the request is still being processed.
    std::shared_ptr<QservRequest> _refToSelf4keepAlive;
};

}  // namespace lsst::qserv::xrdreq

#endif  // LSST_QSERV_XRDREQ_QSERV_REQUEST_H