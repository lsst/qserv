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
/// QservRequest.h
#ifndef LSST_QSERV_WPUBLISH_QSERV_REQUEST_H
#define LSST_QSERV_WPUBLISH_QSERV_REQUEST_H

// System headers

// Third party headers
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "proto/FrameBuffer.h"
#include "proto/worker.pb.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class QservRequest is a base class for a family of the client-side requests
  * (classes) to the Qserv worker management services.
  */
class QservRequest
    :   public XrdSsiRequest {

public:

    // Copy semantics is prohibited
    QservRequest (QservRequest const&) = delete;
    QservRequest& operator= (QservRequest const&) = delete;

    /// Destructor
    virtual ~QservRequest ();

protected:

    /// Default construtor
    QservRequest ();

    /**
     * Serialize a request into the provided buffer. The method is required to be
     * provided by a subclass.
     *
     * @param buf - request buffer for serializing a request 
     */
    virtual void onRequest (proto::FrameBuffer& buf) = 0;

    /**
     * Process response from Qserv. The method is required to be rovided by a subclass.
     *
     * @param view - buffer view for parsing results
     */
    virtual void onResponse (proto::FrameBufferView& view) = 0;

    /// Implements the corresponidng method of the base class
    char* GetRequest (int& dlen) override;

    /// Implements the corresponidng method of the base class
    bool ProcessResponse (const XrdSsiErrInfo&  eInfo,
                          const XrdSsiRespInfo& rInfo) override;

    /// Implements the corresponidng method of the base class
    XrdSsiRequest::PRD_Xeq ProcessResponseData (const XrdSsiErrInfo& eInfo,
                                                char* buff,
                                                int   blen,
                                                bool  last) override;

    /// Request finalizer (cleanup, etc.)
    void Finished ();

private:

    // Request buffer (gets prepared by subclasses before sending a request
    // to the worker service of Qserv)

    proto::FrameBuffer _frameBuf;   ///< buffer for serializing messages before sending them

    // Response buffer (gets updated when receiving a response stream of
    // data from a worker management service of Qserv)

    static int const _bufIncrementSize; ///< for incrementing the capacity of the incoming buffer

    int _bufSize;       ///< actual (meaningful) number of bytes in the incoming buffer
    int _bufCapacity;   ///< total capacity of the incoming buffer

    char* _buf;         ///< buffer for incomming data
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_QSERV_REQUEST_H