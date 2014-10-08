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
#ifndef LSST_QSERV_QDISP_QUERYRESOURCE_H
#define LSST_QSERV_QDISP_QUERYRESOURCE_H

// System headers
#include <string>

// Third-party headers
#include "boost/shared_ptr.hpp"
#include "XrdSsi/XrdSsiService.hh" // Resource

// Local headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace qdisp {
// Local forward declarations
class ExecStatus;
class QueryReceiver;
class QueryRequest;
class ResponseRequester;

/// Note: this object takes responsibility for deleting itself once it is passed
/// off via service->Provision(resourceptr).
class QueryResource : public XrdSsiService::Resource {
public:
    /// @param rPath resource path, e.g. /LSST/12312
    /// @param payload serialized protobuf request message
    /// @param requester response requester
    /// @param retryFunc high-level retry function.
    /// @param status reference to update the current execution status
    QueryResource(std::string const& rPath,
                  std::string const& payload,
                  boost::shared_ptr<ResponseRequester> requester,
                  boost::shared_ptr<util::UnaryCallable<void, bool> > finishFunc,
                  boost::shared_ptr<util::VoidCallable<void> > retryFunc,
                  ExecStatus& status)
        : Resource(rPath.c_str()),
          _payload(payload),
          _requester(requester),
          _finishFunc(finishFunc),
          _retryFunc(retryFunc),
          _status(status) {
    }

    virtual ~QueryResource() {}

    void ProvisionDone(XrdSsiSession* s);

    XrdSsiSession* _session; ///< unowned, do not delete.
    QueryRequest* _request; ///< Owned temporarily, special deletion handling.
    std::string const _payload; ///< Request payload
    boost::shared_ptr<ResponseRequester> _requester; ///< Response requester
    /// Called upon transaction finish
    boost::shared_ptr<util::UnaryCallable<void, bool> > _finishFunc;
    /// Called to retry the transaction
    boost::shared_ptr<util::VoidCallable<void> > _retryFunc;
    ExecStatus& _status;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYRESOURCE_H
