// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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
#include <cstdlib>
#include <functional>
#include <memory>
#include <new>
#include <string>
#include <string.h>

// Third-party headers
#include "XrdSsi/XrdSsiService.hh" // Resource

// Local headers
#include "util/Callable.h"

namespace lsst {
namespace qserv {
namespace qdisp {
// Local forward declarations
class JobQuery;

/** This class is used to request a session from xrootd via Provision
 * ProvisionDone is called by xrootd in some thread with a XrdSsiSession.
 * ProvisionDone creates a QueryRequest using the session and JobQuery,
 * then it calls xrootd's ProcessRequest with that object.
 *
 * This objects existence is controlled by _jobQuery. It must call
 * _jobQuery->freeQueryResource() before leaving ProvisionDone.
 * If ProvisionDone is not called by xrootd, there's really not much that
 * can be done to prevent a memory leak, as there's no way to know that
 * xrootd is done with this.
 */
/// Note: this object takes responsibility for deleting itself once it is passed
/// off via service->Provision(resourceptr).
class QueryResource : public XrdSsiService::Resource {
public:
    typedef std::shared_ptr<QueryResource> Ptr;
    QueryResource(std::shared_ptr<JobQuery> const& jobQuery);

    virtual ~QueryResource();

    void ProvisionDone(XrdSsiSession* s) override;
    const char* eInfoGet(int &code);

    std::shared_ptr<JobQuery> getJobQuery() { return _jobQuery; }
    bool isCancelled();

    friend class QueryResourceDebug;

private:
    XrdSsiSession* _xrdSsiSession {nullptr}; ///< unowned, do not delete.
    std::shared_ptr<JobQuery> _jobQuery;
    int _jobId {-1}; ///< for debugging only TODO delete in DM-3946
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYRESOURCE_H
