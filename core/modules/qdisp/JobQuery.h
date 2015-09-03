// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
 *
 *  Created on: Sep 3, 2015
 *      Author: jgates
 */

#ifndef LSST_QSERV_QDISP_JOBQUERY_H_
#define LSST_QSERV_QDISP_JOBQUERY_H_

// System headers
#include <memory>
#include <mutex>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/Executive.h"
#include "qdisp/ResponseRequester.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class QueryRequest;

/** This class is used to describe, monitor, and control a single query to a worker.
 *
 */
class JobQuery {
public:
    typedef std::shared_ptr<JobQuery> Ptr;
    // Make a copy of the job description.
    JobQuery(Executive* executive, JobDescription const& jobDescription,
             JobStatus::Ptr jobStatus, std::shared_ptr<MarkCompleteFunc> markCompleteFunc) :
        _executive(executive), _jobDescription(jobDescription),
        _markCompleteFunc(markCompleteFunc), _jobStatus(jobStatus),
        _attemptsToRun(0), _cancelled(false) {}

    void setupWeakThis(Ptr jobQuery) {
        _weakThis = jobQuery;
        _jobDescription.respHandler()->setJobQuery(jobQuery);
    }

    virtual ~JobQuery() {
        LOGF_DEBUG("~JobQuery _jobId=%1%" % getId());
    }
    int getAttemptsToRun() const {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _attemptsToRun;
    }
    int getMaxRetries() const { return 5; }
    bool shouldRetry() const { return getAttemptsToRun() < getMaxRetries(); }
    virtual bool runJob();

    int getId() const { return _jobDescription.id(); }
    JobDescription& getDescription() { return _jobDescription; }
    JobStatus::Ptr getStatus() { return _jobStatus; }

    void provisioningFailed(std::string const& msg, int code);
    std::shared_ptr<QueryResource> getQueryResource() { return _queryResourcePtr; }
    void freeQueryResource() { _queryResourcePtr.reset(); }

    void setQueryRequest(std::shared_ptr<QueryRequest> qr) { _queryRequestPtr = qr; }
    std::shared_ptr<QueryRequest> getQueryRequest() { return _queryRequestPtr; }
    void freeQueryRequest() { _queryRequestPtr.reset(); }

    std::shared_ptr<MarkCompleteFunc> getMarkCompleteFunc() { return _markCompleteFunc; }

    bool cancel();
    bool getCancelled() { return _cancelled.get(); }
    std::recursive_mutex& getCancelledMutex() { return _cancelled.getMutex(); }

    std::string toString() const ;
    friend std::ostream& operator<<(std::ostream& os, JobQuery const& jq);

protected:

    // Values that don't change once set.
    Executive* _executive;
    JobDescription _jobDescription;
    std::shared_ptr<MarkCompleteFunc> _markCompleteFunc;
    std::weak_ptr<JobQuery> _weakThis; ///< Connection to the shared_ptr for this object.

    // JobStatus has its own mutex.
    JobStatus::Ptr _jobStatus; ///< Points at status in Executive::_statusMap

    // Values that need mutex protection
    mutable std::recursive_mutex _rmutex;
    int _attemptsToRun;

    // xrootd items
    std::shared_ptr<QueryResource> _queryResourcePtr;
    std::shared_ptr<QueryRequest> _queryRequestPtr;

    // Cancellation
    util::Flag<bool> _cancelled;
};

}}} // end namespace

#endif /* LSST_QSERV_QDISP_JOBQUERY_H_ */
