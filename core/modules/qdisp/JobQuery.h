/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
#include <atomic>
#include <memory>
#include <mutex>

// Qserv headers
#include "qdisp/Executive.h"
#include "qdisp/JobDescription.h"
#include "qdisp/ResponseHandler.h"
#include "util/InstanceCount.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class QueryRequest;

/** This class is used to describe, monitor, and control a single query to a worker.
 *
 */
class JobQuery : public std::enable_shared_from_this<JobQuery> {
public:
    typedef std::shared_ptr<JobQuery> Ptr;

    /// Factory function to make certain a shared_ptr is used and _setup is called.
    static JobQuery::Ptr newJobQuery(Executive* executive, JobDescription const& jobDescription,
            JobStatus::Ptr const& jobStatus, std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc,
            qmeta::QueryId qid) {
        Ptr jq{new JobQuery{executive, jobDescription, jobStatus, markCompleteFunc, qid}};
        jq->_setup();
        return jq;
    }

    virtual ~JobQuery();
    virtual bool runJob();

    int getIdInt() const { return _jobDescription.id(); }
    std::string const& getIdStr() const { return _idStr; }
    JobDescription& getDescription() { return _jobDescription; }
    JobStatus::Ptr getStatus() { return _jobStatus; }

    void setQueryRequest(std::shared_ptr<QueryRequest> const& qr) {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        _queryRequestPtr = qr;
    }
    std::shared_ptr<QueryRequest> getQueryRequest() {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _queryRequestPtr;
    }

    std::shared_ptr<MarkCompleteFunc> getMarkCompleteFunc() { return _markCompleteFunc; }

    bool cancel();
    bool isCancelled() { return _cancelled; }

    void freeQueryResource(QueryResource* qr);

    void provisioningFailed(std::string const& msg, int code);
    std::shared_ptr<QueryResource> getQueryResource() {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _queryResourcePtr;
    }

    friend std::ostream& operator<<(std::ostream& os, JobQuery const& jq);

protected:
    /// Make a copy of the job description. JobQuery::_setup() must be called after creation.
    JobQuery(Executive* executive, JobDescription const& jobDescription,
        JobStatus::Ptr const& jobStatus, std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc,
        qmeta::QueryId qid);

    void _setup() {
        _jobDescription.respHandler()->setJobQuery(shared_from_this());
    }

    int _getRunAttemptsCount() const {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _runAttemptsCount;
    }
    int _getMaxRetries() const { return 5; } // Arbitrary value until solid value with reason determined.

    // Values that don't change once set.
    Executive* _executive;
    JobDescription _jobDescription;
    std::shared_ptr<MarkCompleteFunc> _markCompleteFunc;

    // JobStatus has its own mutex.
    JobStatus::Ptr _jobStatus; ///< Points at status in Executive::_statusMap

    qmeta::QueryId const _qid; // User query id
    std::string const _idStr; ///< Identifier string for logging.

    // Values that need mutex protection
    mutable std::recursive_mutex _rmutex; ///< protects _runAttemtsCount, _queryResourcePtr,
                                          /// _queryRequestPtr
    int _runAttemptsCount {0}; ///< Number of times someone has tried to run this job.

    // xrootd items
    std::shared_ptr<QueryResource> _queryResourcePtr;
    std::shared_ptr<QueryRequest> _queryRequestPtr;

    // Cancellation
    std::atomic<bool> _cancelled {false};
    util::InstanceCount _instC{"JobQuery"};
};

}}} // end namespace

#endif /* LSST_QSERV_QDISP_JOBQUERY_H_ */
