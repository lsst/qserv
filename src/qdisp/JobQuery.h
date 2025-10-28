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

namespace lsst::qserv::qdisp {

class QdispPool;
class QueryRequest;

/** This class is used to describe, monitor, and control a single query to a worker.
 *
 */
class JobQuery : public std::enable_shared_from_this<JobQuery> {
public:
    typedef std::shared_ptr<JobQuery> Ptr;

    /// Factory function to make certain a shared_ptr is used and _setup is called.
    static JobQuery::Ptr create(Executive::Ptr const& executive, JobDescription::Ptr const& jobDescription,
                                JobStatus::Ptr const& jobStatus,
                                std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc, QueryId qid) {
        Ptr jq = std::make_shared<JobQuery>(executive, jobDescription, jobStatus, markCompleteFunc, qid);
        jq->_setup();
        return jq;
    }

    virtual ~JobQuery();
    virtual bool runJob();

    QueryId getQueryId() const { return _qid; }
    int getIdInt() const { return _jobDescription->id(); }
    std::string const& getIdStr() const { return _idStr; }
    JobDescription::Ptr getDescription() { return _jobDescription; }
    std::shared_ptr<ResponseHandler> getRespHandler() { return _jobDescription->respHandler(); }
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

    bool cancel(bool superfluous = false);
    bool isQueryCancelled();

    Executive::Ptr getExecutive() { return _executive.lock(); }

    std::shared_ptr<QdispPool> getQdispPool() { return _qdispPool; }

    friend std::ostream& operator<<(std::ostream& os, JobQuery const& jq);

    /// Make a copy of the job description. JobQuery::_setup() must be called after creation.
    /// Do not call this directly, use create.
    JobQuery(Executive::Ptr const& executive, JobDescription::Ptr const& jobDescription,
             JobStatus::Ptr const& jobStatus, std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc,
             QueryId qid);

    bool isCancelled() { return _cancelled; }

protected:
    void _setup() { _jobDescription->respHandler()->setJobQuery(shared_from_this()); }

    int _getRunAttemptsCount() const {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _jobDescription->getAttemptCount();
    }
    int _getMaxAttempts() const { return 5; }  // Arbitrary value until solid value with reason determined.
    int _getAttemptSleepSeconds() const { return 30; }  // As above or until added to config file.

    // Values that don't change once set.
    std::weak_ptr<Executive> _executive;
    /// The job description needs to survive until the task is complete.
    JobDescription::Ptr _jobDescription;
    std::shared_ptr<MarkCompleteFunc> _markCompleteFunc;

    // JobStatus has its own mutex.
    JobStatus::Ptr _jobStatus;  ///< Points at status in Executive::_statusMap

    QueryId const _qid;        // User query id
    std::string const _idStr;  ///< Identifier string for logging.

    // Values that need mutex protection
    mutable std::recursive_mutex _rmutex;  ///< protects _jobDescription,
                                           ///< _queryRequestPtr, and _inSsi

    // SSI items
    std::shared_ptr<QueryRequest> _queryRequestPtr;
    bool _inSsi{false};

    // Cancellation
    std::atomic<bool> _cancelled{false};  ///< Lock to make sure cancel() is only called once.

    std::shared_ptr<QdispPool> _qdispPool;

    // Remove or disable these members after debugging.
    util::InstanceCount _instC{"JobQuery"};
};

}  // namespace lsst::qserv::qdisp

#endif /* LSST_QSERV_QDISP_JOBQUERY_H_ */
