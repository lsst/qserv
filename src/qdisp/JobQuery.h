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
#include "qdisp/JobBase.h"
#include "qdisp/JobDescription.h"
#include "qdisp/ResponseHandler.h"
#include "util/InstanceCount.h"

namespace lsst::qserv::qdisp {

class QdispPool;
class QueryRequest;

/// This class is used to describe, monitor, and control a single query to a worker.
/// TODO:UJ once all Jobs are sent out as UberJobs, the purpose of this class is a bit
///       vague. It's components should probably be split between UberJob and
///       JobDescription.
class JobQuery : public JobBase {
public:
    typedef std::shared_ptr<JobQuery> Ptr;

    /// Factory function to make certain a shared_ptr is used and _setup is called.
    static JobQuery::Ptr create(Executive::Ptr const& executive, JobDescription::Ptr const& jobDescription,
                                qmeta::JobStatus::Ptr const& jobStatus,
                                std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc, QueryId qid) {
        Ptr jq = Ptr(new JobQuery(executive, jobDescription, jobStatus, markCompleteFunc, qid));
        jq->_setup();
        return jq;
    }

    virtual ~JobQuery();

    QueryId getQueryId() const override { return _qid; }
    JobId getJobId() const override { return _jobDescription->id(); }
    std::string const& getPayload() const override;
    std::string const& getIdStr() const override { return _idStr; }
    std::shared_ptr<ResponseHandler> getRespHandler() override { return _jobDescription->respHandler(); }
    bool getScanInteractive() const override { return _jobDescription->getScanInteractive(); }
    JobDescription::Ptr getDescription() { return _jobDescription; }

    qmeta::JobStatus::Ptr getStatus() override { return _jobStatus; }

    void callMarkCompleteFunc(bool success) override;

    bool cancel(bool superfluous = false);
    bool isQueryCancelled() override;

    std::shared_ptr<Executive> getExecutive() override { return _executive.lock(); }

    std::shared_ptr<QdispPool> getQdispPool() override { return _qdispPool; }

    std::ostream& dumpOS(std::ostream& os) const override;

    /// Make a copy of the job description. JobQuery::_setup() must be called after creation.
    /// Do not call this directly, use create.
    JobQuery(Executive::Ptr const& executive, JobDescription::Ptr const& jobDescription,
             qmeta::JobStatus::Ptr const& jobStatus,
             std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc, QueryId qid);

    /// If the UberJob is unassigned, change the _uberJobId to ujId.
    bool setUberJobId(UberJobId ujId) {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _setUberJobId(ujId);
    }

    UberJobId getUberJobId() const {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _getUberJobId();
    }

    bool isInUberJob() const {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        return _isInUberJob();
    }

    int getAttemptCount() const;

    /// If ujId is the current owner, clear ownership.
    /// @return true if job is unassigned.
    bool unassignFromUberJob(UberJobId ujId);

protected:
    void _setup() {
        JobBase::Ptr jbPtr = shared_from_this();
        _jobDescription->respHandler()->setJobQuery(jbPtr);
    }

    /// @return true if _uberJobId was set, it can only be set if it is unassigned
    ///         or by the current owner.
    /// NOTE: _rmutex must be held before calling this
    bool _setUberJobId(UberJobId ujId);

    /// NOTE: _rmutex must be held before calling this
    UberJobId _getUberJobId() const { return _uberJobId; }

    /// NOTE: _rmutex must be held before calling this
    bool _isInUberJob() const { return _uberJobId >= 0; }

    // Values that don't change once set.
    std::weak_ptr<Executive> _executive;
    /// The job description needs to survive until the task is complete.
    JobDescription::Ptr _jobDescription;
    std::shared_ptr<MarkCompleteFunc> _markCompleteFunc;

    // JobStatus has its own mutex.
    qmeta::JobStatus::Ptr _jobStatus;  ///< Points at status in Executive::_statusMap

    QueryId const _qid;        // User query id
    std::string const _idStr;  ///< Identifier string for logging.

    // Values that need mutex protection
    // TODO:UJ  recursive can probably go away with as well as _inSsi.
    mutable std::recursive_mutex _rmutex;  ///< protects _jobDescription,
                                           ///< _queryRequestPtr, _uberJobId,
                                           ///< and _inSsi

    // Cancellation
    std::atomic<bool> _cancelled{false};  ///< Lock to make sure cancel() is only called once.

    std::shared_ptr<QdispPool> _qdispPool;

    /// The UberJobId that this job is assigned to. Values less than zero
    /// indicate this job is unassigned. To prevent race conditions,
    /// an UberJob may only unassign a job if it has the same ID as
    /// _uberJobId.
    /// All jobs must be unassigned before they can be reassigned.
    UberJobId _uberJobId = -1;
};

}  // namespace lsst::qserv::qdisp

#endif /* LSST_QSERV_QDISP_JOBQUERY_H_ */
