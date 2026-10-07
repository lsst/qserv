/*
 * This file is part of qserv.
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_QDISP_UBERJOB_H
#define LSST_QSERV_QDISP_UBERJOB_H

// System headers
#include <atomic>

// Qserv headers
#include "czar/CzarChunkMap.h"
#include "czar/CzarRegistry.h"
#include "global/UberJobBase.h"
#include "qdisp/CzarStats.h"
#include "qdisp/Executive.h"
#include "qmeta/JobStatus.h"
#include "util/MultiError.h"

namespace lsst::qserv::protojson {
class FileUrlInfo;
}

namespace lsst::qserv::util {
class QdispPool;
}

namespace lsst::qserv::qdisp {

class JobQuery;

class UJState {
public:
    using Ptr = std::shared_ptr<UJState>;
    // objects of this class are created by derived classes.
    virtual ~UJState() = default;
    std::string const name;
    CzarStats::RaiiC::RaiiPtr const raiiPtr;

protected:
    UJState(std::string const& name_, CzarStats::RaiiC::Ptr const& raiiCounter_)
            : name(name_), raiiPtr(raiiCounter_->createRaii()) {}
    UJState(UJState const&) = delete;
    UJState() = delete;
    UJState& operator=(UJState const&) = delete;
};

class UJStateCreated : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateCreated>;
    static Ptr create() { return Ptr(new UJStateCreated()); }
    ~UJStateCreated() override = default;

private:
    UJStateCreated() : UJState("Created", CzarStats::get()->getNumCreatedUberJobs()) {}
};

class UJStateRequest : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateRequest>;
    static Ptr create() { return Ptr(new UJStateRequest()); }
    ~UJStateRequest() override = default;

private:
    UJStateRequest() : UJState("Request", CzarStats::get()->getNumRequestUberJobs()) {}
};

class UJStateResponseReady : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateResponseReady>;
    static Ptr create() { return Ptr(new UJStateResponseReady()); }
    ~UJStateResponseReady() override = default;

private:
    UJStateResponseReady() : UJState("ResponseReady", CzarStats::get()->getNumResponseReadyUberJobs()) {}
};

class UJStateResponseError : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateResponseError>;
    static Ptr create() { return Ptr(new UJStateResponseError()); }
    ~UJStateResponseError() override = default;

private:
    UJStateResponseError() : UJState("ResponseError", CzarStats::get()->getNumResponseErrorUberJobs()) {}
};

class UJStateDone : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateDone>;
    static Ptr create() { return Ptr(new UJStateDone()); }
    ~UJStateDone() override = default;

private:
    UJStateDone() : UJState("Done", CzarStats::get()->getNumDoneUberJobs()) {}
};

class UJStateCancelled : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateCancelled>;
    static Ptr create() { return Ptr(new UJStateCancelled()); }
    ~UJStateCancelled() override = default;

private:
    UJStateCancelled() : UJState("Cancelled", CzarStats::get()->getNumCancelledUberJobs()) {}
};

class UJStateComplete : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateComplete>;
    static Ptr create() { return Ptr(new UJStateComplete()); }
    ~UJStateComplete() override = default;

private:
    UJStateComplete() : UJState("Complete", CzarStats::get()->getNumCompleteUberJobs()) {}
};

class UJStateUnexpected : public UJState {
public:
    using Ptr = std::shared_ptr<UJStateUnexpected>;
    static Ptr create() { return Ptr(new UJStateUnexpected()); }
    ~UJStateUnexpected() override = default;

private:
    UJStateUnexpected() : UJState("Unexpected", CzarStats::get()->getNumUnexpectedUberJobs()) {}
};

/** This class contains state information for an UberJob.
 * The JobStatus values are expected to always advance in the order
 * of REQUEST, RESPONSE_READY, {RESPONSE_DONE or CANCEL}, COMPLETE.
 * Trying to change to an earlier state are ignored as are attempts
 * to change to the existing state. An UberJob that has state
 * RESPONSE_DONE cannot be changed to CANCEL, and the reverse is true.
 */
class UberJobStatus : public qmeta::JobStatus {
public:
    using Ptr = std::shared_ptr<UberJobStatus>;
    UberJobStatus() : qmeta::JobStatus() {}

    virtual ~UberJobStatus() = default;

    void updateInfo(std::string const& idMsg, State s, std::string const& source, int code,
                    std::string const& desc, MessageSeverity severity) override;

private:
    UJState::Ptr _ujState{UJStateCreated::create()};  ///< Current state of this UberJob.
};

/// This class is a contains x number of jobs that need to go to the same worker
/// from a single user query, and contact information for the worker. It also holds
/// some information common to all jobs.
/// The UberJob constructs the message to send to the worker and handles collecting
/// and merging the results.
/// When this UberJobCompletes, all the Jobs it contains are registered as completed.
/// If this UberJob fails, it will be destroyed, un-assigning all of its Jobs.
/// Those Jobs will need to be reassigned to new UberJobs.
class UberJob : public UberJobBase {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static Ptr create(std::shared_ptr<Executive> const& executive,
                      std::shared_ptr<ResponseHandler> const& respHandler, int uberJobId, CzarId czarId,
                      std::shared_ptr<protojson::WorkerContactInfo> const& workerContactInfo,
                      TIMEPOINT familyMapTimestamp);

    UberJob() = delete;
    UberJob(UberJob const&) = delete;
    UberJob& operator=(UberJob const&) = delete;

    virtual ~UberJob();

    std::string cName(const char* funcN) const override {
        return std::string("UberJob::") + funcN + " " + getIdStr();
    }

    bool addJob(std::shared_ptr<JobQuery> const& job);

    /// Make a json version of this UberJob and send it to its worker.
    virtual void runUberJob();

    /// Kill this UberJob and unassign all Jobs so they can be used in a new UberJob if needed.
    /// @return true if the UberJob results were stopped from merging. False means
    ///         the results for this UberJob were already being merged or were merged before
    ///         killUberJob was called.
    /// Note that returning false means merging either already finished or merging has already
    /// started and there's no way to stop it without corrupting the results.
    bool killUberJob();

    std::shared_ptr<ResponseHandler> getRespHandler() { return _respHandler; }
    std::shared_ptr<qmeta::JobStatus> getStatus() { return _jobStatus; }

    void callMarkCompleteFunc(bool success);  ///< call markComplete for all jobs in this UberJob.
    std::shared_ptr<Executive> getExecutive() { return _executive.lock(); }

    /// Return false if not ok to set the status to newState, otherwise set the state for
    /// this UberJob and all jobs it contains to newState.
    /// This is used both to set status and prevent the system from repeating operations
    /// that have already happened. If it returns false, the thread calling this
    /// should stop processing.
    bool setStatusIfOk(qmeta::JobStatus::State newState, std::string const& msg) {
        VLOCK(jobLock, _jobsMtx);
        return _setStatusIfOk(newState, msg);
    }

    int getJobCount() const {
        VLOCK(jLck, _jobsMtx);
        return _jobs.size();
    }

    /// Set the worker information needed to send messages to the worker believed to
    /// be responsible for the chunks handled in this UberJob.
    void setWorkerContactInfo(protojson::WorkerContactInfo::Ptr const& wContactInfo) {
        _wContactInfo = wContactInfo;
    }

    protojson::WorkerContactInfo::Ptr getWorkerContactInfo() { return _wContactInfo; }

    /// Queue the lambda function to collect and merge the results from the worker.
    /// @return a json message indicating success unless the query has been
    ///         cancelled, limit row complete, or similar.
    protojson::ExecutiveRespMsg::Ptr importResultFile(protojson::FileUrlInfo const& fileUrlInfo_);

    /// Handle an error from the worker.
    void workerError(util::MultiError const& multiErr_, protojson::ExecutiveRespMsg& execRespMsg);

    void setResultFileSize(uint64_t fileSize) { _resultFileSize = fileSize; }
    uint64_t getResultFileSize() { return _resultFileSize; }

    /// Update UberJob status, return true if successful.
    bool importResultFinish();

    void setContaminated();

    bool getContaminated() const { return _contaminated; };

    /// Import and error from trying to collect results.
    protojson::ExecutiveRespMsg::Ptr importResultError(bool shouldCancel, std::string const& errorType,
                                                       std::string const& note);

    std::ostream& dump(std::ostream& os) const override;

protected:
    UberJob(std::shared_ptr<Executive> const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
            UberJobId uberJobId_, CzarId czarId_,
            std::shared_ptr<protojson::WorkerContactInfo> const& workerContactInfo,
            TIMEPOINT familyMapTimestamp);

private:
    /// @see setStatusIfOk
    /// note: _jobsMtx must be locked before calling.
    bool _setStatusIfOk(UberJobStatus::State newState, std::string const& msg);

    /// unassign all Jobs in this UberJob and set the Executive flag to indicate that Jobs need
    /// reassignment. The list of _jobs is cleared, so multiple calls of this should be harmless.
    void _unassignJobs();

    /// Let the Executive know about errors while handling results.
    void _workerErrorFinish(protojson::ExecutiveRespMsg& execRespMsg,
                            std::string const& errorType = std::string(),
                            std::string const& note = std::string());

    std::vector<std::shared_ptr<JobQuery>> _jobs;  ///< List of Jobs in this UberJob.
    mutable VMUTEX _jobsMtx;                       ///< Protects _jobs, _jobStatus
    std::atomic<bool> _started{false};
    UberJobStatus::Ptr _jobStatus{new UberJobStatus()};

    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    int const _rowLimit;  ///< Number of rows in the query LIMIT clause.
    uint64_t _resultFileSize = 0;

    /// Contact information for the target worker.
    protojson::WorkerContactInfo::Ptr _wContactInfo;

    /// true if the result may have been contaminated during merging.
    std::atomic<bool> _contaminated{false};

    /// Timestamp of the family map used to create this UberJob.
    TIMEPOINT _familyMapTimestamp;

    /// RAII object to increment (and eventually decrement) the total number of UberJobs in CzarStats.
    CzarStats::RaiiC::RaiiPtr const _numTotal{CzarStats::get()->getNumTotalUberJobs()->createRaii()};
};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_UBERJOB_H
