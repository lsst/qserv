/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_QDISP_UBERJOB_H
#define LSST_QSERV_QDISP_UBERJOB_H

// System headers

// Qserv headers
#include "czar/CzarChunkMap.h"
#include "czar/CzarRegistry.h"
#include "global/UberJobBase.h"
#include "qdisp/Executive.h"
#include "qmeta/JobStatus.h"

namespace lsst::qserv::protojson {
class FileUrlInfo;
}

namespace lsst::qserv::util {
class QdispPool;
}

namespace lsst::qserv::qdisp {

class JobQuery;

/// This class is a contains x number of jobs that need to go to the same worker
/// from a single user query, and contact information for the worker. It also holds
/// some information common to all jobs.
/// The UberJob constructs the message to send to the worker and handles collecting
/// and merging the results.
/// When this UberJobCompletes, all the Jobs it contains are registered as completed.
/// If this UberJob fails, it will be destroyed, un-assigning all of its Jobs.
/// Those Jobs will need to be reassigned to new UberJobs, or the query cancelled.
class UberJob : public UberJobBase {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static Ptr create(std::shared_ptr<Executive> const& executive,
                      std::shared_ptr<ResponseHandler> const& respHandler, int queryId, int uberJobId,
                      CzarId czarId);

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
        std::lock_guard<std::mutex> jobLock(_jobsMtx);
        return _setStatusIfOk(newState, msg);
    }

    int getJobCount() const { return _jobs.size(); }

    /// Set the worker information needed to send messages to the worker believed to
    /// be responsible for the chunks handled in this UberJob.
    void setWorkerContactInfo(protojson::WorkerContactInfo::Ptr const& wContactInfo) {
        _wContactInfo = wContactInfo;
    }

    protojson::WorkerContactInfo::Ptr getWorkerContactInfo() { return _wContactInfo; }

    /// Queue the lambda function to collect and merge the results from the worker.
    /// @param retry - true indicates this is a retry of failed communication
    ///            and should not be used to kill this UberJob due to an unexpected
    ///            state.
    /// @return a json message indicating success unless the query has been
    ///         cancelled, limit row complete, or similar.
    nlohmann::json importResultFile(protojson::FileUrlInfo const& fileUrlInfo_, bool const retry = false);

    /// Handle an error from the worker.
    nlohmann::json workerError(int errorCode, std::string const& errorMsg);

    void setResultFileSize(uint64_t fileSize) { _resultFileSize = fileSize; }
    uint64_t getResultFileSize() { return _resultFileSize; }

    /// Update UberJob status, return true if successful.
    bool importResultFinish();

    /// Import and error from trying to collect results.
    nlohmann::json importResultError(bool shouldCancel, std::string const& errorType,
                                     std::string const& note);

    std::ostream& dump(std::ostream& os) const override;

protected:
    UberJob(std::shared_ptr<Executive> const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
            QueryId queryId_, UberJobId uberJobId_, CzarId czarId_, int rowLimit);

private:
    /// Used to setup elements that can't be done in the constructor.
    void _setup();

    /// @see setStatusIfOk
    /// note: _jobsMtx must be locked before calling.
    bool _setStatusIfOk(qmeta::JobStatus::State newState, std::string const& msg);

    /// unassign all Jobs in this UberJob and set the Executive flag to indicate that Jobs need
    /// reassignment. The list of _jobs is cleared, so multiple calls of this should be harmless.
    void _unassignJobs();

    /// Let the Executive know about errors while handling results.
    nlohmann::json _workerErrorFinish(bool successful, std::string const& errorType = std::string(),
                                      std::string const& note = std::string());

    std::vector<std::shared_ptr<JobQuery>> _jobs;  ///< List of Jobs in this UberJob.
    mutable std::mutex _jobsMtx;                   ///< Protects _jobs, _jobStatus
    std::atomic<bool> _started{false};
    qmeta::JobStatus::Ptr _jobStatus{new qmeta::JobStatus()};

    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    int const _rowLimit;  ///< Number of rows in the query LIMIT clause.
    uint64_t _resultFileSize = 0;

    // Contact information for the target worker.
    protojson::WorkerContactInfo::Ptr _wContactInfo;
};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_UBERJOB_H
