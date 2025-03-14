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
#include "qmeta/types.h"
#include "czar/CzarChunkMap.h"  // Need nested class. &&&uj Make non-nested?
#include "czar/CzarRegistry.h"  // Need nested class. &&&uj Make non-nested?
#include "qdisp/Executive.h"
#include "qdisp/JobBase.h"

// This header declarations
namespace lsst { namespace qserv { namespace qdisp {

class QueryRequest;

class UberJob : public JobBase {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static uint32_t getMagicNumber() { return 93452; }

    static Ptr create(Executive::Ptr const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
                      int queryId, int uberJobId, qmeta::CzarId czarId,
                      czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData);

    UberJob() = delete;
    UberJob(UberJob const&) = delete;
    UberJob& operator=(UberJob const&) = delete;

    virtual ~UberJob(){};

    static int getFirstIdNumber() { return 9'000'000; }

    bool addJob(JobQuery* job);
    bool runUberJob();

    QueryId getQueryId() const override { return _queryId; }  // TODO:UJ relocate to JobBase
    int getIdInt() const override { return _uberJobId; }
    std::string const& getIdStr() const override { return _idStr; }
    std::shared_ptr<QdispPool> getQdispPool() override { return _qdispPool; }  // TODO:UJ relocate to JobBase
    std::string const& getPayload() const override { return _payload; }
    std::shared_ptr<ResponseHandler> getRespHandler() override { return _respHandler; }
    std::shared_ptr<JobStatus> getStatus() override { return _jobStatus; }  // TODO:UJ relocate to JobBase
    bool getScanInteractive() const override { return false; }  ///< UberJobs are never interactive.
    bool isQueryCancelled() override;                           // TODO:UJ relocate to JobBase
    void callMarkCompleteFunc(bool success) override;  ///< call markComplete for all jobs in this UberJob.

    void setQueryRequest(std::shared_ptr<QueryRequest> const& qr) override {
        std::lock_guard<std::mutex> lock(_qrMtx);
        _queryRequestPtr = qr;
    }

    bool verifyPayload() const;

    int getJobCount() const { return _jobs.size(); }

    /// &&&uj uj may not need,
    void prepScrubResults();

    //&&&uj
    void setWorkerContactInfo(czar::CzarRegistry::WorkerContactInfo::Ptr const& wContactInfo) {
        _wContactInfo = wContactInfo;
    }

    //&&&uj
    czar::CzarChunkMap::WorkerChunksData::Ptr getWorkerData() { return _workerData; }

    std::ostream& dumpOS(std::ostream& os) const override;

private:
    UberJob(Executive::Ptr const& executive, std::shared_ptr<ResponseHandler> const& respHandler, int queryId,
            int uberJobId, qmeta::CzarId czarId, czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData);

    void _setup() {
        JobBase::Ptr jbPtr = shared_from_this();
        _respHandler->setJobQuery(jbPtr);
    }

    std::vector<JobQuery*> _jobs;  // &&&uj should be a shared ptr ???
    std::atomic<bool> _started{false};
    bool _inSsi = false;
    JobStatus::Ptr _jobStatus;

    std::shared_ptr<QueryRequest> _queryRequestPtr;
    std::mutex _qrMtx;

    std::string _payload;  ///< XrdSsi message to be sent to the _workerResource. //&&&uj remove when possible

    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    int const _queryId;
    int const _uberJobId;
    qmeta::CzarId _czarId;

    std::string const _idStr;
    std::shared_ptr<QdispPool> _qdispPool;  //&&&uj needed?

    // &&&uj
    czar::CzarChunkMap::WorkerChunksData::Ptr _workerData;  // &&& check if this is needed

    // &&&uj
    czar::CzarRegistry::WorkerContactInfo::Ptr _wContactInfo;
};

}}}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_UBERJOB_H
