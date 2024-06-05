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
//&&&#include "qdisp/Executive.h"
#include "qdisp/JobBase.h"

// This header declarations
namespace lsst::qserv::qdisp {

class JobQuery;

class QueryRequest;

class UberJob : public JobBase {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static uint32_t getMagicNumber() { return 93452; }

    static Ptr create(std::shared_ptr<Executive> const& executive, std::shared_ptr<ResponseHandler> const& respHandler,
                      int queryId, int uberJobId, qmeta::CzarId czarId,
                      czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData);

    UberJob() = delete;
    UberJob(UberJob const&) = delete;
    UberJob& operator=(UberJob const&) = delete;

    virtual ~UberJob(){};

    static int getFirstIdNumber() { return 9'000'000; }

    bool addJob(std::shared_ptr<JobQuery> const& job);
    bool runUberJob();

    QueryId getQueryId() const override { return _queryId; }  // TODO:UJ relocate to JobBase
    UberJobId getJobId() const override { return _uberJobId; } // &&&uj change name
    std::string const& getIdStr() const override { return _idStr; }
    std::shared_ptr<QdispPool> getQdispPool() override { return _qdispPool; }  // TODO:UJ relocate to JobBase
    std::string const& getPayload() const override { return _payload; }
    std::shared_ptr<ResponseHandler> getRespHandler() override { return _respHandler; }
    std::shared_ptr<JobStatus> getStatus() override { return _jobStatus; }  // TODO:UJ relocate to JobBase
    bool getScanInteractive() const override { return false; }  ///< UberJobs are never interactive.
    bool isQueryCancelled() override;                           // TODO:UJ relocate to JobBase
    void callMarkCompleteFunc(bool success) override;  ///< call markComplete for all jobs in this UberJob.
    std::shared_ptr<Executive> getExecutive() override { return _executive.lock(); }

    void setQueryRequest(std::shared_ptr<QueryRequest> const& qr) override {
        /* &&&
        std::lock_guard<std::mutex> lock(_qrMtx);
        _queryRequestPtr = qr;
        }
        */
        ; // Do nothing as QueryRequest is only needed for xrootd.
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

    /// &&&uj doc
    nlohmann::json importResultFile(std::string const& fileUrl, uint64_t rowCount, uint64_t fileSize);

    std::ostream& dumpOS(std::ostream& os) const override;

private:
    UberJob(std::shared_ptr<Executive> const& executive, std::shared_ptr<ResponseHandler> const& respHandler, int queryId,
            int uberJobId, qmeta::CzarId czarId, czar::CzarChunkMap::WorkerChunksData::Ptr const& workerData);

    /// Used to setup elements that can't be done in the constructor.
    void _setup();

    /// &&&uj doc
    nlohmann::json _errorFinish(bool shouldCancel);

    /// &&&uj doc
    nlohmann::json _finish(uint64_t resultRows);


    std::vector<std::shared_ptr<JobQuery>> _jobs;  //&&&uj
    std::mutex _jobsMtx; ///< Protects _jobs
    std::atomic<bool> _started{false};
    bool _inSsi = false;
    std::shared_ptr<JobStatus> _jobStatus; // &&&uj uber jobstatus probably needs to different than jobstatus.

    //&&& std::shared_ptr<QueryRequest> _queryRequestPtr;
    std::mutex _qrMtx;

    std::string _payload;  ///< XrdSsi message to be sent to the _workerResource. //&&&uj remove when possible

    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    QueryId const _queryId;
    UberJobId const _uberJobId;
    qmeta::CzarId _czarId;

    std::string const _idStr;
    std::shared_ptr<QdispPool> _qdispPool;  //&&&uj needed?

    // &&&uj
    czar::CzarChunkMap::WorkerChunksData::Ptr _workerData;  // &&& check if this is needed

    // &&&uj
    czar::CzarRegistry::WorkerContactInfo::Ptr _wContactInfo;
};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_UBERJOB_H
