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
#include "qdisp/Executive.h"
#include "qdisp/JobBase.h"
#include "qdisp/JobQuery.h"
#include "qdisp/QueryRequest.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace qdisp {

class UberJob : public JobBase {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static Ptr create(Executive::Ptr const& executive,
                      std::shared_ptr<ResponseHandler> const& respHandler,
                      int queryId, int uberJobId);
    UberJob() = delete;
    UberJob(UberJob const&) = delete;
    UberJob& operator=(UberJob const&) = delete;

    virtual ~UberJob() {};

    static int getFirstIdNumber() { return 9'000'000; }

    bool addJob(JobQuery* job);
    bool runUberJob();

    QueryId getQueryId() const override { return _queryId; } //TODO:UJ relocate to JobBase
    int getIdInt() const override { return _uberJobId; }
    std::string const& getIdStr() const override { return _idStr; }
    std::shared_ptr<QdispPool> getQdispPool() override { return _qdispPool; } //TODO:UJ relocate to JobBase
    std::string const& getPayload() const override { return _payload; }
    std::shared_ptr<ResponseHandler> getRespHandler() override { return _respHandler; }
    std::shared_ptr<JobStatus> getStatus() override { return _jobStatus; } //TODO:UJ relocate to JobBase
    bool getScanInteractive() const override { return false; } ///< UberJobs are never interactive.
    bool isQueryCancelled() override; //TODO:UJ relocate to JobBase
    void callMarkCompleteFunc(bool success) override;

    void setQueryRequest(std::shared_ptr<QueryRequest> const& qr) override {
        std::lock_guard<std::mutex> lock(_qrMtx);
        _queryRequestPtr = qr;
    }

    bool verifyPayload() const;

    /// &&& TODO:UJ may not need,
    void prepScrubResults();

    std::string workerResource; // TODO:UJ make private

    std::ostream& dumpOS(std::ostream &os) const override;

private:
    UberJob(Executive::Ptr const& executive,
            std::shared_ptr<ResponseHandler> const& respHandler,
            int queryId, int uberJobId);

    std::vector<JobQuery*> _jobs;
    std::atomic<bool> _started{false};
    bool _inSsi = false;
    JobStatus::Ptr _jobStatus;

    std::shared_ptr<QueryRequest> _queryRequestPtr;
    std::mutex _qrMtx;

    std::string _payload{"&&& NEED_REAL_PAYLOAD"};

    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    int const _queryId;
    int const _uberJobId;
    std::string const _idStr;
    std::shared_ptr<QdispPool> _qdispPool;
};


}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_UBERJOB_H
