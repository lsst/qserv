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

// This header declarations
namespace lsst {
namespace qserv {
namespace qdisp {


class UberJob : public JobBase, std::enable_shared_from_this<UberJob> {
public:
    using Ptr = std::shared_ptr<UberJob>;

    static Ptr create(Executive::Ptr const& executive,
                      std::shared_ptr<ResponseHandler> const& respHandler);
    UberJob() = delete;
    UberJob(UberJob const&) = delete;
    UberJob& operator=(UberJob const&) = delete;

    virtual ~UberJob() {};

    bool addJob(JobQuery* job);
    void runUberJob();
    std::string getIdStr() { return _idStr; }

    /// &&& TODO: may not need,
    void prepScrubResults();

    std::string workerResource; // TODO: private


    virtual std::ostream& dump(std::ostream &os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream &out, UberJob const& uberJob);

private:
    UberJob(Executive::Ptr const& executive,
            std::shared_ptr<ResponseHandler> const& respHandler,
            int queryId, int uberJobId);

    std::vector<JobQuery*> _jobs;
    std::atomic<bool> _started{false};
    bool _inSsi = false;
    JobStatus::Ptr _jobStatus;


    std::weak_ptr<Executive> _executive;
    std::shared_ptr<ResponseHandler> _respHandler;
    int const _queryId;
    int const _uberJobId;
    std::string const _idStr;
};


}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_UBERJOB_H
