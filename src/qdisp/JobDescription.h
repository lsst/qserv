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
 * JobDescription.h
 *
 *      Author: jgates
 */

#ifndef LSST_QSERV_QDISP_JOBDESCRIPTION_H_
#define LSST_QSERV_QDISP_JOBDESCRIPTION_H_

// System headers
#include <memory>
#include <sstream>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/constants.h"
#include "global/intTypes.h"
#include "global/ResourceUnit.h"
#include "qmeta/types.h"

// Forward declarations

namespace lsst::qserv {

namespace proto {
class TaskMsg;
}

namespace qproc {
class ChunkQuerySpec;
class TaskMsgFactory;
}  // namespace qproc

namespace qdisp {

class Executive;
class ResponseHandler;

/** Description of a job managed by the executive
 */
class JobDescription {
public:
    using Ptr = std::shared_ptr<JobDescription>;
    static JobDescription::Ptr create(qmeta::CzarId czarId, QueryId qId, JobId jobId,
                                      ResourceUnit const& resource,
                                      std::shared_ptr<ResponseHandler> const& respHandler,
                                      std::shared_ptr<qproc::TaskMsgFactory> const& taskMsgFactory,
                                      std::shared_ptr<qproc::ChunkQuerySpec> const& chunkQuerySpec,
                                      std::string const& chunkResultName, bool mock = false) {
        JobDescription::Ptr jd(new JobDescription(czarId, qId, jobId, resource, respHandler, taskMsgFactory,
                                                  chunkQuerySpec, chunkResultName, mock));
        return jd;
    }

    JobDescription(JobDescription const&) = delete;
    JobDescription& operator=(JobDescription const&) = delete;

    void buildPayload();  ///< Must be run after construction to avoid problems with unit tests.
    JobId id() const { return _jobId; }
    ResourceUnit const& resource() const { return _resource; }
    std::string const& payload() { return _payloads[_attemptCount]; }
    std::shared_ptr<ResponseHandler> respHandler() { return _respHandler; }
    int getAttemptCount() const { return _attemptCount; }

    bool getScanInteractive() const;
    int getScanRating() const;

    /// @returns true when _attemptCount is incremented correctly and the payload is built.
    /// If the starting value of _attemptCount was greater than or equal to zero, that
    /// attempt is scrubbed from the result table.
    bool incrAttemptCountScrubResults();  // TODO:UJ - to be deleted
    /// Increase the attempt count by 1 and return false if that puts it over the limit.
    /// TODO:UJ scrubbing results unneeded with uj. This should be renamed.
    bool incrAttemptCountScrubResultsJson(std::shared_ptr<Executive> const& exec, bool increase);
    bool verifyPayload() const;  ///< @return true if the payload is acceptable to protobufs.

    std::shared_ptr<nlohmann::json> getJsForWorker() { return _jsForWorker; }

    void resetJsForWorker() { _jsForWorker.reset(); }  // TODO:UJ may need mutex for _jsForWorker

    friend std::ostream& operator<<(std::ostream& os, JobDescription const& jd);

private:
    JobDescription(qmeta::CzarId czarId, QueryId qId, JobId jobId, ResourceUnit const& resource,
                   std::shared_ptr<ResponseHandler> const& respHandler,
                   std::shared_ptr<qproc::TaskMsgFactory> const& taskMsgFactory,
                   std::shared_ptr<qproc::ChunkQuerySpec> const& chunkQuerySpec,
                   std::string const& chunkResultName, bool mock = false);

    qmeta::CzarId _czarId;
    QueryId _queryId;
    JobId _jobId;  ///< Job's Id number.
    std::string const _qIdStr;
    int _attemptCount{-1};   ///< Start at -1 so that first attempt will be 0, see incrAttemptCount().
    ResourceUnit _resource;  ///< path, e.g. /q/LSST/23125

    /// _payloads - encoded requests, one per attempt. No guarantee that xrootd is done
    /// with the payload buffer, so hang onto all of them until the query is finished.
    /// Also, using a map so the strings wont be moved.
    /// The xrootd callback function QueryRequest::GetRequest should
    /// return something other than a char*.
    std::map<int, std::string> _payloads;
    std::shared_ptr<ResponseHandler> _respHandler;  // probably MergingHandler
    std::shared_ptr<qproc::TaskMsgFactory> _taskMsgFactory;
    std::shared_ptr<qproc::ChunkQuerySpec> _chunkQuerySpec;
    std::string _chunkResultName;

    bool _mock{false};  ///< True if this is a mock in a unit test.

    /// The information the worker needs to run this job. Reset once sent.
    std::shared_ptr<nlohmann::json> _jsForWorker;
};
std::ostream& operator<<(std::ostream& os, JobDescription const& jd);

}  // namespace qdisp
}  // namespace lsst::qserv

#endif /* LSST_QSERV_QDISP_JOBDESCRIPTION_H_ */
