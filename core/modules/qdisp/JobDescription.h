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

// Qserv headers
#include "global/intTypes.h"
#include "global/ResourceUnit.h"

// Forward declarations

namespace lsst {
namespace qserv {

namespace qproc {

class ChunkQuerySpec;
class TaskMsgFactory;

} // namespace qproc


namespace qdisp {

class ResponseHandler;

/** Description of a job managed by the executive
 */
class JobDescription {
public:
    JobDescription(QueryId qId, int jobId, ResourceUnit const& resource,
        std::shared_ptr<ResponseHandler> const& respHandler,
        std::shared_ptr<qproc::TaskMsgFactory> const& taskMsgFactory,
        std::shared_ptr<qproc::ChunkQuerySpec> const& chunkQuerySpec,
        std::string const& chunkResultName);

    /// &&& only used for testing.  Keep, or remove and change tests ???
    JobDescription(QueryId qId,int jobId, ResourceUnit const& resource, std::string const& payload,
        std::shared_ptr<ResponseHandler> const& respHandler)
        : _queryId(qId), _jobId(jobId), _resource(resource), _payload(payload), _respHandler(respHandler) {}

    int id() const { return _jobId; } // &&& change to jobId()
    ResourceUnit const& resource() const { return _resource; }
    std::string const& payload() const { return _payload; }
    std::shared_ptr<ResponseHandler> respHandler() { return _respHandler; }
    friend std::ostream& operator<<(std::ostream& os, JobDescription const& jd);
private:
    void _setPayload();

    QueryId _queryId;
    int _jobId; // Job's Id number.
    std::string const _qIdStr;
    int _retryCount{0};
    ResourceUnit _resource; // path, e.g. /q/LSST/23125
    std::string _payload; // encoded request
    std::shared_ptr<ResponseHandler> _respHandler; // probably MergingHandler
    std::shared_ptr<qproc::TaskMsgFactory> _taskMsgFactory;
    std::shared_ptr<qproc::ChunkQuerySpec> _chunkQuerySpec;
    std::string _chunkResultName;
};
std::ostream& operator<<(std::ostream& os, JobDescription const& jd);

}}} // end namespace

#endif /* LSST_QSERV_QDISP_JOBDESCRIPTION_H_ */
