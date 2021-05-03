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

// Class header
#include "qdisp/UberJob.h"

// System headers
#include <stdexcept>

// Qserv headers


// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.UberJob");
}

namespace lsst {
namespace qserv {
namespace qdisp {

bool UberJob::addJob(JobQuery::Ptr const& job) {
    bool success = false;
    if (job->inUberJob()) {
        throw Bug("job already in UberJob job=" + job->dump() + " uberJob=" + dump());
    }
    _jobs.push_back(job);
    job->setInUberJob(true);
    success = true;
    return success;
}


std::ostream& UberJob::dump(std::ostream &os) const {
    os << "(workerResource=" << workerResource
       << " jobs sz=" << _jobs.size() << "(";
    for (auto const& job:_jobs) {
        JobDescription::Ptr desc = job->getDescription();
        ResourceUnit ru = desc->resource();
        os << ru.db() << ":" << ru.chunk() << ",";
    }
    os << "))";
    return os;
}


std::string UberJob::dump() const {
    std::ostringstream os;
    dump(os);
    return os.str();
}


std::ostream& operator<<(std::ostream &os, UberJob const& uberJob) {
    return uberJob.dump(os);
}


}}} //namespace lsst::qserv::qdisp

