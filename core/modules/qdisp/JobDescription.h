/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include "global/ResourceUnit.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace qdisp {

class ResponseHandler;

/** Description of a job managed by the executive
 */
class JobDescription {
public:
    JobDescription(int id, ResourceUnit const& resource, std::string const& payload,
        std::shared_ptr<ResponseHandler> const& respHandler)
        : _id{id}, _resource{resource}, _payload{payload}, _respHandler{respHandler} {};

    int id() const { return _id; }
    ResourceUnit const& resource() const { return _resource; }
    std::string const& payload() const { return _payload; }
    std::shared_ptr<ResponseHandler> respHandler() { return _respHandler; }
    std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, JobDescription const& jd);
private:
    int _id; // Job's Id number.
    ResourceUnit _resource; // path, e.g. /q/LSST/23125
    std::string _payload; // encoded request
    std::shared_ptr<ResponseHandler> _respHandler; // probably MergingHandler
};
std::ostream& operator<<(std::ostream& os, JobDescription const& jd);

}}} // end namespace

#endif /* LSST_QSERV_QDISP_JOBDESCRIPTION_H_ */
