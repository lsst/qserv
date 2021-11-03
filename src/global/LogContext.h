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

#ifndef LSST_QSERV_LOGCONTEXT_H
#define LSST_QSERV_LOGCONTEXT_H

#include <string>
#include <tuple>

#include "lsst/log/Log.h"

#include "global/intTypes.h"

// helper macro to reduce boilerplate whe generating logging context for Query ID and Job ID
#define QSERV_LOGCONTEXT_QUERY(qid) LOG_MDC_SCOPE("QID", qid == 0 ? "" : std::to_string(qid))
#define QSERV_LOGCONTEXT_QUERY_JOB(qid, jobid) LOG_MDC_SCOPE("QID", qid == 0 ? "" : std::to_string(qid) + "#" + std::to_string(jobid))

#endif // LSST_QSERV_LOGCONTEXT_H
