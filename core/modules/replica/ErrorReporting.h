/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_ERROR_REPORTING_H
#define LSST_QSERV_REPLICA_ERROR_REPORTING_H

/// CmdParser.h declares:
///
/// function reportRequestState
/// (see individual class documentation for more information)

// System headers
#include <iomanip>
#include <ostream>

// Qserv headers
#include "replica/Request.h"
#include "replica/ServiceManagementRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Print a report on a state of requests
 *
 * @param requests - an iterable collection of requests
 * @param os       - an output stream
 */
template <class COLLECTION>
void reportRequestState (COLLECTION const& requests,
                         std::ostream& os) {
    os  << "\n"
        << "REQUESTS:\n"
        << "--------------------------------------+----------------------+--------+-------------+----------------------+--------------------------\n"
        << "                                   id |                 type | worker |       state |            ext.state |          server err.code \n"
        << "--------------------------------------+----------------------+--------+-------------+----------------------+--------------------------\n";   
    for (const auto &ptr: requests) {
        os  << " "   << std::setw(36) <<                                ptr->id()
            << " | " << std::setw(20) <<                                ptr->type()
            << " | " << std::setw( 6) <<                                ptr->worker()
            << " | " << std::setw(11) << replica::Request::state2string(ptr->state())
            << " | " << std::setw(20) << replica::Request::state2string(ptr->extendedState())
            << " | " << std::setw(24) << replica::status2string(        ptr->extendedServerStatus())
            << "\n";
    }
    os  << "--------------------------------------+----------------------+--------+-------------+----------------------+--------------------------\n"
        << std::endl;
}

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_ERROR_REPORTING_H