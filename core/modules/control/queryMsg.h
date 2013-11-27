// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
/// queryMsg.h declares an interface for the query messaging mechanism for exporting
/// via SWIG to the python layer of Qserv.

#ifndef LSST_QSERV_CONTROL_QUERYMSG_H
#define LSST_QSERV_CONTROL_QUERYMSG_H
#include <string>

namespace lsst {
namespace qserv {
namespace control {

int queryMsgGetCount(int session);

// Python call: msg, chunkId, code, timestamp = queryMsgGetMsg(session, idx)
// int* chunkId, int* code, time_t* timestamp matches with %apply directive to help SWIG
std::string queryMsgGetMsg(int session, int idx, int* chunkId, int* code, time_t* timestamp);

void queryMsgAddMsg(int session, int chunkId, int code, std::string const& message);

}}} // namespace lsst::qserv::control

#endif // LSST_QSERV_CONTROL_QUERYMSG_H

