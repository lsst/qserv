// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
/// msgCode.h maintains message codes used by the query messaging system
/// within the C++ layer of Qesrv.

#ifndef LSST_QSERV_LOG_MSGCODE_H
#define LSST_QSERV_LOG_MSGCODE_H

namespace lsst {
namespace qserv {
namespace log {

// Codes for C++ layer are >= 1000.
// (<1000 reserved for Python layer.)
const int MSG_MGR_ADD       = 1200;
const int MSG_XRD_OPEN_FAIL = 1290;
const int MSG_XRD_WRITE     = 1300;
const int MSG_XRD_READ      = 1400;
const int MSG_RESULT_ERROR  = 1470;
const int MSG_MERGE_ERROR   = 1480;
const int MSG_MERGED        = 1500;
const int MSG_ERASED        = 1600;
const int MSG_EXEC_SQUASHED = 1990;
const int MSG_FINALIZED     = 2000;

}}} // namespace lsst::qserv::log

#endif // LSST_QSERV_LOG_MSGCODE_H

