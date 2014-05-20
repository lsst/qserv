// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2013 LSST Corporation.
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
/// Implements MySqlFsDirectory, which rejects directory modification ops.

#include "xrdfs/MySqlFsDirectory.h"

// System headers
#include <errno.h>

// Third-party headers
#include "XrdSys/XrdSysError.hh"

// Local headers
#include "wlog/WLogger.h"


namespace lsst {
namespace qserv {
namespace xrdfs {


MySqlFsDirectory::MySqlFsDirectory(boost::shared_ptr<wlog::WLogger> log,
                                   char* user) :
    XrdSfsDirectory(user), _log(log) {
}

MySqlFsDirectory::~MySqlFsDirectory(void) {
}

int MySqlFsDirectory::open(
    char const* dirName, XrdSecEntity const* client,
    char const* opaque) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* MySqlFsDirectory::nextEntry(void) {
    return 0;
}

int MySqlFsDirectory::close(void) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* MySqlFsDirectory::FName(void) {
    _log->info("In MySqlFsDirectory::Fname()");
    return 0;
}

}}} // namespace lsst::qserv::xrdfs
