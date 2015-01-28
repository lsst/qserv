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

#ifndef LSST_QSERV_XRDFS_MYSQLFSDIRECTORY_H
#define LSST_QSERV_XRDFS_MYSQLFSDIRECTORY_H

// Third-party include
#include "boost/shared_ptr.hpp"
#include "XrdSfs/XrdSfsInterface.hh"

// Local headers
#include "lsst/log/Log.h"

// Forward declarations
class XrdSysError;
// End of forward declarations


namespace lsst {
namespace qserv {
namespace xrdfs {

/// MySqlFsDirectory is directory object returned by MySqlFs. It
/// rejects directory operations because they have not been assigned
/// any meaning in qserv.
class MySqlFsDirectory : public XrdSfsDirectory {
public:
    MySqlFsDirectory(LOG_LOGGER const& log, char* user = 0);
    ~MySqlFsDirectory(void);

    int open(char const* dirName, XrdSecEntity const* client = 0,
             char const* opaque = 0);
    char const* nextEntry(void);
    int close(void);
    char const* FName(void);

private:
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::xrdfs

#endif // LSST_QSERV_XRDFS_MYSQLFSDIRECTORY_H
