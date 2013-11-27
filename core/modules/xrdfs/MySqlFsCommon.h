/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

#ifndef LSST_QSERV_XRDFS_MYSQLFSCOMMON_H
#define LSST_QSERV_XRDFS_MYSQLFSCOMMON_H

#include <string>
#include <boost/shared_ptr.hpp>

// Forward declarations
class XrdSysError;
namespace lsst {
namespace qserv {
namespace obsolete {
    class QservPath;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace xrdfs {
        
enum FileClass {COMBO, TWO_WRITE, TWO_READ, UNKNOWN};

// Xrootd file path functionality
FileClass computeFileClass(std::string const& filename);
std::string stripPath(std::string const& filename);

class FileValidator {
public:
    typedef boost::shared_ptr<FileValidator> Ptr;
    virtual ~FileValidator() {}
    virtual bool operator()(std::string const& filename) = 0;
};

class PathValidator {
public:
    typedef boost::shared_ptr<PathValidator> Ptr;
    virtual ~PathValidator() {}
    virtual bool operator()(obsolete::QservPath const& qp) = 0;
};

}}} // namespace lsst::qserv:xrdfs

#endif // LSST_QSERV_XRDFS_MYSQLFSCOMMON_H
