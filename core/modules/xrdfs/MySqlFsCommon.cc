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

#include "xrdfs/MySqlFsCommon.h"

#include <string>

namespace lsst {
namespace qserv {
namespace xrdfs {

FileClass
computeFileClass(std::string const& filename) {
    if(std::string::npos != filename.find("/query2/")) {
        return TWO_WRITE;
    } else if(std::string::npos != filename.find("/result/")) {
        return TWO_READ;
    } else if(std::string::npos != filename.find("/query/")) {
        return COMBO;
    } else {
        return UNKNOWN;
    }
}

std::string
stripPath(std::string const& filename) {
    // Expecting something like "/results/0123aeb31b1c29a"
    // Strip out everything before and including the last /
    std::string::size_type pos = filename.rfind("/");
    if(pos == std::string::npos) {
        return filename;
    }
    return filename.substr(1+pos, std::string::npos);
}

}}} // namespace lsst::qserv::xrdfs
