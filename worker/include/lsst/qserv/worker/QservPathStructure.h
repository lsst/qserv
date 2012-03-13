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
 
#ifndef LSST_QSERV_QSERVPATHSTRUCTURE_H
#define LSST_QSERV_QSERVPATHSTRUCTURE_H

#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace worker {

class QservPathStructure {

public:
    QservPathStructure() {}

    bool insert(const std::vector<std::string>& paths);

    /// calls mkdir for each directory, including parent
    /// directories from the inserted paths
    bool persist();

    // for testing/debugging
    const std::vector<std::string> uniqueDirs() const;
    void printUniquePaths() const;

private:
    bool processOneDir(const std::string&);
    bool isStored(const std::string&) const;

    bool createDirectories() const;
    bool createPaths() const;

    std::vector<std::string> _paths;
    std::vector<std::string> _uniqueDirs;
};

}}} // namespace lsst::qserv::worker
#endif // LSST_QSERV_QSERVPATHEXPORT_H
