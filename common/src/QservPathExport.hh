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
 
#ifndef LSST_QSERV_QSERVPATHEXPORT_H
#define LSST_QSERV_QSERVPATHEXPORT_H

#include <string>
#include <vector>

using std::string;
using std::vector;

namespace lsst {
namespace qserv {

class QservPathExport {

public:
    QservPathExport() {}

    /// extracts a list of unique directory names
    /// from the exportPaths including parent directories,
    /// and stores them in the uniqueDirs vector
    bool extractUniqueDirs(const vector<string>& exportPaths,
                           vector<string>& uniqueDirs);

    /// calls mkdir for each directory in the passed vector
    bool mkDirs(const vector<string>& dirs);
    
private:
    bool processOneDir(const string, vector<string>&);
};

}} // namespace lsst::qserv
#endif // LSST_QSERV_QSERVPATHEXPORT_H
