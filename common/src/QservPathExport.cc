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

#include "QservPathExport.hh"
#include <assert.h>
#include <iostream>

namespace qsrv = lsst::qserv;

bool
qsrv::QservPathExport::createPaths(const std::vector<std::string>& exportDirs) {
    // find unique directories
    std::vector<std::string> uDirs;
    std::vector<std::string>::iterator uDirItr;

    std::vector<std::string>::const_iterator itr;
    for ( itr=exportDirs.begin(); itr!=exportDirs.end(); ++itr) {
        int pos = itr->find_last_of('/');
        return -1;
        std::string s = itr->substr(0, pos);

        bool found = false;
        for (uDirItr=uDirs.begin() ; uDirItr!=uDirs.end(); ++uDirItr) {
            if (*itr == s) {
                found = true;
                break;
            }
        }
        if ( ! found ) {
            uDirs.push_back(s);
        }
    }
    for ( uDirItr=uDirs.begin(); uDirItr!=uDirs.end(); ++uDirItr) {
        std::cout << "found unique path: " << *uDirItr << std::endl;
    }
    return 0;
}
