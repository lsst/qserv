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
#include <sys/stat.h>
#include <iostream>

namespace qsrv = lsst::qserv;

bool
qsrv::QservPathExport::extractUniqueDirs(const vector<string>& exportPaths,
                                         vector<string>& uniqueDirs) {
    uniqueDirs.clear();

    vector<string>::const_iterator pItr;
    for ( pItr=exportPaths.begin(); pItr!=exportPaths.end(); ++pItr) {
        int pos = pItr->find_last_of('/');
        if ( pos == -1 ) {
            std::cerr << "Problems with path: " << *pItr << std::endl;
            return false;
        }
        string s = pItr->substr(0, pos);

        bool found = false;
        vector<string>::iterator dItr;
        for (dItr=uniqueDirs.begin() ; dItr!=uniqueDirs.end(); ++dItr) {
            if (*dItr == s) {
                found = true;
                break;
            }
        }
        if ( ! found ) {
            uniqueDirs.push_back(s);
        }
    }
    return true;
}

bool
qsrv::QservPathExport::mkDirs(const vector<string>& dirs) {
    vector<string>::const_iterator dItr;
    for ( dItr=dirs.begin(); dItr!=dirs.end(); ++dItr) {
        int n = mkdir(dItr->c_str(), 0x755);
        if ( n != 0 ) {
            std::cerr << "Failed to mkdir(" << *dItr << "), err: " 
                      << n << std::endl;
            return false;
        }
    }
    return true;
}
