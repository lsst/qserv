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

#include "lsst/qserv/worker/QservPathStructure.h"
#include <sys/stat.h>
#include <iostream>

namespace qsrv = lsst::qserv;

using std::string;
using std::vector;

bool
qsrv::QservPathStructure::insert(const vector<string>& paths) {
    _uniqueDirs.clear();

    vector<string>::const_iterator pItr;
    for ( pItr=paths.begin(); pItr!=paths.end(); ++pItr) {
        int pos = pItr->find_last_of('/');
        if ( pos == -1 ) {
            std::cerr << "Problems with path: " << *pItr << std::endl;
            return false;
        }
        if ( ! processOneDir(pItr->substr(0, pos)) ) {
            return false;
        }
    }
    return true;
}

bool
qsrv::QservPathStructure::persist() {
    vector<string>::const_iterator dItr;
    for ( dItr=_uniqueDirs.begin(); dItr!=_uniqueDirs.end(); ++dItr) {
        const char* theDir = dItr->c_str();

        struct stat st;
        if ( stat(theDir, &st) != 0 ) {
            std::cout << "creating: " << theDir << std::endl;
            int n = mkdir(theDir, 0755);
            if ( n != 0 ) {
                std::cerr << "Failed to mkdir(" << *dItr << "), err: " 
                          << n << std::endl;
                return false;
            }
        } else {
            std::cout << theDir << " exists" << std::endl;
        }
    }
    return true;
}

const std::vector<std::string>
qsrv::QservPathStructure::uniqueDirs() const {
    return _uniqueDirs;
}

void
qsrv::QservPathStructure::printUniquePaths() const {
    std::vector<std::string>::const_iterator dItr;
    for ( dItr=_uniqueDirs.begin(); dItr!=_uniqueDirs.end(); ++dItr) {
        std::cout << "Unique dir: " << *dItr << std::endl;
    }
}

bool
qsrv::QservPathStructure::processOneDir(const string& s)
{
    int pos = s.find_last_of('/');
    if ( pos == -1 ) {
        std::cerr << "Problems with path: " << s << std::endl;
        return false;
    } else if ( pos > 2 ) { // there is at least one more parent dir
        if ( !processOneDir(s.substr(0, pos)) ) {
            return false;
        }
    }
    if ( !isStored(s) ) {
        _uniqueDirs.push_back(s);
    }
    return true;
}

bool
qsrv::QservPathStructure::isStored(const std::string& s) const {
    vector<string>::const_iterator dItr;
    for (dItr=_uniqueDirs.begin() ; dItr!=_uniqueDirs.end(); ++dItr) {
        if (*dItr == s) {
            return true;
        }
    }
    return false;
}
