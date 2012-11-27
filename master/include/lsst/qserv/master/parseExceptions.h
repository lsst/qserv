/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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

// parseExceptions.h: Parse exception classes.
//
//
 
#ifndef LSST_QSERV_MASTER_PARSEEXCEPTIONS_H
#define LSST_QSERV_MASTER_PARSEEXCEPTIONS_H

// C++ 
#include <string>

namespace lsst {
namespace qserv {
namespace master {

class UnsupportedSyntaxError : public std::exception {
public:
    UnsupportedSyntaxError(std::string const& d) 
        : desc("UnsupportedSyntaxError(" + d + ")") {}
    virtual ~UnsupportedSyntaxError() throw() {}
    virtual const char* what() const throw() { return desc.c_str(); }
    
    std::string desc;
};

}}} // lsst::qserv::master
#endif // LSST_QSERV_MASTER_PARSEEXCEPTIONS_H
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             
