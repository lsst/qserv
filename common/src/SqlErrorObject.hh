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
 
#ifndef LSST_QSERV_SQL_ERROR_OBJECT_H
#define LSST_QSERV_SQL_ERROR_OBJECT_H

// Standard
#include <string>

namespace lsst {
namespace qserv {

class SqlErrorObject {
public:
    SqlErrorObject() : _errNo(0) {}
    
    int errNo() const { return _errNo; }
    std::string errMsg() const { return _errMsg; }
    bool isSet() { return _errNo != 0 || !_errMsg.empty(); }

    int setErrNo(int e) { _errNo = e; return e; }
    int addErrMsg(std::string const& s) { 
        if (_errMsg.empty()) _errMsg = s;
        else _errMsg += ' '; _errMsg += s;
        return _errNo;
    };
    void reset() { _errNo = 0; _errMsg.clear(); }

private:    
    int _errNo;           // error number
    std::string _errMsg;  // error message

    std::string printErrMsg() const;
};

}} // namespace lsst::qserv
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_SQL_ERROR_OBJECT_H
