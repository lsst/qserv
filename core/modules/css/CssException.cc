/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file CssException.h
  *
  * @brief Exception class for KvInterface.
  *
  * @Author Jacek Becla, SLAC
  */


// Standard library
#include <iostream>

// Boost includes
#include "boost/assign.hpp"

// Local includes
#include "CssException.h"

using std::map;
using std::string;

namespace lsst {
namespace qserv {
namespace css {

map<CssException::errCodeT, string> 
CssException::_errMap = boost::assign::map_list_of 
    (DB_DOES_NOT_EXIST,  "Database does not exist.")
    (KEY_EXISTS,         "Key already exist.")
    (KEY_DOES_NOT_EXIST, "Key does not exist.")
    (TB_DOES_NOT_EXIST,  "Table does not exist.")
    (AUTH_FAILURE,       "Authorization failure.")
    (CONN_FAILURE,       "Failed to connect to persistent store.")
    (INTERNAL_ERROR,     "Internal error.");

CssException::CssException(errCodeT errCode, string const& extraMsg) :
    std::runtime_error(""),
    _errCode(errCode) {
    string s = "CssException: " + _errMap.find(_errCode)->second;
    if (extraMsg != "") {
        s = s + " (" + extraMsg + ")";
    }
    _errMsg = new char[s.length()+1];
    strcpy(_errMsg, s.c_str());
}

const char*
CssException::what() const throw () {
    return _errMsg;
}

}}} // namespace lsst::qserv::css
