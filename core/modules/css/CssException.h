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

#ifndef LSST_QSERV_CSS_EXCEPTION_HH
#define LSST_QSERV_CSS_EXCEPTION_HH

// Standard library
#include <map>
#include <stdexcept>
#include <string>

namespace lsst {
namespace qserv {
namespace css {

class CssException : public std::runtime_error {
public:
    enum errCodeT { 
        DB_DOES_NOT_EXIST,
        KEY_DOES_NOT_EXIST,
        KEY_EXISTS,
        TB_DOES_NOT_EXIST,
        AUTH_FAILURE,
        CONN_FAILURE,
        INTERNAL_ERROR
    };     
    //CssException(errCodeT errCode);
    CssException(errCodeT errCode, std::string const& extraMsg="");
    virtual ~CssException() throw() {
        delete [] _errMsg;
    }
    
    virtual const char* what() const throw();
    errCodeT errCode() const {
        return _errCode;
    }

protected:
    explicit CssException(std::string const& msg)
        : std::runtime_error(msg) {}

private:
    errCodeT _errCode;
    char* _errMsg;
    static std::map<errCodeT, std::string> _errMap;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_EXCEPTION_HH
