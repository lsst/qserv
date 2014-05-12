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

// System headers
#include <map>
#include <stdexcept>
#include <string>

namespace lsst {
namespace qserv {
namespace css {

/**
 * Base class for all CSS run-time errors
 */
class CssRunTimeException : public std::runtime_error {
protected:
    explicit CssRunTimeException(std::string const& msg)
        : std::runtime_error(msg) {}
};

/**
 * Specialized run-time error: database does not exist.
 */
class CssException_DbDoesNotExist : public CssRunTimeException {
public:
    CssException_DbDoesNotExist(std::string const& dbName)
        : CssRunTimeException("Database '" + dbName + "' does not exist.") {}
};

/**
 * Specialized run-time error: key does not exist.
 */
class CssException_KeyDoesNotExist : public CssRunTimeException {
public:
    CssException_KeyDoesNotExist(std::string const& keyName)
        : CssRunTimeException("Key '" + keyName + "' does not exist.") {}
};

/**
 * Specialized run-time error: table does not exist.
 */
class CssException_TableDoesNotExist : public CssRunTimeException {
public:
    CssException_TableDoesNotExist(std::string const& tableName)
        : CssRunTimeException("Table '" + tableName + "' does not exist.") {}
};

/**
 * Specialized run-time error: authorization failure.
 */
class CssException_AuthFailure : public CssRunTimeException {
public:
    CssException_AuthFailure()
        : CssRunTimeException("Authorization failure.") {}
};

/**
 * Specialized run-time error: connection failure.
 */
class CssException_ConnFailure : public CssRunTimeException {
public:
    CssException_ConnFailure()
        : CssRunTimeException("Failed to connect to persistent store.") {}
};

/**
 * Generic CSS run-time error.
 */
class CssException_InternalRunTimeError : public CssRunTimeException {
public:
    CssException_InternalRunTimeError(std::string const& extraMsg)
        : CssRunTimeException("Internal run-time error. (" + extraMsg + ")") {}
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_EXCEPTION_HH
