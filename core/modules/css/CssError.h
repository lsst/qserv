// -*- LSST-C++ -*-
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
  * @file
  *
  * @brief RunTimeErrors for KvInterface.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSSERROR_H
#define LSST_QSERV_CSSERROR_H

// System headers
#include <map>
#include <stdexcept>
#include <string>

namespace lsst {
namespace qserv {
namespace css {

/**
 * Base class for CSS run-time errors, represents a generic CSS run-time error.
 */
class CssError : public std::runtime_error {
public:
    explicit CssError(std::string const& msg)
        : std::runtime_error(msg) {}
};

/**
 * Specialized run-time error: database does not exist.
 */
class NoSuchDb : public CssError {
public:
    NoSuchDb(std::string const& dbName)
        : CssError("Database '" + dbName + "' does not exist.") {}
};

/**
 * Specialized run-time error: key does not exist.
 */
class NoSuchKey : public CssError {
public:
    NoSuchKey(std::string const& keyName)
        : CssError("Key '" + keyName + "' does not exist.") {}
};

/**
 * Specialized run-time error: table does not exist.
 */
class NoSuchTable : public CssError {
public:
    NoSuchTable(std::string const& tableName)
        : CssError("Table '" + tableName + "' does not exist.") {}
};

/**
 * Specialized run-time error: authorization failure.
 */
class AuthError : public CssError {
public:
    AuthError()
        : CssError("Authorization failure.") {}
};

/**
 * Specialized run-time error: connection failure.
 */
class ConnError : public CssError {
public:
    ConnError()
        : CssError("Failed to connect to persistent store.") {}
    ConnError(std::string const& reason)
        : CssError("Failed to connect to persistent store. (" + reason + ")") {}
};

/**
 * Specialized run-time error: key exists.
 */
class KeyExistsError : public CssError {
public:
    KeyExistsError(std::string const& key)
        : CssError("Key '" + key +"' already exists.") {}
};

/**
 * Specialized run-time error: can't allocate memory to get data for a given key.
 */
class BadAllocError : public CssError {
public:
    BadAllocError(std::string const& key, std::string const& sizeTried)
        : CssError("Can't allocate memory to get data for key'" + key +"'"
                   + ", tried allocating up to " + sizeTried + " bytes.") {}
};

/**
 * Specialized run-time error: missing version number
 */
class VersionMissingError : public CssError {
public:
    VersionMissingError(std::string const& key)
        : CssError("Key for CSS version is not defined: '" + key +"'") {}
};

/**
 * Specialized run-time error: version number mismatch
 */
class VersionMismatchError : public CssError {
public:
    VersionMismatchError(std::string const& expected, std::string const& actual)
        : CssError("CSS version number mismatch: expected=" + expected +", actual=" + actual) {}
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSSERROR_H
