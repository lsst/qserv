// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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
#include <stdexcept>
#include <string>
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "sql/SqlErrorObject.h"
#include "util/Issue.h"

namespace lsst::qserv::css {

/**
 * Base class for CSS run-time errors, represents a generic CSS run-time error.
 */
class CssError : public util::Issue {
public:
    explicit CssError(util::Issue::Context const& ctx, std::string const& msg) : util::Issue(ctx, msg) {}

    explicit CssError(util::Issue::Context const& ctx, sql::SqlErrorObject const& sqlErr)
            : util::Issue(ctx, "Error from mysql: (" + boost::lexical_cast<std::string>(sqlErr.errNo()) +
                                       ") " + sqlErr.errMsg()) {}

    // this name will be used to find corresponding Python exception type
    virtual std::string typeName() const { return "CssError"; }
};

/**
 * Specialized run-time error: database does not exist.
 */
class NoSuchDb : public CssError {
public:
    explicit NoSuchDb(util::Issue::Context const& ctx, std::string const& dbName)
            : CssError(ctx, "Database '" + dbName + "' does not exist.") {}

    virtual std::string typeName() const override { return "NoSuchDb"; }
};

/**
 * Specialized run-time error: key does not exist.
 */
class NoSuchKey : public CssError {
public:
    explicit NoSuchKey(util::Issue::Context const& ctx, std::string const& keyName)
            : CssError(ctx, "Key '" + keyName + "' does not exist.") {}

    explicit NoSuchKey(util::Issue::Context const& ctx, sql::SqlErrorObject const& sqlErr)
            : CssError(ctx, sqlErr) {}

    virtual std::string typeName() const override { return "NoSuchKey"; }
};

/**
 * Specialized run-time error: table does not exist.
 */
class NoSuchTable : public CssError {
public:
    explicit NoSuchTable(util::Issue::Context const& ctx, std::string const& dbName,
                         std::string const& tableName)
            : CssError(ctx, "Table '" + dbName + "." + tableName + "' does not exist.") {}

    virtual std::string typeName() const override { return "NoSuchTable"; }
};

/**
 * Specialized run-time error: table already exist.
 */
class TableExists : public CssError {
public:
    explicit TableExists(util::Issue::Context const& ctx, std::string const& dbName,
                         std::string const& tableName)
            : CssError(ctx, "Table '" + dbName + "." + tableName + "' already exists.") {}

    virtual std::string typeName() const override { return "TableExists"; }
};

/**
 * Specialized run-time error: authorization failure.
 */
class AuthError : public CssError {
public:
    AuthError(util::Issue::Context const& ctx) : CssError(ctx, "Authorization failure.") {}

    virtual std::string typeName() const override { return "AuthError"; }
};

/**
 * Specialized run-time error: connection failure.
 */
class ConnError : public CssError {
public:
    ConnError(util::Issue::Context const& ctx) : CssError(ctx, "Failed to connect to persistent store.") {}
    explicit ConnError(util::Issue::Context const& ctx, std::string const& reason)
            : CssError(ctx, "Failed to connect to persistent store. (" + reason + ")") {}

    virtual std::string typeName() const override { return "ConnError"; }
};

/**
 * Specialized run-time error: key exists.
 */
class KeyExistsError : public CssError {
public:
    explicit KeyExistsError(util::Issue::Context const& ctx, std::string const& key)
            : CssError(ctx, "Key '" + key + "' already exists.") {}

    explicit KeyExistsError(util::Issue::Context const& ctx, sql::SqlErrorObject const& sqlErr)
            : CssError(ctx, sqlErr) {}

    virtual std::string typeName() const override { return "KeyExistsError"; }
};

/**
 * Specialized run-time error: something is wrong with key value.
 */
class KeyValueError : public CssError {
public:
    explicit KeyValueError(util::Issue::Context const& ctx, std::string const& key,
                           std::string const& message)
            : CssError(ctx, "Key '" + key + "' value error: " + message) {}

    virtual std::string typeName() const override { return "KeyValueError"; }
};

/**
 * Specialized run-time error: can't allocate memory to get data for a given key.
 */
class BadAllocError : public CssError {
public:
    BadAllocError(util::Issue::Context const& ctx, std::string const& key, std::string const& sizeTried)
            : CssError(ctx, "Can't allocate memory to get data for key'" + key + "'" +
                                    ", tried allocating up to " + sizeTried + " bytes.") {}

    virtual std::string typeName() const override { return "BadAllocError"; }
};

/**
 * Specialized run-time error: missing version number
 */
class VersionMissingError : public CssError {
public:
    explicit VersionMissingError(util::Issue::Context const& ctx, std::string const& key)
            : CssError(ctx, "Key for CSS version is not defined: '" + key + "'") {}

    virtual std::string typeName() const override { return "VersionMissingError"; }
};

/**
 * Specialized run-time error: version number mismatch
 */
class VersionMismatchError : public CssError {
public:
    VersionMismatchError(util::Issue::Context const& ctx, std::string const& expected,
                         std::string const& actual)
            : CssError(ctx, "CSS version number mismatch: expected=" + expected + ", actual=" + actual) {}

    virtual std::string typeName() const override { return "VersionMismatchError"; }
};

/**
 * Specialized run-time error: database does not exist.
 */
class ReadonlyCss : public CssError {
public:
    explicit ReadonlyCss(util::Issue::Context const& ctx)
            : CssError(ctx, "Attempt to modify read-only CSS.") {}

    virtual std::string typeName() const override { return "ReadonlyCss"; }
};

/**
 * Specialized run-time error: node does not exist.
 */
class NoSuchNode : public CssError {
public:
    explicit NoSuchNode(util::Issue::Context const& ctx, std::string const& nodeName)
            : CssError(ctx, "Node '" + nodeName + "' does not exist.") {}

    virtual std::string typeName() const override { return "NoSuchNode"; }
};

/**
 * Specialized run-time error: node does not exist.
 */
class NodeExists : public CssError {
public:
    explicit NodeExists(util::Issue::Context const& ctx, std::string const& nodeName)
            : CssError(ctx, "Node '" + nodeName + "' already exists.") {}

    virtual std::string typeName() const override { return "NodeExists"; }
};

/**
 * Specialized run-time error: node in use, cannot be deleted.
 */
class NodeInUse : public CssError {
public:
    explicit NodeInUse(util::Issue::Context const& ctx, std::string const& nodeName)
            : CssError(ctx, "Node '" + nodeName + "' is in use and cannot be deleted.") {}

    virtual std::string typeName() const override { return "NodeInUse"; }
};

/**
 * Specialized run-time error: configuration is invalid.
 */
class ConfigError : public CssError {
public:
    explicit ConfigError(util::Issue::Context const& ctx, std::string const& msg)
            : CssError(ctx, "Invalid config: " + msg) {}

    virtual std::string typeName() const override { return "ConfigError"; }
};

}  // namespace lsst::qserv::css

#endif  // LSST_QSERV_CSSERROR_H
