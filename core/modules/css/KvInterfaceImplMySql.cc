// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @file KvInterfaceImplMySql.h
  *
  * @brief Interface to the Central State System - MySql-based implementation.
  *
  * @Author Nathan Pease, SLAC
  */


// Class header
#include "css/KvInterfaceImplMySql.h"

// System headers
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/format.hpp"
#include "boost/lexical_cast.hpp"
#include "mysql/mysqld_error.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "sql/SqlResults.h"
#include "sql/SqlTransaction.h"

using std::map;
using std::string;
using std::vector;

namespace lsst {
namespace qserv {
namespace css {

namespace {

    // logger instance for this module
    LOG_LOGGER _logger = LOG_GET("lsst.qserv.css.KvInterfaceImplMySql");
    const char KEY_PATH_DELIMITER('/');

    /**
     * @brief Helper for getting the value from an INT row
     * @param results the object containing results information
     * @param value the output var to return the result
     * @return true if an int result could be obtained else false
     */
    bool extractIntValueFromSqlResults(sql::SqlResults* results, unsigned int* value) {
        // ideally results would be a sql::SqlResults const*, but extractFirstValue is non-const
        if (NULL == results || NULL == value) {
            throw CssError("null inout variable");
        }

        std::string resStr;
        sql::SqlErrorObject errObj;
        results->extractFirstValue(resStr, errObj);
        if (errObj.isSet()) {
            return false;
        }
        try {
            *value = boost::lexical_cast<unsigned int>(resStr);
        } catch (boost::bad_lexical_cast& ex) {
            return false;
        }
        return true;
    }
}

// this is copied more or less directly from QMetaTransaction. Refactor to shared location?
// proper place is to port it into db module
// https://github.com/LSST/db
// right now it's python only.
// create a ticket to make generalized code to be used between c++ and python
class KvTransaction
{
public:
    // Constructors
    KvTransaction(sql::SqlConnection& conn)
        : _errObj(), _trans(conn, _errObj) {
        if (_errObj.isSet()) {
            throw CssError(_errObj);
        }
    }

    // Destructor
    ~KvTransaction() {
        // instead of just destroying SqlTransaction instance we call abort and see
        // if error happens. We cannot throw here but we can print a message.
        if (_trans.isActive()) {
            _trans.abort(_errObj);
            if (_errObj.isSet()) {
                LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
                     _errObj.errNo() % _errObj.errMsg());
            }
        }
    }

    /// Explicitly commit transaction, throws SqlError for errors.
    void commit() {
        _trans.commit(_errObj);
        if (_errObj.isSet()) {
            LOGF(_logger, LOG_LVL_ERROR, "Failed to commit transaction: mysql error: (%1%) %2%" %
                 _errObj.errNo() % _errObj.errMsg());
            throw CssError(_errObj);
        }
    }

    /// Explicitly abort transaction, throws SqlError for errors.
    void abort() {
        _trans.abort(_errObj);
        if (_errObj.isSet()) {
            LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
                 _errObj.errNo() % _errObj.errMsg());
            throw CssError(_errObj);
        }
    }

    /// Query to find out if this represents an active transaction
    bool isActive() const {
        return _trans.isActive();
    }

private:
    sql::SqlErrorObject _errObj; // this must be declared before _trans
    sql::SqlTransaction _trans;
};


KvInterfaceImplMySql::KvInterfaceImplMySql(mysql::MySqlConfig const& mysqlConf)
: _conn(mysqlConf) {
}


void
KvInterfaceImplMySql::_findParentId(std::string const& childKvKey, bool* hasParent, unsigned int* parentKvId, KvTransaction const& transaction) {
    if (not transaction.isActive()) {
        throw CssError("A transaction must active here.");
    }
    if (hasParent == NULL || parentKvId == NULL) {
        throw CssError("null out-var ptr");
    }
    // Keys should always start with the delimiter and since we validate all keys created it should always be there.
    // However when looking for parent keys we do pull keys from the database, and there is some possibility the
    // values have been tampered with and the first delimiter removed.
    // It's easy enough to check for that condition here, so do a quick sanity check:
    if (0 != childKvKey.find_first_of(KEY_PATH_DELIMITER)) {
        LOGF(_logger, LOG_LVL_ERROR, "_findParentId - badly formatted childKvKey:%1%" % childKvKey);
        throw CssError("_findParentId - invalid childKvKey");
    }

    size_t loc = childKvKey.find_last_of(KEY_PATH_DELIMITER);
    if (loc == 0) {
        *hasParent = false;
        return;
    }
    std::string parentKey = childKvKey.substr(0, loc);
    std::string query = str(boost::format("SELECT kvId FROM kvData WHERE kvKey='%1%'") % _escapeSqlString(parentKey));
    sql::SqlResults results;
    sql::SqlErrorObject errObj;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "_findParentId - query failed: %1%" % query);
        throw CssError(errObj);
    } else {
        // If the key is not found then the parent does not exist; create it.
        if (not extractIntValueFromSqlResults(&results, parentKvId)) {
            // _create will throw if parent can't be created.
            *parentKvId = _create(parentKey, "", false, transaction);
        }
        *hasParent = true;
    }
}


void
KvInterfaceImplMySql::create(std::string const& key, std::string const& value) {
    // key is validated by _create
    KvTransaction transaction(_conn);
    _create(key, value, false, transaction);
    transaction.commit();
}


unsigned int
KvInterfaceImplMySql::_create(std::string const& key, std::string const& value, bool updateIfExists, KvTransaction const& transaction) {
    if (not transaction.isActive()) {
        throw CssError("A transaction must active here.");
    }

    _validateKey(key);

    unsigned int parentKvId(0);
    bool hasParent(false);
    _findParentId(key, &hasParent, &parentKvId, transaction);

    boost::format fmQuery;
    if (hasParent) {
        fmQuery = boost::format("INSERT INTO kvData (kvKey, kvVal, parentKvId) VALUES ('%1%', '%2%', '%3%')");
        fmQuery % _escapeSqlString(key) % _escapeSqlString(value) % parentKvId;
    } else {
        fmQuery = boost::format("INSERT INTO kvData (kvKey, kvVal) VALUES ('%1%', '%2%')"); // leave parentKvId NULL
        fmQuery % _escapeSqlString(key) % _escapeSqlString(value);
    }
    if (updateIfExists) {
        fmQuery = boost::format("%1% ON DUPLICATE KEY UPDATE kvVal='%2%'") % fmQuery % _escapeSqlString(value);
    }
    std::string query = fmQuery.str();
    sql::SqlErrorObject errObj;
    if (not _conn.runQuery(query, errObj)) {
        switch (errObj.errNo()) {
        default:
            throw CssError(errObj);
            break;

        case ER_DUP_ENTRY:
            LOGF(_logger, LOG_LVL_ERROR, "_create - SQL INSERT INTO failed: %1%" % query);
            throw KeyExistsError(errObj);
            break;
        }
    }

    unsigned int kvId = _conn.getInsertId();
    LOGF(_logger, LOG_LVL_DEBUG, "_create - executed query: %1%, kvId is:%2%" % query % kvId);
    return kvId;
}


void
KvInterfaceImplMySql::set(std::string const& key, std::string const& value) {
    // key is validated by _create
    KvTransaction transaction(_conn);
    _create(key, value, true, transaction);
    transaction.commit();
}


bool
KvInterfaceImplMySql::exists(std::string const& key) {
    KvTransaction transaction(_conn);
    std::string query = str(boost::format("SELECT COUNT(*) FROM kvData WHERE kvKey='%1%'") % _escapeSqlString(key));
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    LOGF(_logger, LOG_LVL_DEBUG, "exists - executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "exists - %1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("exists - error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    unsigned int count(0);
    if (not extractIntValueFromSqlResults(&results, &count)) {
        throw CssError("failed to extract int value from query");
    }

    if (count > 1) {
        throw CssError("multiple keys for key");
    }
    transaction.commit();
    return 1 == count;
}


std::vector<std::string>
KvInterfaceImplMySql::getChildren(std::string const& parentKey) {
    _validateKey(parentKey);
    // get the children with a /fully/qualified/path
    KvTransaction transaction(_conn);
    std::vector<std::string> strVec = _getChildrenFullPath(parentKey, transaction);
    transaction.commit();

    // trim off the parent key, leaving only the last item in the path.
    for (std::vector<std::string>::iterator strItr = strVec.begin(); strItr != strVec.end(); ++strItr) {
        size_t loc = strItr->find_last_of(KEY_PATH_DELIMITER)+1;
        if (strItr->size() == loc) {
            // technically this shouldn't happen because keys shoudln't end with '/', but a little safety does not hurt...
            continue;
        }
        *strItr = strItr->substr(loc, strItr->size());
    }
    return strVec;
}


std::vector<std::string>
KvInterfaceImplMySql::_getChildrenFullPath(std::string const& parentKey, KvTransaction const& transaction) {
    if (not transaction.isActive()) {
        throw CssError("A transaction must active here.");
    }
    _validateKey(parentKey);
    unsigned int parentId;
    if (not _getIdFromServer(parentKey, &parentId, transaction)) {
        if (not exists(parentKey)) {
            throw NoSuchKey(parentKey);
        }
    }

    std::string query = str(boost::format("SELECT kvKey FROM kvData WHERE parentKvId='%1%'") % parentId);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    LOGF(_logger, LOG_LVL_DEBUG, "_getChildrenFullPath - executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "_getChildrenFullPath - %1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("_getChildrenFullPath - error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    errObj.reset();
    std::vector<std::string> strVec;
    if (not results.extractFirstColumn(strVec, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "_getChildrenFullPath - failed to extract children from %1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("_getChildrenFullPath - failed to extract children error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    return strVec;
}


void
KvInterfaceImplMySql::deleteKey(std::string const& key) {
    KvTransaction transaction(_conn);
    _delete(key, transaction);
    transaction.commit();
}


void
KvInterfaceImplMySql::_delete(std::string const& key, KvTransaction const& transaction) {
    if (not transaction.isActive()) {
        throw CssError("A transaction must active here.");
    }
    _validateKey(key);

    // recursively delete child keys first
    std::vector<string> childKeys = _getChildrenFullPath(key, transaction);
    for (std::vector<std::string>::iterator strItr = childKeys.begin(); strItr != childKeys.end(); ++strItr) {
        _delete(*strItr, transaction);
    }

    std::string query = str(boost::format("DELETE FROM kvData WHERE kvKey='%1%'") % _escapeSqlString(key));
    sql::SqlErrorObject errObj;
    sql::SqlResults resultsObj;
    LOGF(_logger, LOG_LVL_DEBUG, "deleteKey - executing query: %1%" % query);
    if (not _conn.runQuery(query, resultsObj, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "deleteKey - failed running query:%1% with sqlErr:%2%" % query % errObj.errMsg());
        throw CssError(errObj);
    }

    // limit the row count to 1 - there should not be multiple rows for any key.
    auto affectedRows = resultsObj.getAffectedRows();
    if (affectedRows < 1) {
        LOGF(_logger, LOG_LVL_ERROR, "deleteKey - failed (no such key) running query:%2%" % query);
        throw NoSuchKey(errObj);
    } else if (affectedRows > 1) {
        LOGF(_logger, LOG_LVL_ERROR, "deleteKey - failed (too many (%1%) rows deleted) running query: %2%" % affectedRows % query);
        throw CssError("deleteKey - unexpectedly deleted more than 1 row.");
    }

}


std::string
KvInterfaceImplMySql::_get(std::string const& key, std::string const& defaultValue, bool throwIfKeyNotFound) {
    KvTransaction transaction(_conn);

    std::string val;
    sql::SqlErrorObject errObj;
    std::string query = str(boost::format("SELECT kvVal FROM kvData WHERE kvKey='%1%'") % _escapeSqlString(key));
    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "_get - query failed: %1%" % query);
        throw CssError(errObj);
    } else {
        errObj.reset();
        if (not results.extractFirstValue(val, errObj)) {
            if (throwIfKeyNotFound) {
                LOGF(_logger, LOG_LVL_ERROR, "_get - error extracting value:%1%" % errObj.errMsg());
                throw NoSuchKey(errObj);
            } else {
                val = defaultValue;
            }
        }
    }

    transaction.commit();
    return val;
}


bool KvInterfaceImplMySql::_getIdFromServer(std::string const& key, unsigned int* id, KvTransaction const& transaction) {
    if (not transaction.isActive()) {
        throw CssError("A transaction must active here.");
    }

    std::string query = str(boost::format("SELECT kvId FROM kvData WHERE kvKey='%1%'") % _escapeSqlString(key));
    sql::SqlResults results;
    sql::SqlErrorObject errObj;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "_getIdFromServer - query failed: %1%" % query);
        throw CssError(errObj);
        return false;
    }
    errObj.reset();
    if (not extractIntValueFromSqlResults(&results, id)) {
        return false;
    }

    return true;
}


void KvInterfaceImplMySql::_validateKey(std::string const& key) {
    // There is no need for a transaction here.

    // verify that:
    // - key is less than max length
    // - first character should be a delimiter
    // - last character should not be a delimiter
    if (key.size() > KvInterface::MAX_KEY_LENGTH ||
            key.find_first_of(KEY_PATH_DELIMITER) != 0 ||
            key.find_last_of(KEY_PATH_DELIMITER) == key.size()-1) {
        LOGF(_logger, LOG_LVL_DEBUG, "create - rejecting key:%1%" % key);
        throw CssError("invalid key");
    }
}


std::string KvInterfaceImplMySql::_escapeSqlString(std::string const& str) {
    // There is no need for a transaction here.

    sql::SqlErrorObject errObj;
    std::string escapedStr;
    if (not _conn.escapeString(str, escapedStr, errObj)) {
        throw CssError(errObj);
    }
    return escapedStr;
}

}}} // namespace lsst::qserv::css
