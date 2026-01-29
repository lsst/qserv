// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
 * @ingroup util
 *
 * @brief Store a Qserv error
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#ifndef LSST_QSERV_UTIL_ERROR_H_
#define LSST_QSERV_UTIL_ERROR_H_

// System headers
#include <set>
#include <string>
#include <vector>

namespace lsst::qserv::util {

/// Store a Qserv error to be used with util::MultiError
class Error {
public:
    /// TODO: fix confusion between status and code see: DM-2996
    /// Final Errors sent to the user are set by qmeta::MessageStore which
    /// writes the errors to a database table, which makes them impossible
    /// to sort from this side.
    /// All of the errors in the MultiError object go into a single error
    /// row in the table, so sorting them in MultiError does have an effect,
    /// but the proxy may reorder all of the rows in qmeta::MessageStore
    ///
    /// List of Qserv errors
    /// Errors codes should be in order of likely usefulness to the end user.
    /// MariahDB errors should all be below 5000. Any error in qana
    /// makes it unlikely the work will continue long enough to see
    /// any of the errors listed here.
    enum ErrCode {
        // Default for blank error.
        NONE = 0,
        // Significant MySQL error codes
        UNKNOWN_TABLE = 1051,      // usually associated with ALTER and DROP
        NONEXISTANT_TABLE = 1146,  // usually associated with SELECT, INSERT
        // Qserv errors begin
        QSERV_ERR = 5000,  // Should avoid conflicts with MariaDB errors.
        // Query plugin errors:
        DUPLICATE_SELECT_EXPR,
        // InfileMerger errors:
        HEADER_IMPORT,
        HEADER_OVERFLOW,
        RESULT_IMPORT,
        RESULT_MD5,
        MYSQLOPEN,
        MERGEWRITE,
        TERMINATE,
        CREATE_TABLE,
        MYSQLCONNECT,
        MYSQLEXEC,
        CZAR_RESULT_TOO_LARGE,
        JOB_CANCEL,
        // Worker errors:
        WORKER_RESULT_TOO_LARGE,
        WORKER_ERROR,
        WORKER_QUERY,
        WORKER_SQL_CONNECT,
        WORKER_SQL,
        // Czar internal errors
        INTERNAL,
        RETRY_FAILS,
        RETRY_UNASSIGN,
        RESULT_CONNECT,
        RESULT_CREATETABLE,
        RESULT_SCHEMA,
        RESULT_SQL,
        // Communication errors
        CZAR_WORKER_COM,
        WORKER_CZAR_COM,
        // Use a large number to put at the end of the list.
        CANCEL = 10'000'000
    };

    Error(int code, int subCode, std::string const& msg = "", bool logLvlErr = true);
    Error(int code, int subCode, std::set<int> const& chunkIds, std::set<int> const& jobIds,
          std::string const& msg, bool logLvlErr = true);

    Error() = default;
    Error(Error const&) = default;
    Error& operator=(Error const&) = default;
    bool operator==(Error const& other) const = default;

    ~Error() = default;

    int getCode() const { return _code; }
    int getSubCode() const { return _subCode; }
    std::vector<int> getChunkIdsVect() const;
    std::vector<int> getJobIdsVect() const;

    const std::string& getMsg() const { return _msg; }

    /// Check if current Object contains an error
    /// @return true if current object doesn't contain an error
    bool isNone() { return (_code == NONE); }

    void incrCount(int val = 1) { _count += val; }
    int getCount() const { return _count; }

    std::string dump() const;
    std::ostream& dump(std::ostream& os, bool showJobs = false) const;
    friend std::ostream& operator<<(std::ostream& out, Error const& error);

private:
    int _code = NONE;
    /// Only used for certain cases, such as SQL error numbers, may have any value.
    int _subCode = 0;
    std::set<int> _jobIds;    /// Job ID number, when useful.
    std::set<int> _chunkIds;  /// Chunk ID number, when useful.
    std::string _msg;
    int _count = 1;
};

}  // namespace lsst::qserv::util

#endif /* LSST_QSERV_UTIL_ERROR_H_ */
