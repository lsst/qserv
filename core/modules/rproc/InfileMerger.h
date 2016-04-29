// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_RPROC_INFILEMERGER_H
#define LSST_QSERV_RPROC_INFILEMERGER_H
/// InfileMerger.h declares:
///
/// struct InfileMergerError
/// class InfileMergerConfig
/// class InfileMerger
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/Error.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}
namespace proto {
    class ProtoHeader;
    class Result;
    struct WorkerResponse;
}
namespace qdisp {
    class MessageStore;
}
namespace query {
    class SelectStmt;
}
namespace sql {
    class SqlConnection;
}
}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace rproc {

/** \typedef InfileMergerError Store InfileMerger error code.
 *
 * \note:
 * Keep this indirection to util::Error in case
 * InfileMergerError::resultTooBig() method might is needed in the future
 *
 * */
typedef util::Error InfileMergerError;

/// class InfileMergerConfig - value class for configuring a InfileMerger
class InfileMergerConfig {
public:
    InfileMergerConfig() {}
    InfileMergerConfig(mysql::MySqlConfig const& mySqlConfig)
        :  mySqlConfig(mySqlConfig)
    {
    }
    // for final result, and imported result
    mysql::MySqlConfig const mySqlConfig;
    std::string targetTable;
    std::shared_ptr<query::SelectStmt> mergeStmt;
};

/// InfileMerger is a row-based merger that imports rows from result messages
/// and inserts them into a MySQL table, as specified during construction by
/// InfileMergerConfig.
///
/// To use, construct a configured instance, then call merge() to kick off the
/// merging process, and finalize() to wait for outstanding merging processes
/// and perform the appropriate post-processing before returning.  merge() right
/// now expects an entire message buffer, where a message buffer consists of:
/// Byte 0: unsigned char size of ProtoHeader message
/// Bytes 1 - size_ph : ProtoHeader message (containing size of result message)
/// Bytes size_ph - size_ph + size_rm : Result message
/// At present, Result messages are not chained.
class InfileMerger {
public:
    explicit InfileMerger(InfileMergerConfig const& c);
    ~InfileMerger();

    /// Merge a worker response, which contains:
    /// Size of ProtoHeader message
    /// ProtoHeader message
    /// Result message
    /// @return true if merge was successfully imported (queued)
    bool merge(std::shared_ptr<proto::WorkerResponse> response);

    /// @return error details if finalize() returns false
    InfileMergerError const& getError() const { return _error; }
    /// @return final target table name  storing results after post processing
    std::string getTargetTable() const {return _config.targetTable; }
    /// Finalize a "merge" and perform postprocessing
    bool finalize();
    /// Check if the object has completed all processing.
    bool isFinished() const;

private:
    int _readHeader(proto::ProtoHeader& header, char const* buffer, int length);
    int _readResult(proto::Result& result, char const* buffer, int length);
    bool _verifySession(int sessionId);
    bool _importResponse(std::shared_ptr<proto::WorkerResponse> response);
    bool _setupTable(proto::WorkerResponse const& response);
    void _setupRow();
    bool _applySql(std::string const& sql);
    bool _applySqlLocal(std::string const& sql);
    void _fixupTargetName();

    InfileMergerConfig _config; ///< Configuration
    std::shared_ptr<sql::SqlConnection> _sqlConn; ///< SQL connection

    std::string _mergeTable; ///< Table for result loading
    InfileMergerError _error; ///< Error state

    bool _isFinished; ///< Completed?
    std::mutex _createTableMutex; ///< protection from creating tables
    std::mutex _sqlMutex; ///< Protection for SQL connection

    class Mgr;
    std::unique_ptr<Mgr> _mgr; ///< Delegate merging action object

    bool _needCreateTable; ///< Does the target table need creating?
};

}}} // namespace lsst::qserv::rproc

#endif // LSST_QSERV_RPROC_INFILEMERGER_H
