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

/// TableMerger.h declares:
///
/// struct TableMergerError
/// class TableMergerConfig
/// class TableMerger

/// FIXME:::::The TableMerger classes are responsible for properly feeding in
/// chunkquery results into a mysql instance. When all results are
/// collected, a fixup step may be needed, as specified when
/// configuring the TableMerger.

#ifndef LSST_QSERV_RPROC_INFILEMERGER_H
#define LSST_QSERV_RPROC_INFILEMERGER_H

// System headers
#include <string>

// Third-party headers
#include <boost/thread.hpp> // for mutex.
#include <boost/shared_ptr.hpp>

// Local headers
#include "rproc/mergeTypes.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}
namespace proto {
    class ProtoHeader;
    class Result;
}
namespace qdisp {
    class MessageStore;
}
namespace query {
    class SelectStmt;
}
namespace rproc {
    class SqlInsertIter;
}
namespace sql {
    class SqlConnection;
}
namespace util {
    class PacketBuffer;
}
namespace xrdc {
    class PacketIter;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace rproc {

/// struct InfileMergerError - value class for InfileMerger error code.
struct InfileMergerError {
public:
    enum {NONE=0, HEADER_IMPORT, HEADER_OVERFLOW,
          RESULT_IMPORT, RESULT_MD5, MYSQLOPEN, MERGEWRITE, TERMINATE,
          CREATE_TABLE,
          MYSQLCONNECT, MYSQLEXEC} status;
    InfileMergerError() {}
    InfileMergerError(int code) : errorCode(code) {}
    InfileMergerError(int code, char const* desc)
        : errorCode(code), description(desc) {}
    int errorCode;
    std::string description;
    bool resultTooBig() const;
};

/// class InfileMergerConfig - value class for configuring a InfileMerger
class InfileMergerConfig {
public:
    InfileMergerConfig() {}
    InfileMergerConfig(boost::shared_ptr<qdisp::MessageStore> messageStore_,
                       std::string const& targetDb_,
                       std::string const& targetTable_,
                       boost::shared_ptr<query::SelectStmt> mergeStmt_,
                       std::string const& user_, std::string const& socket_)
        :  messageStore(messageStore_),
           targetDb(targetDb_),  targetTable(targetTable_),
           mergeStmt(mergeStmt_), user(user_), socket(socket_)
    {
    }

    boost::shared_ptr<qdisp::MessageStore> messageStore;
    std::string targetDb; // for final result, and imported result
    std::string targetTable;
    boost::shared_ptr<query::SelectStmt> mergeStmt;
    std::string user;
    std::string socket;
};

/// class InfileMerger : A class that performs merging of subquery
/// result tables from dumpfiles sent back by workers. merge() should
/// be called after each result is read back from the worker.
class InfileMerger {
public:
    typedef boost::shared_ptr<util::PacketBuffer> PacketBufferPtr;
    explicit InfileMerger(InfileMergerConfig const& c);
    ~InfileMerger();

    off_t merge(char const* dumpBuffer, int dumpLength);

    InfileMergerError const& getError() const { return _error; }
    std::string getTargetTable() const {return _config.targetTable; }

    bool finalize();
    bool isFinished() const;

private:
    class Msgs;
#if 0
    int _fetchWithHeader(char const* buffer, int length);
    int _fetchWithoutHeader(char const* buffer, int length);
#endif
    int _readHeader(proto::ProtoHeader& header, char const* buffer, int length);
    int _readResult(proto::Result& result, char const* buffer, int length);
    bool _verifySession(int sessionId);
    bool _verifyMd5(std::string const& expected, std::string const& actual);
    int _importBuffer(char const* buffer, int length, bool setupTable);
    bool _setupTable(Msgs const& msgs);
    void _setupRow();

    class CreateStmt;
    bool _applySql(std::string const& sql);
    bool _applySqlLocal(std::string const& sql);
    std::string _buildMergeSql(std::string const& tableName, bool create);
    std::string _buildOrderByLimit();
    bool _createTableIfNotExists(CreateStmt& cs);

    void _fixupTargetName();
    bool _importResult(std::string const& dumpFile);
    bool _slowImport(std::string const& dumpFile,
                     std::string const& tableName);
    bool _importFromBuffer(char const* buf, std::size_t size,
                           std::string const& tableName);
    bool _importBufferInsert(char const* buf, std::size_t size,
                             std::string const& tableName, bool allowNull);
    bool _dropAndCreate(std::string const& tableName, std::string createSql);
    bool _importIter(SqlInsertIter& sii, std::string const& tableName);

    static std::string const _dropSql;
    static std::string const _createSql;
    static std::string const _createFixSql;
    static std::string const _insertSql;
    static std::string const _cleanupSql;
    static std::string const _cmdBase;

    InfileMergerConfig _config;
    std::string _loadCmd;
    boost::shared_ptr<mysql::MySqlConfig> _sqlConfig;
    boost::shared_ptr<sql::SqlConnection> _sqlConn;

    std::string _mergeTable;
    InfileMergerError _error;
    int _tableCount;
    bool _isFinished;
    boost::mutex _createTableMutex;
    boost::mutex _sqlMutex;

    class Mgr;
    std::auto_ptr<Mgr> _mgr;

    bool _needCreateTable;
    bool _needHeader;
};

}}} // namespace lsst::qserv::rproc

// Local Variables:
// mode:c++
// comment-column:0
// End:

#endif // LSST_QSERV_RPROC_INFILEMERGER_H
