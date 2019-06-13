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
#ifndef LSST_QSERV_SQL_MOCKSQL_H
#define LSST_QSERV_SQL_MOCKSQL_H

#include <memory>

// Local headers
#include "sql/SqlConnection.h"
#include "sql/SqlException.h"


namespace lsst {
namespace qserv {
namespace sql {
class MockSql : public SqlConnection {
public:
    MockSql() {}
    ~MockSql() {}

    typedef std::map<std::string, std::map<std::string, std::vector<std::string>>> DbTableColumns;

    MockSql(DbColumns dbColumns) : _dbColumns(dbColumns) {}

    static std::shared_ptr<MockSql> MakeMockSql(DbTableColumns dbTableColumns) {
        auto mockSql = std::make_shared<MockSql>();
        mockSql->_dbTableColumns = dbTableColumns;
        return mockSql;
    }

    virtual void reset(mysql::MySqlConfig const& sc, bool useThreadMgmt=false) {}
    virtual bool connectToDb(SqlErrorObject&) { return false; }
    virtual bool selectDb(std::string const& dbName, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(char const* query, int qSize,
                          SqlResults& results, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(char const* query, int qSize, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(std::string const query, SqlResults&,
                          SqlErrorObject&) {
        return false; }
    virtual std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query);
    virtual bool runQuery(std::string const query, SqlErrorObject&) {
        return false; }
    virtual bool dbExists(std::string const& dbName, SqlErrorObject&) {
        return false; }
    virtual bool createDb(std::string const& dbName, SqlErrorObject&,
                          bool failIfExists=true) {
        return false; }
    virtual bool createDbAndSelect(std::string const& dbName,
                                   SqlErrorObject&,
                                   bool failIfExists=true) {
        return false; }
    virtual bool dropDb(std::string const& dbName, SqlErrorObject&,
                        bool failIfDoesNotExist=true) {
        return false; }
    virtual bool tableExists(std::string const& tableName,
                             SqlErrorObject&,
                             std::string const& dbName="") {
        return false; }
    virtual bool dropTable(std::string const& tableName,
                           SqlErrorObject&,
                           bool failIfDoesNotExist=true,
                           std::string const& dbName="") {
        return false; }
    virtual bool listTables(std::vector<std::string>&,
                            SqlErrorObject&,
                            std::string const& prefixed="",
                            std::string const& dbName="") {
        return false; }
    virtual void listColumns(std::vector<std::string>& columns,
                            SqlErrorObject&,
                            std::string const& dbName,
                            std::string const& tableName) {
        // The QueryContext gets all the columns in each table used by the query and stores this information
        // for lookup later. Here we return a list of column names for a table.

        if (not _dbTableColumns.empty()) {
            auto tableItr = _dbTableColumns.find(dbName);
            if (tableItr == _dbTableColumns.end()) {
                throw sql::NoSuchDb(lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__), dbName);
            }
            auto columnsItr = tableItr->second.find(tableName);
            if (columnsItr == tableItr->second.end()) {
                throw sql::NoSuchTable(lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__),
                        dbName, tableName);
            }
            for (auto const& column : columnsItr->second) {
                columns.push_back(column);
            }
            return;
        }

        // TODO FIX COMMENT
        // There is no test for db name right now; the users of this class do not provide a db name, just a
        // table name and the columns it would/should contain. If database name testing needs to be done it
        // would need to get added here for testing.

        auto dbColumnsItr = _dbColumns.find(tableName);
        if (dbColumnsItr == _dbColumns.end()) {
            throw sql::NoSuchTable(lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__),
                    "(unused)" + dbName, tableName);
        }
        for (auto const& column : dbColumnsItr->second) {
            columns.push_back(column);
        }
    }

    virtual std::string getActiveDbName() const { return std::string(); }

    template <class TupleListIter>
    struct Iter : public SqlResultIter {
        Iter() {}
        Iter(TupleListIter begin, TupleListIter end) {
            _cursor = begin;
            _end = end;
        }
        virtual ~Iter() {}
        virtual SqlErrorObject& getErrorObject() { return _errObj; }
        virtual StringVector const& operator*() const { return *_cursor; }
        virtual SqlResultIter& operator++() { ++_cursor; return *this; }
        virtual bool done() const { return _cursor == _end; }

        SqlErrorObject _errObj;
        TupleListIter _cursor;
        TupleListIter _end;
    };

private:
    DbColumns _dbColumns;
    DbTableColumns _dbTableColumns;
}; // class MockSql

}}} // lsst::qserv::sql
// Local Variables:
// mode:c++
// comment-column:0
// End:
#endif // LSST_QSERV_SQL_MOCKSQL_H
