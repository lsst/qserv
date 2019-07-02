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


// System headers
#include <map>
#include <vector>

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

    MockSql(DbTableColumns const& dbTableColumns) : _dbTableColumns(dbTableColumns) {}

    void reset(mysql::MySqlConfig const& sc) override {}

    bool connectToDb(SqlErrorObject&) override { return false; }

    bool selectDb(std::string const& dbName, SqlErrorObject&) override { return false; }

    bool runQuery(char const* query, int qSize, SqlResults& results, SqlErrorObject&) override {
        return false;
    }

    bool runQuery(char const* query, int qSize, SqlErrorObject&) override { return false; }

    bool runQuery(std::string const query, SqlResults&, SqlErrorObject&) override { return false; }

    std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query) override;

    bool runQuery(std::string const query, SqlErrorObject&) override { return false; }

    bool dbExists(std::string const& dbName, SqlErrorObject&) override { return false; }

    bool createDb(std::string const& dbName, SqlErrorObject&, bool failIfExists=true) override {
        return false;
    }

    bool createDbAndSelect(std::string const& dbName, SqlErrorObject&, bool failIfExists=true) override {
        return false;
    }

    bool dropDb(std::string const& dbName, SqlErrorObject&, bool failIfDoesNotExist=true) override {
        return false;
    }

    bool tableExists(std::string const& tableName, SqlErrorObject&, std::string const& dbName="") override {
        return false;
    }

    bool dropTable(std::string const& tableName, SqlErrorObject&, bool failIfDoesNotExist=true,
                   std::string const& dbName="") override {
        return false;
    }

    bool listTables(std::vector<std::string>&, SqlErrorObject&, std::string const& prefixed="",
                    std::string const& dbName="") override {
        return false;
    }

    /**
     * @brief Get the names of the columns in the given db and table
     *
     * @throws NoSuchDb, NoSuchTable, if the database or table do not exist, or an SqlException for other
     *         failures.
     *
     * @param dbName The name of the database to look in.
     * @param tableName The name of the table to look in.
     * @return std::vector<std::string> The column names.
     */
    std::vector<std::string> listColumns(std::string const& dbName,
                                         std::string const& tableName) override {
        // The QueryContext gets all the columns in each table used by the query and stores this information
        // for lookup later. Here we return a list of column names for a table.
        auto tableItr = _dbTableColumns.find(dbName);
        if (tableItr == _dbTableColumns.end()) {
            throw sql::NoSuchDb(lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__), dbName);
        }
        auto columnsItr = tableItr->second.find(tableName);
        if (columnsItr == tableItr->second.end()) {
            throw sql::NoSuchTable(lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__),
                    dbName, tableName);
        }
        std::vector<std::string> columns;
        for (auto const& column : columnsItr->second) {
            columns.push_back(column);
        }
        return columns;
    }

    std::string getActiveDbName() const override { return std::string(); }

    template <class TupleListIter>
    struct Iter : public SqlResultIter {
        Iter() {}
        Iter(TupleListIter begin, TupleListIter end) {
            _cursor = begin;
            _end = end;
        }
        virtual ~Iter() {}
        SqlErrorObject& getErrorObject() override { return _errObj; }
        StringVector const& operator*() const override { return *_cursor; }
        SqlResultIter& operator++() override { ++_cursor; return *this; }
        bool done() const override { return _cursor == _end; }

        SqlErrorObject _errObj;
        TupleListIter _cursor;
        TupleListIter _end;
    };

    unsigned long long getInsertId() const override;

    std::string escapeString(std::string const& rawString) const override;

    bool escapeString(std::string const& rawString, std::string& escapedString,
                      SqlErrorObject& errObj) override;

private:
    DbTableColumns _dbTableColumns;
};


}}} // lsst::qserv::sql


#endif // LSST_QSERV_SQL_MOCKSQL_H
