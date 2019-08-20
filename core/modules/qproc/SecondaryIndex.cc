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
  * @brief SecondaryIndex implementation
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/SecondaryIndex.h"

// System headers
#include <algorithm>

// LSST headers
#include "lsst/log/Log.h"

// Third-party headers
#include "boost/format.hpp"

// Qserv headers
#include "global/Bug.h"
#include "global/intTypes.h"
#include "global/constants.h"
#include "global/stringUtil.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/QsRestrictor.h"
#include "qproc/ChunkSpec.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.SecondaryIndex");

enum QueryType { IN, NOT_IN, BETWEEN, NOT_BETWEEN };

/**
 * @brief Get the sql string for the given query type.
 *
 * Includes the whitespace around the string value, simply because it was easier to use that way.
 *
 * @param queryType The query type to get the string for.
 * @return std::string The sql string to use for the given query type.
 */
std::string toSqlStr(QueryType queryType) {
    switch (queryType) {
        case IN:
            return " IN";

        case NOT_IN:
            return " NOT IN";

        case BETWEEN:
            return " BETWEEN ";

        case NOT_BETWEEN:
            return " NOT BETWEEN ";

        default:
            throw lsst::qserv::Bug("Unhandled QueryType: " + queryType);
    }
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {

class SecondaryIndex::Backend {
public:
    virtual ~Backend() {}
    /// Lookup an index restrictor. Ignore restrictors that are not "sIndex" restrictors.
    virtual ChunkSpecVector lookup(query::QsRestrictor::PtrVector const& restrictors) = 0;
};

class MySqlBackend : public SecondaryIndex::Backend {
public:
    MySqlBackend(mysql::MySqlConfig const& c)
        : _sqlConnection(sql::SqlConnectionFactory::make(c)) {
    }

    ChunkSpecVector lookup(query::QsRestrictor::PtrVector const& restrictors) override {
        ChunkSpecVector output;
        bool hasIndex = false;
        for(auto const& restrBase : restrictors) {
            if (auto siRestr = std::dynamic_pointer_cast<query::SIRestrictor>(restrBase)) {
                // handle it
                hasIndex = true;
                auto const& secondaryIndexCol = siRestr->getSecondaryIndexColumnRef();
                std::string index_table = _buildIndexTableName(secondaryIndexCol->getDb(),
                                                            secondaryIndexCol->getTable());
                auto sql = siRestr->getSILookupQuery(SEC_INDEX_DB, index_table, CHUNK_COLUMN,
                                                     SUB_CHUNK_COLUMN);
                LOGS(_log, LOG_LVL_DEBUG, "secondary lookup sql:" << sql);
                _sqlLookup(output, sql);
            } else {
                auto restrictor = std::dynamic_pointer_cast<query::QsRestrictorFunction>(restrBase);
                if (nullptr == restrictor) {
                    continue;
                }
                if (restrictor->getName() == "sIndex"){
                    hasIndex = true;
                    _sqlLookup(output, restrictor->getParameters(), IN);
                } else if (restrictor->getName() == "sIndexNotIn"){
                    hasIndex = true;
                    _sqlLookup(output, restrictor->getParameters(), NOT_IN);
                }
            }
        }
        if (!hasIndex) {
            throw SecondaryIndex::NoIndexRestrictor();
        }
        normalize(output);
        return output;
    }


private:
    static std::string _buildIndexTableName(
        std::string const& db,
        std::string const& table) {
        return sanitizeName(db) + "__" + sanitizeName(table);
    }

    /**
     *  Build sql query string to run against secondary index
     *
     *  @param params:  vector of string used to build the query,
     *                  format is:
     *                  [db, table, keyColumn, id_0, ..., id_n]
     *                  where:
     *                  - db.table is the director table,
     *                  - keyColumn is its primary key,
     *                  - id_x are keyColumn values
     *
     *  @param query_type: Type of the query launched against
     *                     secondary index. Use IN or BETWEEN on object ids
     *                     to find chunk ids.
     *
     *  @return:   the sql query string to run against secondary index in
     *             order to get (chunks, subchunks) couples containing [id_0, ..., id_n]
     */
    static std::string _buildLookupQuery(
        std::vector<std::string> const& params,
        QueryType const& query_type) {

        LOGS(_log, LOG_LVL_TRACE, "params: " << util::printable(params));

        auto iter = params.begin();
        std::string const& db = *(iter++); // params[0]
        std::string const& table = *(iter++); // params[1]
        std::string const& key_column = *(iter++); // params[2]

        std::string index_table = _buildIndexTableName(db, table);
        std::string sql = "SELECT " + std::string(CHUNK_COLUMN) + ", " + std::string(SUB_CHUNK_COLUMN) +
                          " FROM " + SEC_INDEX_DB + "." + index_table +
                          " WHERE " + key_column;
        if (query_type == QueryType::IN || query_type == QueryType::NOT_IN) {
            std::string secondaryVals; // params[3] to end
            bool first = true;
            // Do not use util::printable here. It adds unwanted characters.
            for (; iter != params.end(); ++iter) {
                if (first) {
                    first = false;
                } else {
                    secondaryVals += ", ";
                }
                secondaryVals += *iter;
            }
            sql += toSqlStr(query_type) + "(" + secondaryVals + ")";
        } else if (query_type == QueryType::BETWEEN || query_type == QueryType::NOT_BETWEEN) {
            if (params.size() != 5) {
                throw Bug("Incorrect parameters for bounded secondary index lookup ");
            }
            std::string const& par3 = *(iter++);
            std::string const& par4 = *iter;
            sql += toSqlStr(query_type) + par3 + " AND " + par4;
        }

        LOGS(_log, LOG_LVL_DEBUG, "secondary lookup sql:" << sql);
        return sql;
    }


    /**
     *  Add results from secondary index sql query to existing ChunkSpec vector
     *
     *  @param output:      existing ChunkSpec vector
     *  @param params:      parameters used to query secondary index
     *  @param query_type:  Type of the query launched against
     *                      secondary index. Use IN or BETWEEN on object ids
     *                      to find chunk ids.
     */
    void _sqlLookup(ChunkSpecVector& output, StringVector const& params, QueryType const& query_type) {
        _sqlLookup(output, _buildLookupQuery(params, query_type));
    }


    void _sqlLookup(ChunkSpecVector& output, std::string sql) {

        std::map<int, Int32Vector> tmp;

        // Insert sql query result:
        //   chunkId_x1, subChunkId_y1
        //   chunkId_x1, subChunkId_y2
        //   ...
        //   chunkId_xi, subChunkId_yj
        //   ...
        //   chunkId_xm, subChunkId_yn
        //
        // in a std::map<int, Int32Vector>:
        // key       , value
        // chunkId_x1, [subChunkId_y1, subChunkId_y2, ...]
        // chunkId_xi, [subChunkId_yj, ..., subChunkId_yk]
        // chunkId_xm, [subChunkId_yl, ..., subChunkId_yn]
        for(std::shared_ptr<sql::SqlResultIter> results = _sqlConnection->getQueryIter(sql);
            not results->done();
            ++(*results)) {
            StringVector const& row = **results;
            int chunkId = std::stoi(row[0]);
            int subChunkId = std::stoi(row[1]);
            tmp[chunkId].push_back(subChunkId);
        }

        // Add results to output
        for(auto i=tmp.begin(), e=tmp.end();
            i != e; ++i) {
            output.push_back(ChunkSpec(i->first, i->second));
        }
    }

    std::shared_ptr<sql::SqlConnection> _sqlConnection;
};

class FakeBackend : public SecondaryIndex::Backend {
public:
    FakeBackend() {}
    virtual ChunkSpecVector lookup(query::QsRestrictor::PtrVector const& restrictors) {
        ChunkSpecVector dummy;
        if (_hasSecondary(restrictors)) {
            for(int i=100; i < 103; ++i) {
                int bogus[] = {1,2,3};
                std::vector<int> subChunks(bogus, bogus+3);
                dummy.push_back(ChunkSpec(i, subChunks));
            }
        }
        return dummy;
    }
private:
    struct _checkIndex {
        bool operator()(std::shared_ptr<query::QsRestrictor> const& restrictor) {
            if (auto restrFunc = std::dynamic_pointer_cast<query::QsRestrictorFunction>(restrictor)) {
                return (restrFunc->getName() == "sIndex" || restrFunc->getName() == "sIndexBetween");
            }
            if (auto restrFunc = std::dynamic_pointer_cast<query::SIRestrictor>(restrictor)) {
                return true;
            }
            return false;
        }
    };
    bool _hasSecondary(query::QsRestrictor::PtrVector const& restrictors) {
        return restrictors.end() != std::find_if(restrictors.begin(), restrictors.end(), _checkIndex());
    }
};

SecondaryIndex::SecondaryIndex(mysql::MySqlConfig const& c)
    : _backend(std::make_shared<MySqlBackend>(c)) {
}

SecondaryIndex::SecondaryIndex()
    : _backend(std::make_shared<FakeBackend>()) {
}

ChunkSpecVector SecondaryIndex::lookup(query::QsRestrictor::PtrVector const& restrictors) {
    if (_backend) {
        return _backend->lookup(restrictors);
    } else {
        throw Bug("SecondaryIndex : no backend initialized");
    }
}

}}} // namespace lsst::qserv::qproc

