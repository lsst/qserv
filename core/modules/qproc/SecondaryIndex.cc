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
#include "qproc/ChunkSpec.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.SecondaryIndex");

enum QueryType { IN, NOT_IN, BETWEEN, NOT_BETWEEN, EQUAL, NOT_EQUAL, LESS_THAN, GREATER_THAN,
                 LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL };

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

        case EQUAL:
            return " = ";

        case NOT_EQUAL:
            return " != ";

        case LESS_THAN:
            return " < ";

        case GREATER_THAN:
            return " > ";

        case LESS_THAN_OR_EQUAL:
            return " <= ";

        case GREATER_THAN_OR_EQUAL:
            return " >= ";

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
        for(auto const& restrictor : restrictors) {
            if (restrictor->_name == "sIndex"){
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, IN);
            } else if (restrictor->_name == "sIndexNotIn"){
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, NOT_IN);
            } else if (restrictor->_name == "sIndexBetween") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, BETWEEN);
            } else if (restrictor->_name == "sIndexNotBetween") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, NOT_BETWEEN);
            } else if (restrictor->_name == "sIndexEqual") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, EQUAL);
            } else if (restrictor->_name == "sIndexNotEqual") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, NOT_EQUAL);
            } else if (restrictor->_name == "sIndexGreaterThan") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, GREATER_THAN);
            } else if (restrictor->_name == "sIndexLessThan") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, LESS_THAN);
            } else if (restrictor->_name == "sIndexGreaterThanOrEqual") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, GREATER_THAN_OR_EQUAL);
            } else if (restrictor->_name == "sIndexLessThanOrEqual") {
                hasIndex = true;
                _sqlLookup(output, restrictor->_params, LESS_THAN_OR_EQUAL);
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
        return (std::string(SEC_INDEX_DB) + "."
                + sanitizeName(db) + "__" + sanitizeName(table));
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
                          " FROM " + index_table +
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
        } else if (query_type == EQUAL || query_type == NOT_EQUAL || query_type == LESS_THAN ||
                   query_type == GREATER_THAN || query_type == LESS_THAN_OR_EQUAL ||
                   query_type == GREATER_THAN_OR_EQUAL) {
            if (params.size() != 4) {
                throw Bug("Incorrect parameters for comparison secondary index lookup ");
            }
            // todo I'm going to be able to break this by putting the key column 2nd
            std::string const& par3 = *iter;
            sql += toSqlStr(query_type) + par3;
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
        IntVector ids;

        std::string sql = _buildLookupQuery(params, query_type);
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
        return (restrictor->_name == "sIndex" || restrictor->_name == "sIndexBetween"); }
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

