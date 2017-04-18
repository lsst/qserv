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
#include "query/Constraint.h"
#include "sql/SqlConnection.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.SecondaryIndex");

enum QueryType { IN, BETWEEN };

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {

class SecondaryIndex::Backend {
public:
    virtual ~Backend() {}
    /// Lookup an index constraint. Ignore constraints that are not "sIndex"
    /// constraints.
    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) = 0;
};

class MySqlBackend : public SecondaryIndex::Backend {
public:
    MySqlBackend(mysql::MySqlConfig const& c)
        : _sqlConnection(c, true) {
    }

    ChunkSpecVector lookup(query::ConstraintVector const& cv) override {
        ChunkSpecVector output;
        bool hasIndex = false;
        for(query::ConstraintVector::const_iterator i=cv.begin(), e=cv.end();
            i != e;
            ++i) {
            if (i->name == "sIndex"){
                hasIndex = true;
                _sqlLookup(output, i->params, IN);
            }
            else if (i->name == "sIndexBetween") {
                hasIndex = true;
                _sqlLookup(output, i->params, BETWEEN);
            }
        }
        if (!hasIndex) {
            throw SecondaryIndex::NoIndexConstraint();
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
        if (query_type == QueryType::IN) {
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
            sql += " IN (" + secondaryVals + ")";

        } else if (query_type == QueryType::BETWEEN) {
            if (params.size() != 5) {
                throw Bug("Incorrect parameters for bounded secondary index lookup ");
            }
            std::string const& par3 = *(iter++);
            std::string const& par4 = *iter;
            sql += " BETWEEN " + par3 + " AND " + par4;
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
        for(std::shared_ptr<sql::SqlResultIter> results = _sqlConnection.getQueryIter(sql);
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

    sql::SqlConnection _sqlConnection;
};

class FakeBackend : public SecondaryIndex::Backend {
public:
    FakeBackend() {}
    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) {
        ChunkSpecVector dummy;
        if (_hasSecondary(cv)) {
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
        bool operator()(query::Constraint const& c) {
            return (c.name == "sIndex" || c.name == "sIndexBetween"); }
    };
    bool _hasSecondary(query::ConstraintVector const& cv) {
        return cv.end() != std::find_if(cv.begin(), cv.end(), _checkIndex());
    }
};

SecondaryIndex::SecondaryIndex(mysql::MySqlConfig const& c)
    : _backend(std::make_shared<MySqlBackend>(c)) {
}

SecondaryIndex::SecondaryIndex()
    : _backend(std::make_shared<FakeBackend>()) {
}

ChunkSpecVector SecondaryIndex::lookup(query::ConstraintVector const& cv) {
    if (_backend) {
        return _backend->lookup(cv);
    } else {
        throw Bug("SecondaryIndex : no backend initialized");
    }
}

}}} // namespace lsst::qserv::qproc

