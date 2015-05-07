// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "global/Bug.h"
#include "global/intTypes.h"
#include "global/constants.h"
#include "global/stringUtil.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace qproc {
char const lookupSqlTemplate[] = "SELECT chunkId, subChunkId FROM %s WHERE %s IN (%s);";

std::string makeIndexTableName(
    std::string const& db,
    std::string const& table
) {
    return (std::string(SEC_INDEX_DB) + "."
            + sanitizeName(db) + "__" + sanitizeName(table));
}

std::string makeLookupSql(
    std::string const& db,
    std::string const& table,
    std::string const& keyColumn,
    std::string const& stringValues
) {
    // Template: "SELECT chunkId, subChunkId FROM %s WHERE %s IN (%s);";
    std::string s;
    s += std::string("SELECT ") + CHUNK_COLUMN + "," + SUB_CHUNK_COLUMN
        + " FROM " + makeIndexTableName(db, table) + " WHERE "
        + keyColumn + " IN " + "(";
    s += stringValues + ")";
    return s;
}

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

    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) {
        ChunkSpecVector output;
        bool hasIndex = false;
        for(query::ConstraintVector::const_iterator i=cv.begin(), e=cv.end();
            i != e;
            ++i) {
            if (i->name == "sIndex") {
                lookup(output, *i);
                hasIndex = true;
            }
        }
        if(!hasIndex) {
            throw SecondaryIndex::NoIndexConstraint();
        }
        normalize(output);
        return output;
    }

    virtual void lookup(ChunkSpecVector& output, query::Constraint const& c) {
        IntVector ids;
        if (c.name != "sIndex") {
            throw Bug("Unexpected non-index constraint");
        }
        std::string lookupSql = makeLookupSql(c.params[0], c.params[1],
                                              c.params[2], c.params[3]);
        ChunkSpecMap m;
        for(std::shared_ptr<sql::SqlResultIter> results
                = _sqlConnection.getQueryIter(lookupSql);
            !results->done();
            ++(*results)) {
            StringVector const& row = **results;
            int chunkId = boost::lexical_cast<int>(row[0]);
            int subChunkId = boost::lexical_cast<int>(row[1]);
            ChunkSpecMap::iterator e = m.find(chunkId);
            if (e == m.end()) {
                ChunkSpec& cs = m[chunkId];
                cs.chunkId = chunkId;
                cs.subChunks.push_back(subChunkId);
            } else {
                ChunkSpec& cs = e->second;
                cs.subChunks.push_back(subChunkId);
            }
        }
        for(ChunkSpecMap::const_iterator i=m.begin(), e=m.end();
            i != e; ++i) {
            output.push_back(i->second);
        }
    }
private:
    sql::SqlConnection    SqlConnection();
    sql::SqlConnection _sqlConnection;

};

class FakeBackend : public SecondaryIndex::Backend {
public:
    FakeBackend() {}
    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) {
        ChunkSpecVector dummy;
        if(_hasSecondary(cv)) {
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
            return c.name == "sIndex"; }
    };
    bool _hasSecondary(query::ConstraintVector const& cv) {
        return cv.end() != std::find_if(cv.begin(), cv.end(), _checkIndex());
    }
};

SecondaryIndex::SecondaryIndex(mysql::MySqlConfig const& c)
    : _backend(std::make_shared<MySqlBackend>(c)) {
}

SecondaryIndex::SecondaryIndex(int)
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

