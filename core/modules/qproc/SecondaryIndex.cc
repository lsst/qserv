// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// Third-party headers
#include "boost/make_shared.hpp" // switch to make_unique
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "global/Bug.h"
#include "global/intTypes.h"
#include "global/constants.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace qproc {
char const lookupSqlTemplate[] = "SELECT chunkId, subChunkId FROM %s WHERE %s IN (%s);";

std::string sanitizeName(
    std::string const& input) {
    return input; // FIXME. need to expose sanitizeName used in css/EmptyChunks
}

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
    //std::vector<int32_t> const& keyValues
) {
    // Template: "SELECT chunkId, subChunkId FROM %s WHERE %s IN (%s);";
    std::string s;
    s += std::string("SELECT ") + CHUNK_COLUMN + "," + SUB_CHUNK_COLUMN
        + " FROM " + makeIndexTableName(db, table) + " WHERE "
        + keyColumn + " IN " + "(";
#if 0
    std::ostringstream os;
    bool notFirst = false;
    std::vector<int32_t>::const_iterator i,e;
    for(i=keyValues.begin(), e=keyValues.end(); i != e; ++i) {
        if (notFirst) {
            os << ",";
        } else {
            notFirst = true;
        }
        os << *i;
    }
    s += os.str() + ")";
#else
    s += stringValues + ")";
#endif
    return s;
}
class SecondaryIndex::Backend {
public:
    virtual ~Backend() {}
    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) = 0;
};

class MySqlBackend : public SecondaryIndex::Backend {
public:
    MySqlBackend(mysql::MySqlConfig const& c)
        : _sqlConnection(c, true) {
    }
    virtual ChunkSpecVector lookup(query::ConstraintVector const& cv) {
    // cv should only contain index constraints
    // Because the only constraint possible is "objectid in []", and all
    // constraints are AND-ed, it really only makes sense to have one index
    // constraint.
    IntVector ids;
    // For now, use only the first constraint, assert that it is an index
    // constraint.
    if (cv.empty()) { throw Bug("SecondaryIndex::lookup without constraint"); }
    query::Constraint const& c = cv[0];
    if (c.name != "sIndex") {
        throw Bug("SecondaryIndex::lookup cv[0] != index constraint");
    }
    std::string lookupSql = makeLookupSql(c.params[0], c.params[1],
                                          c.params[2], c.params[3]);
    ChunkSpecMap m;
    for(boost::shared_ptr<sql::SqlResultIter> results
            = _sqlConnection.getQueryIter(lookupSql);
        !results->done();
        ++(*results)) {
        sql::SqlResultIter::List const& row = **results;
        int chunkId = boost::lexical_cast<int>(row[0]);
        int subChunkId = boost::lexical_cast<int>(row[1]);
        ChunkSpecMap::iterator e = m.find(chunkId);
        if (e == m.end()) {
            ChunkSpec& cs = m[row[0]];
            cs.chunkId = chunkId;
            cs.subChunks.push_back(subChunkId);
        } else {
            ChunkSpec& cs = *e;
            cs.subChunks.push_back(subChunkId);
        }
    }
    ChunkSpecVector output;
    for(ChunkSpecMap::const_iterator i=m.begin(), e=m.end();
        i != e; ++i) {
        output.push_back(i->second);
    }
    return output;
    }
private:
    sql::SqlConnection    SqlConnection();
    sql::SqlConnection _sqlConnection;

};

SecondaryIndex::SecondaryIndex(mysql::MySqlConfig const& c)
    : _backend(boost::make_shared<MySqlBackend>(c)) {
}

ChunkSpecVector SecondaryIndex::lookup(query::ConstraintVector const& cv) {
    if (_backend) {
        return _backend->lookup(cv);
    } else {
        raise Bug("SecondaryIndex : no backend initialized");
    }
}

#if 0
       logger.inf("Looking for indexhints in ", hintList)
        secIndexSpecs = ifilter(lambda t: t[0] == "sIndex", hintList)
        lookups = []
        for s in secIndexSpecs:
            params = s[1]
// db table keycolumn, values
            lookup = IndexLookup(params[0], params[1], params[2], params[3:])
            lookups.append(lookup)
            pass
        index = SecondaryIndex()
        chunkIds = index.lookup(lookups)
        logger.inf("lookup got chunks:", chunkIds)
        return chunkIds
#endif

}}} // namespace lsst::qserv::qproc

