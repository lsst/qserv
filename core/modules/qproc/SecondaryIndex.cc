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

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {


class SecondaryIndex::Backend {
public:
    virtual ~Backend() {}
    /// Lookup an index restrictor. Ignore restrictors that are not "sIndex" restrictors.
    virtual ChunkSpecVector lookup(query::SecIdxRestrictorVec const& restrictors) = 0;
};


class MySqlBackend : public SecondaryIndex::Backend {
public:
    MySqlBackend(mysql::MySqlConfig const& c)
        : _sqlConnection(sql::SqlConnectionFactory::make(c)) {
    }

    ChunkSpecVector lookup(query::SecIdxRestrictorVec const& restrictors) override {
        ChunkSpecVector output;
        for(auto const& secIdxRestrictor : restrictors) {
            // handle it
            auto const& secondaryIndexCol = secIdxRestrictor->getSecIdxColumnRef();
            std::string index_table = _buildIndexTableName(secondaryIndexCol->getDb(),
                                                           secondaryIndexCol->getTable());
            auto sql = secIdxRestrictor->getSecIdxLookupQuery(SEC_INDEX_DB, index_table, CHUNK_COLUMN,
                                                              SUB_CHUNK_COLUMN);
            LOGS(_log, LOG_LVL_DEBUG, "secondary lookup sql:" << sql);
            _sqlLookup(output, sql);
        }
        normalize(output);
        return output;
    }


private:
    static std::string _buildIndexTableName(std::string const& db, std::string const& table) {
        return sanitizeName(db) + "__" + sanitizeName(table);
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
    virtual ChunkSpecVector lookup(query::SecIdxRestrictorVec const& restrictors) {
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
        bool operator()(std::shared_ptr<query::SecIdxRestrictor> const& restrictor) {
            return true;
        }
    };
    bool _hasSecondary(query::SecIdxRestrictorVec const& restrictors) {
        return restrictors.end() != std::find_if(restrictors.begin(), restrictors.end(), _checkIndex());
    }
};


SecondaryIndex::SecondaryIndex(mysql::MySqlConfig const& c)
    : _backend(std::make_shared<MySqlBackend>(c)) {
}

SecondaryIndex::SecondaryIndex()
    : _backend(std::make_shared<FakeBackend>()) {
}


ChunkSpecVector SecondaryIndex::lookup(query::SecIdxRestrictorVec const& restrictors) {
    if (_backend) {
        return _backend->lookup(restrictors);
    } else {
        throw Bug("SecondaryIndex : no backend initialized");
    }
}


}}} // namespace lsst::qserv::qproc

