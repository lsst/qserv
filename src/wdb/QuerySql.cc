// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
  * @brief QuerySql is a bundle of SQL statements that represent an accepted
  * query's generated SQL.
  *
  * FIXME: Unfinished infrastructure for passing subchunk table name to worker.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "wdb/QuerySql.h"

// System headers
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "global/DbTable.h"
#include "proto/worker.pb.h"
#include "wbase/Base.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wdb.QuerySql");

template <typename T>
class ScScriptBuilder {
public:
    ScScriptBuilder(lsst::qserv::wdb::QuerySql& qSql_,
                    std::string const& db, std::string const& table,
                    std::string const& scColumn,
                    int chunkId) : qSql(qSql_) {
        buildT = (boost::format(lsst::qserv::wbase::CREATE_SUBCHUNK_SCRIPT)
                  % db % table % scColumn
                  % chunkId % "%1%").str();
        cleanT = (boost::format(lsst::qserv::wbase::CLEANUP_SUBCHUNK_SCRIPT)
                  % db % table
                  % chunkId % "%1%").str();

    }
    void operator()(T const& subc) {
        qSql.buildList.push_back((boost::format(buildT) % subc).str());
        qSql.cleanupList.push_back((boost::format(cleanT) % subc).str());
    }
    std::string buildT;
    std::string cleanT;
    lsst::qserv::wdb::QuerySql& qSql;
};
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace wdb {

////////////////////////////////////////////////////////////////////////
// QuerySql ostream friend
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, QuerySql const& q) {
    os << "QuerySql(bu=";
    std::copy(q.buildList.begin(), q.buildList.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << "; ex=";
    std::copy(q.executeList.begin(), q.executeList.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << "; cl=";
    std::copy(q.cleanupList.begin(), q.cleanupList.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << ")";
    return os;
}

////////////////////////////////////////////////////////////////////////
// QuerySql constructor
////////////////////////////////////////////////////////////////////////
QuerySql::QuerySql(std::string const& db,
                   int chunkId,
                   proto::TaskMsg_Fragment const& f,
                   bool needCreate,
                   std::string const& defaultResultTable) {

    std::string resultTable;
    if (f.has_resulttable()) { resultTable = f.resulttable(); }
    else { resultTable = defaultResultTable; }
    assert(!resultTable.empty());

    // Create executable statement.
    // Obsolete when results marshalling is implemented
    std::stringstream ss;
    for(int i=0; i < f.query_size(); ++i) {
        if (needCreate) {
            ss << "CREATE TABLE " + resultTable + " ";
            needCreate = false;
        } else {
            ss << "INSERT INTO " + resultTable + " ";
        }
        ss << f.query(i);
        executeList.push_back(ss.str());
        ss.str("");
    }

    if (f.has_subchunks()) {
        proto::TaskMsg_Subchunk const& sc = f.subchunks();
        for(int i=0; i < sc.dbtbl_size(); ++i) {
            DbTable dbTable(sc.dbtbl(i).db(), sc.dbtbl(i).tbl());
            LOGS(_log, LOG_LVL_DEBUG, "Building subchunks for table=" << dbTable << " chunkId=" << chunkId);
            ScScriptBuilder<int> scb(*this, dbTable.db, dbTable.table, SUB_CHUNK_COLUMN, chunkId);
            for(int i=0; i < sc.id_size(); ++i) {
                scb(sc.id(i));
            }
        }
    }
}

}}} // namespace lsst::qserv::wdb
