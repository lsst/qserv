/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file QuerySql.cc
  *
  * @brief QuerySql is a bundle of SQL statements that represent an accepted
  * query's generated SQL.
  *
  * FIXME: Unfinished infrastructure for passing subchunk table name to worker.
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include "lsst/qserv/worker/QuerySql.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/constants.h"
#include <iostream>

namespace qWorker = lsst::qserv::worker;

namespace { 
template <typename T>
class ScScriptBuilder {
public:
    ScScriptBuilder(qWorker::QuerySql& qSql_,
                    std::string const& db, std::string const& table, 
                    std::string const& scColumn, 
                    int chunkId) : qSql(qSql_) {
        buildT = (boost::format(qWorker::CREATE_SUBCHUNK_SCRIPT)
                  % db % table % scColumn
                  % chunkId % "%1%").str(); 
        cleanT = (boost::format(qWorker::CLEANUP_SUBCHUNK_SCRIPT)
                  % db % table 
                  % chunkId % "%1%").str();

    }
    void operator()(T const& subc) {
        qSql.buildList.push_back((boost::format(buildT) % subc).str());
        qSql.cleanupList.push_back((boost::format(cleanT) % subc).str());
    }
    std::string buildT;
    std::string cleanT;
    qWorker::QuerySql& qSql;
};
} // anonymous namespace

namespace lsst { namespace qserv { namespace worker {
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
// QuerySql::Factory
////////////////////////////////////////////////////////////////////////
boost::shared_ptr<QuerySql>
QuerySql::Factory::newQuerySql(std::string const& db, 
                               int chunkId,
                               Task::Fragment const& f, 
                               bool needCreate,
                               std::string const& defaultResultTable) {
    boost::shared_ptr<QuerySql> qSql(new QuerySql);
    std::string resultTable;
    if(f.has_resulttable()) { resultTable = f.resulttable(); }
    else { resultTable = defaultResultTable; }
    assert(!resultTable.empty());
        
    // Create executable statement.
    // Obsolete when results marshalling is implemented
    std::stringstream ss;
    for(int i=0; i < f.query_size(); ++i) {
        if(needCreate) { 
            ss << "CREATE TABLE " << resultTable << " ";
            needCreate = false;
        } else {
            ss << "INSERT INTO " << resultTable << " "; 
        }
        ss << f.query(i);
        qSql->executeList.push_back(ss.str());
        ss.str("");
    }

    std::string table("Object");
    if(f.has_subchunks()) {
        TaskMsg_Subchunk const& sc = f.subchunks();
        for(int i=0; i < sc.table_size(); ++i) {
//            std::cout << "Building subchunks for table=" << sc.table(i) << std::endl;
            table = sc.table(i);
            ScScriptBuilder<int> scb(*qSql, db, table, 
                                     SUB_CHUNK_COLUMN, chunkId);
            for(int i=0; i < sc.id_size(); ++i) {
                scb(sc.id(i));
            }
        }
    } 
    return qSql;
}

}}} // lsst::qserv::worker


