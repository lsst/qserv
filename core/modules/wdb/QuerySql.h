// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_WDB_QUERYSQL_H
#define LSST_QSERV_WDB_QUERYSQL_H
 /**
  * @file QuerySql.h
  *
  * @brief QuerySql is a bundle of SQL statements that represent an accepted
  * query's generated SQL.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <deque>
#include <ostream>
#include <string>
#include <boost/shared_ptr.hpp>


// Forward declarations
namespace lsst {
namespace qserv {
namespace proto {
    class TaskMsg_Fragment;
}
namespace wdb {
    class Task;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wdb {

class QuerySql {
public:
typedef boost::shared_ptr<QuerySql> Ptr;
    typedef std::deque<std::string> StringDeque;
    typedef lsst::qserv::proto::TaskMsg_Fragment Fragment;

    QuerySql() {}
    QuerySql(std::string const& db,
             int chunkId,
             proto::TaskMsg_Fragment const& f,
             bool needCreate,
             std::string const& defaultResultTable);

    StringDeque buildList;
    StringDeque executeList; // Consider using SqlFragmenter to break this up into fragments.
    StringDeque cleanupList;
    class Batch;
    friend std::ostream& operator<<(std::ostream& os, QuerySql const& q);
};

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_QUERYSQL_H
