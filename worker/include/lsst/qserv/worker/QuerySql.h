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
#ifndef LSST_QSERV_WORKER_QUERYSQL_H
#define LSST_QSERV_WORKER_QUERYSQL_H
 /**
  * @file QuerySql.h  
  *
  * @brief QuerySql is a bundle of SQL statements that represent an accepted
  * query's generated SQL.
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include <deque>
#include <boost/shared_ptr.hpp>

#include "lsst/qserv/worker/Task.h"
namespace lsst { namespace qserv { namespace worker {
class Task;

class QuerySql {
public:
    typedef std::deque<std::string> StringList;
    QuerySql() {}

    StringList buildList;
    StringList executeList; // Consider using SqlFragmenter to break this up into fragments.
    StringList cleanupList;
    class Factory;
    class Batch;
    friend std::ostream& operator<<(std::ostream& os, QuerySql const& q);
};

class QuerySql::Factory {
public:
    boost::shared_ptr<QuerySql> make(std::string const& db, 
                                     int chunkId,
                                     Task::Fragment const& f,
                                     bool needCreate,
                                     std::string const& defaultResultTable);
};

}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_QUERYSQL_H

