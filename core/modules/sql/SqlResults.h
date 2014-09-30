// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// SqlResults is a class that buffers up results from a particular query. It is
// currently mysql-specific, but this may (likely) change in the future.
// Unfortunately, including SqlResults.h will pull in mysql.h since MYSQL_RES is
// used in the class declaration and MYSQL_RES is a typedef rather than a class
// or simple struct.

#ifndef LSST_QSERV_SQL_SQLRESULTS_H
#define LSST_QSERV_SQL_SQLRESULTS_H

// System headers
#include <string>
#include <vector>

// Third-party headers
#include "boost/utility.hpp"
#include <mysql/mysql.h>

// Local headers
#include "sql/SqlErrorObject.h"


namespace lsst {
namespace qserv {
namespace sql {

class SqlResults : boost::noncopyable {
public:
    SqlResults(bool discardImmediately=false)
        :_discardImmediately(discardImmediately) {};
    ~SqlResults() {freeResults();};

    void addResult(MYSQL_RES* r);
    bool extractFirstValue(std::string&, SqlErrorObject&);
    bool extractFirstColumn(std::vector<std::string>&,
                            SqlErrorObject&);
    bool extractFirst2Columns(std::vector<std::string>&, //FIXME: generalize
                              std::vector<std::string>&,
                              SqlErrorObject&);
    bool extractFirst3Columns(std::vector<std::string>&, //FIXME: generalize
                              std::vector<std::string>&,
                              std::vector<std::string>&,
                              SqlErrorObject&);
    bool extractFirst4Columns(std::vector<std::string>&,
                              std::vector<std::string>&,
                              std::vector<std::string>&,
                              std::vector<std::string>&,
                              SqlErrorObject&);
    void freeResults();

private:
    std::vector<MYSQL_RES*> _results;
    bool _discardImmediately;
};

}}} // namespace lsst::qserv:: sql

#endif // LSST_QSERV_SQL_SQLRESULTS_H
