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
// X is a ...

#ifndef LSST_QSERV_SQLRESULTS_H
#define LSST_QSERV_SQLRESULTS_H

namespace lsst { namespace qserv {
class SqlResults {
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



}} // namespace lsst::qserv


#endif // LSST_QSERV_SQLRESULTS_H

