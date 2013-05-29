// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_FROMLIST_H
#define LSST_QSERV_MASTER_FROMLIST_H
/**
  * @file FromList.h
  *
  * @brief FromList is a representation of the contents of a SQL query's FROM
  * list. 
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/ColumnRefList.h"
#include "lsst/qserv/master/TableRefN.h"

namespace lsst { namespace qserv { namespace master {
// FromList is a representation of SQL FROM.
class FromList {
public:
    FromList() : _columnRefList(new ColumnRefList()) {}
    ~FromList() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }
    TableRefnList& getTableRefnList() { return *_tableRefns; }
    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;

    boost::shared_ptr<FromList> copyDeep() const;
    boost::shared_ptr<FromList> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, FromList const& fl);
    friend class FromFactory;

    boost::shared_ptr<ColumnRefList> _columnRefList;
    TableRefnListPtr _tableRefns;
};
}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_FROMLIST_H

