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
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include "parser/ColumnRefMap.h"
#include "query/TableRefN.h"

namespace lsst {
namespace qserv {
namespace master {

// FromList is a representation of SQL FROM.
class FromList {
public:
    FromList() : _columnRefMap(new ColumnRefMap()) {}
    explicit FromList(TableRefnListPtr p) : _tableRefns(p) {}
    ~FromList() {}
    boost::shared_ptr<ColumnRefMap> getColumnRefMap() {
        return _columnRefMap;
    }
    /// @return a list of TableRefN that occur
    TableRefnList& getTableRefnList() { return *_tableRefns; }
    /// @return a list of TableRefN that occur
    TableRefnList const& getTableRefnList() const { return *_tableRefns; }
    /// @return a flattened string representation.
    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;

    /// Deep-copy this node
    boost::shared_ptr<FromList> copyDeep() const;
    /// Shallow copy this node, sharing its linked objects.
    boost::shared_ptr<FromList> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, FromList const& fl);
    friend class FromFactory;

    boost::shared_ptr<ColumnRefMap> _columnRefMap;
    TableRefnListPtr _tableRefns;
};
}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_FROMLIST_H
