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
// HavingClause is a representation of a SQL HAVING clause.
// HavingClause is represented much like a WhereClause, but with more
// limitations.
#ifndef LSST_QSERV_MASTER_HAVINGCLAUSE_H
#define LSST_QSERV_MASTER_HAVINGCLAUSE_H

#include <boost/shared_ptr.hpp>
#if 0
#include <deque>
#include <string>
#include "lsst/qserv/master/ValueExpr.h"
#endif

namespace lsst {
namespace qserv {
namespace master {

class QueryTemplate;
class BoolTerm;

class HavingClause {
public:
    HavingClause() {}
    ~HavingClause() {}

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<HavingClause> copyDeep();
    boost::shared_ptr<HavingClause> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, HavingClause const& h);
    friend class ModFactory;
    boost::shared_ptr<BoolTerm> _tree;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_HAVINGCLAUSE_H

