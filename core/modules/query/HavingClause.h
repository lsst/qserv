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
#ifndef LSST_QSERV_QUERY_HAVINGCLAUSE_H
#define LSST_QSERV_QUERY_HAVINGCLAUSE_H
/**
  * @file HavingClause.h
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {

namespace parser {
    // Forward
    class ModFactory;
}

namespace query {

class QueryTemplate;
class BoolTerm;

/// HavingClause: a representation of SQL HAVING. Support for this construct is
/// incomplete.
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
    friend class parser::ModFactory;
    boost::shared_ptr<BoolTerm> _tree;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_HAVINGCLAUSE_H

