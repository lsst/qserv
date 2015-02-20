// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_WHERECLAUSE_H
#define LSST_QSERV_QUERY_WHERECLAUSE_H
/**
  * @file
  *
  * @brief WhereClause is a parsed SQL WHERE; QsRestrictor is a queryspec
  * spatial restrictor.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>
#include <stack>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/QsRestrictor.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class WhereFactory;
}
namespace query {
    class BoolTerm;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// WhereClause is a SQL WHERE containing QsRestrictors and a BoolTerm tree.
class WhereClause {
public:
    WhereClause() {}
    ~WhereClause() {}

    boost::shared_ptr<QsRestrictor::PtrVector const> getRestrs() const {
        return _restrs;
    }
    boost::shared_ptr<BoolTerm const> getRootTerm() const { return _tree; }
    boost::shared_ptr<BoolTerm> getRootTerm() { return _tree; }
    boost::shared_ptr<ColumnRef::Vector const> getColumnRefs() const;
    boost::shared_ptr<AndTerm> getRootAndTerm();

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<WhereClause> clone() const;
    boost::shared_ptr<WhereClause> copySyntax();

    void findValueExprs(ValueExprPtrVector& list);

    void resetRestrs();
    void prependAndTerm(boost::shared_ptr<BoolTerm> t);

private:
    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend class parser::WhereFactory;

    std::string _original;
    boost::shared_ptr<BoolTerm> _tree;
    boost::shared_ptr<QsRestrictor::PtrVector> _restrs;

};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_WHERECLAUSE_H
