// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2019 LSST Corporation.
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
  * @file
  *
  * @brief WhereClause is a parsed SQL WHERE; QsRestrictor is a queryspec
  * spatial restrictor.
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_WHERECLAUSE_H
#define LSST_QSERV_QUERY_WHERECLAUSE_H


// System headers
#include <iostream>
#include <memory>
#include <stack>
#include <vector>


// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class WhereFactory;
}
namespace query {
    class BoolTerm;
    class ColumnRef;
    class LogicalTerm;
    class AndTerm;
    class OrTerm;
    class QsRestrictor;
    class QueryTemplate;
    class ValueExpr;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// WhereClause is a SQL WHERE containing QsRestrictors and a BoolTerm tree.
class WhereClause {
public:
    WhereClause() {}
    ~WhereClause() {}

    std::shared_ptr<std::vector<std::shared_ptr<QsRestrictor>> const> getRestrs() const {
        return _restrs;
    }
    std::shared_ptr<OrTerm>& getRootTerm() { return _rootOrTerm; }

    // Set the root term of the where clause. If `term` is an OrTerm, this will be the root term. If not then
    // an OrTerm that contains term will be the root term.
    void setRootTerm(std::shared_ptr<LogicalTerm> const& term);

    std::shared_ptr<std::vector<std::shared_ptr<ColumnRef>> const> getColumnRefs() const;
    std::shared_ptr<AndTerm> getRootAndTerm() const;

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    std::shared_ptr<WhereClause> clone() const;
    std::shared_ptr<WhereClause> copySyntax();

    void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& list) const;

    void resetRestrs();
    void prependAndTerm(std::shared_ptr<BoolTerm> t);

    bool operator==(WhereClause const& rhs) const;

private:
    std::shared_ptr<AndTerm> _addRootAndTerm();

    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend std::ostream& operator<<(std::ostream& os, WhereClause const* wc);

    friend class parser::WhereFactory;

    std::shared_ptr<OrTerm> _rootOrTerm;
    std::shared_ptr<std::vector<std::shared_ptr<QsRestrictor>>> _restrs {
        std::make_shared<std::vector<std::shared_ptr<QsRestrictor>>>()
    };
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_WHERECLAUSE_H
