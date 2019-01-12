// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_LOGICALTERM_H
#define LSST_QSERV_QUERY_LOGICALTERM_H


#include "query/BoolTerm.h"


namespace lsst {
namespace qserv {
namespace query {


class BoolFactor;
class ColumnRef;
class ValueExpr;


class LogicalTerm : public BoolTerm {
public:

    LogicalTerm() {}
    LogicalTerm(std::vector<std::shared_ptr<BoolTerm>> const& terms) : _terms(terms) {}
    LogicalTerm(std::shared_ptr<BoolTerm> const & term) : _terms({term}) {}

    virtual std::vector<std::shared_ptr<BoolTerm>>::iterator iterBegin() { return _terms.begin(); }
    virtual std::vector<std::shared_ptr<BoolTerm>>::iterator iterEnd() { return _terms.end(); }

    void addBoolTerm(std::shared_ptr<BoolTerm> boolTerm);

    void setBoolTerms(const std::vector<std::shared_ptr<BoolTerm>>& terms);

    void setBoolTerms(const std::vector<std::shared_ptr<BoolFactor>>& terms); // not needed? BoolFactor inherits from BoolTerm...

    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const;

    virtual void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const;

    virtual std::shared_ptr<BoolTerm> getReduced();

    virtual std::ostream& putStream(std::ostream& os) const;

    std::vector<std::shared_ptr<BoolTerm>> _terms;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_LOGICALTERM_H
