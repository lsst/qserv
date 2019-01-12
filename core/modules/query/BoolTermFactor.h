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


#ifndef LSST_QSERV_QUERY_BOOLTERMFACTOR_H
#define LSST_QSERV_QUERY_BOOLTERMFACTOR_H


#include <ostream>

#include "query/BoolFactorTerm.h"
#include "query/ColumnRef.h"


namespace lsst {
namespace qserv {
namespace query {


class BoolTerm;
class ValueExpr;


/// BoolTermFactor is a bool factor term that contains a bool term. Occurs often
/// when parentheses are used within a bool term. The parenthetical group is an
/// entire factor, and it contains bool terms.
class BoolTermFactor : public BoolFactorTerm {
public:
    typedef std::shared_ptr<BoolTermFactor> Ptr;

    BoolTermFactor() {}
    BoolTermFactor(std::shared_ptr<BoolTerm> term) : _term(term) {}

    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;

    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const;
    virtual void findColumnRefs(ColumnRef::Vector& vector) const;

    bool operator==(const BoolFactorTerm& rhs) const override;

    std::shared_ptr<BoolTerm> _term;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_BOOLTERMFACTOR_H
