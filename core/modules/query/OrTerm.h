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


#ifndef LSST_QSERV_QUERY_ORTERM_H
#define LSST_QSERV_QUERY_ORTERM_H


#include "query/LogicalTerm.h"


namespace lsst {
namespace qserv {
namespace query {


/// OrTerm is a set of OR-connected BoolTerms
class OrTerm : public LogicalTerm {
public:
    using LogicalTerm::LogicalTerm;

    typedef std::shared_ptr<OrTerm> Ptr;

    virtual char const* getName() const { return "OrTerm"; }
    virtual OpPrecedence getOpPrecedence() const { return OR_PRECEDENCE; }

    virtual void renderTo(QueryTemplate& qt) const;
    virtual std::shared_ptr<BoolTerm> clone() const;
    virtual std::shared_ptr<BoolTerm> copySyntax() const;
    // copy is like copySyntax, but returns an OrTerm ptr.
    Ptr copy() const;

    bool merge(const BoolTerm& other) override;

    bool operator==(const BoolTerm& rhs) const override;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ORTERM_H
