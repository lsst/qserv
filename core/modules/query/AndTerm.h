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


#ifndef LSST_QSERV_QUERY_ANDTERM_H
#define LSST_QSERV_QUERY_ANDTERM_H


#include "query/LogicalTerm.h"


namespace lsst {
namespace qserv {
namespace query {


/// AndTerm is a set of AND-connected BoolTerms
class AndTerm : public LogicalTerm {
public:
    using LogicalTerm::LogicalTerm;

    typedef std::shared_ptr<AndTerm> Ptr;

    char const* getName() const override { return "AndTerm"; }
    OpPrecedence getOpPrecedence() const override { return AND_PRECEDENCE; }

    void renderTo(QueryTemplate& qt) const override;

    std::shared_ptr<BoolTerm> clone() const override;
    std::shared_ptr<BoolTerm> copySyntax() const override;

    bool merge(const BoolTerm& other) override;

    bool operator==(const BoolTerm& rhs) const;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ANDTERM_H
