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

    /// Get the class name.
    char const* getName() const override { return "OrTerm"; }

    /// Get the operator precedence for this class.
    OpPrecedence getOpPrecedence() const override { return OR_PRECEDENCE; }

    /// Serialize this instance as SQL to the QueryTemplate.
    void renderTo(QueryTemplate& qt) const override;

    /// Make a deep copy of this term.
    std::shared_ptr<BoolTerm> clone() const override;

    /// Make a shallow copy of this term.
    std::shared_ptr<BoolTerm> copySyntax() const override;

    // copy is like copySyntax, but returns an OrTerm ptr.
    Ptr copy() const;

    /**
     * @brief Merge this term with the other term if possible.
     *
     * @note If two BoolTerm subclasses are of the same type then the terms of the other instance can be
     * added to the terms of this instance and the other instance can be thrown away.
     *
     * @param other[in] the BoolTerm subclass instance to try to merge with this one.
     * @returns true if the terms were merged and false if not.
     */
    bool merge(const BoolTerm& other) override;

    bool operator==(const BoolTerm& rhs) const override;

protected:
    /// Serialize this instance to os for debug output.
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ORTERM_H
