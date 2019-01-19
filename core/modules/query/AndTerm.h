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


// Qserv headers
#include "query/LogicalTerm.h"


namespace lsst {
namespace qserv {
namespace query {


/// AndTerm is a set of AND-connected BoolTerms
class AndTerm : public LogicalTerm {
public:
    using LogicalTerm::LogicalTerm;

    typedef std::shared_ptr<AndTerm> Ptr;

    /// Get the class name.
    char const* getName() const override { return "AndTerm"; }

    /// Get the operator precidence for this class.
    OpPrecedence getOpPrecedence() const override { return AND_PRECEDENCE; }

    /// Serialze this instance as SQL to the QueryTemplate.
    void renderTo(QueryTemplate& qt) const override;

    /// Make a deep copy of this term.
    std::shared_ptr<BoolTerm> clone() const override;

    /// Make a shallow copy of this term.
    std::shared_ptr<BoolTerm> copySyntax() const override;

    /**
     * @brief Merge this term with the other term if possible.
     *
     * @note If two BoolTerm subclasses are of the same type then the terms of the other instance can be
     * added to the terms of this instance and the other instance can be thrown away.
     *
     * @param other[in] the BoolTerm subclass instance to try to merge with this one.
     * @returns true if the terms were merged and false if not.
     */
    bool merge(BoolTerm const& other) override;

    /**
     * @brief Used with merge(BoolTerm, MergeBehavior) to indicate if terms from the other AndTerm should be
     * before or after the terms from this term.
     */
    enum MergeBehavior {PREPEND, APPEND};

    /// Like `merge(BoolTerm)` but can be told what order this term and the other term.
    bool merge(BoolTerm const& other, MergeBehavior mergeBehavior);

    bool operator==(BoolTerm const& rhs) const;

protected:
    /// Serialize this instance to os for debug output.
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_ANDTERM_H
