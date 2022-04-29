// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_BOOLTERM_H
#define LSST_QSERV_QUERY_BOOLTERM_H

// System headers
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// Third-party headers
#include "boost/iterator_adaptors.hpp"

// Local headers
#include "global/stringTypes.h"
#include "query/ColumnRef.h"
#include "typedefs.h"
#include "util/PointerCompare.h"

// Forward declarations
namespace lsst { namespace qserv { namespace query {
class BoolFactorTerm;
class QueryTemplate;
class ValueExpr;
}}}  // namespace lsst::qserv::query

namespace lsst { namespace qserv { namespace query {

/// BoolTerm is a representation of a boolean-valued term in a SQL WHERE
class BoolTerm {
public:
    typedef std::shared_ptr<BoolTerm> Ptr;
    typedef std::vector<Ptr> PtrVector;

    virtual ~BoolTerm() = default;

    /// Get the class name.
    virtual char const* getName() const { return "BoolTerm"; }

    /// Definition of the precidence order for different operators.
    enum OpPrecedence {
        OTHER_PRECEDENCE = 3,   // terms joined stronger than AND -- no parens needed
        AND_PRECEDENCE = 2,     // terms joined by AND
        OR_PRECEDENCE = 1,      // terms joined by OR
        UNKNOWN_PRECEDENCE = 0  // terms joined by ??? -- always add parens
    };

    /// Get the operator precidence for this class.
    virtual OpPrecedence getOpPrecedence() const { return UNKNOWN_PRECEDENCE; }

    /// Get a vector of the ValueExprs this contains.
    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {}

    /// Get a vector of pointers to the ValueExprs this contains.
    virtual void findValueExprRefs(ValueExprPtrRefVector& list) {}

    /// Get a vector of the ColumnRefs this contains.
    virtual void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {}

    /// Get a mutable vector iterator for the contained terms
    virtual PtrVector::iterator iterBegin() { return PtrVector::iterator(); }

    /// Get the terminal iterator
    virtual PtrVector::iterator iterEnd() { return PtrVector::iterator(); }

    /// Get the reduced form of this term, or null if no reduction is possible.
    virtual std::shared_ptr<BoolTerm> getReduced() { return Ptr(); }

    /// Write a human-readable version of this instance to the ostream for debug output.
    virtual std::ostream& putStream(std::ostream& os) const = 0;

    /// Serialze this instance as SQL to the QueryTemplate.
    virtual void renderTo(QueryTemplate& qt) const = 0;

    /// Make a deep copy of this term.
    virtual std::shared_ptr<BoolTerm> clone() const = 0;

    /// Make a shallow copy of this term.
    virtual std::shared_ptr<BoolTerm> copySyntax() const;

    /**
     * @brief Merge this term with the other term if possible.
     *
     * @note If two BoolTerm subclasses are of the same type then the terms of the other instance can be
     * added to the terms of this instance and the other instance can be thrown away.
     *
     * @param other[in] the BoolTerm subclass instance to try to merge with this one.
     * @returns true if the terms were merged and false if not.
     */
    virtual bool merge(BoolTerm const& other) { return false; }

    virtual bool operator==(BoolTerm const& rhs) const = 0;

    friend std::ostream& operator<<(std::ostream& os, BoolTerm const& bt);
    friend std::ostream& operator<<(std::ostream& os, BoolTerm const* bt);

protected:
    /// Serialize this instance to os for debug output.
    virtual void dbgPrint(std::ostream& os) const = 0;

    /**
     * @brief Render a list of BoolTerm to the QueryTemplate.
     *
     * @note Used by subclasses that own a list of BoolTerm. Uses `this` to determine operator precidence.
     *
     * @param qt[out] The QueryTemplate to which this is rendered.
     * @param terms[in] The terms to render.
     * @param sep[in] The separation string to put between terms.
     */
    void renderList(QueryTemplate& qt, BoolTerm::PtrVector const& terms, std::string const& sep) const;

    /**
     * @brief Render a list of BoolFactorTerm to the QueryTemplate.
     *
     * @note Used by subclasses that own a list of BoolFactorTerm. Uses `this` to determine operator
     * precidence.
     *
     * @param qt[out] The QueryTemplate to which this is rendered.
     * @param terms[in] The terms to render.
     * @param sep[in] The separation string to put between terms.
     */
    void renderList(QueryTemplate& qt, std::vector<std::shared_ptr<BoolFactorTerm>> const& terms,
                    std::string const& sep) const;
};

}}}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_BOOLTERM_H
