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

namespace lsst {
namespace qserv {
namespace query {


class BoolFactorTerm;
class QueryTemplate;
class ValueExpr;


/// BoolTerm is a representation of a boolean-valued term in a SQL WHERE
class BoolTerm {
public:
    typedef std::shared_ptr<BoolTerm> Ptr;
    typedef std::vector<Ptr> PtrVector;

    virtual ~BoolTerm() {}
    virtual char const* getName() const { return "BoolTerm"; }

    enum OpPrecedence {
        OTHER_PRECEDENCE   = 3,  // terms joined stronger than AND -- no parens needed
        AND_PRECEDENCE     = 2,  // terms joined by AND
        OR_PRECEDENCE      = 1,  // terms joined by OR
        UNKNOWN_PRECEDENCE = 0   // terms joined by ??? -- always add parens
    };

    virtual OpPrecedence getOpPrecedence() const { return UNKNOWN_PRECEDENCE; }

    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {}
    virtual void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {}

    /// @return a mutable vector iterator for the contained terms
    virtual PtrVector::iterator iterBegin() { return PtrVector::iterator(); }
    /// @return the terminal iterator
    virtual PtrVector::iterator iterEnd() { return PtrVector::iterator(); }

    /// @return the reduced form of this term, or null if no reduction is
    /// possible.
    virtual std::shared_ptr<BoolTerm> getReduced() { return Ptr(); }

    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    /// Deep copy this term.
    virtual std::shared_ptr<BoolTerm> clone() const = 0;

    virtual std::shared_ptr<BoolTerm> copySyntax() const {
        return std::shared_ptr<BoolTerm>(); }

    /// Merge is implemented in subclasses; if they are of the same type (that is, if the subclass instance is
    /// e.g. an AndTerm and `other` is also an AndTerm, then the _terms of the other AndTerm can be added to
    /// this and the other AndTerm can be thrown away (by the caller, if they desire).
    /// Returns true if the terms were merged, and false if not (this could happen e.g. if this is an AndTerm
    /// and other is an OrTerm, or for any other reason implemented by subclass's merge function.
    virtual bool merge(const BoolTerm& other) { return false; }

    virtual bool operator==(const BoolTerm& rhs) const = 0;

    friend std::ostream& operator<<(std::ostream& os, BoolTerm const& bt);
    friend std::ostream& operator<<(std::ostream& os, BoolTerm const* bt);

protected:
    virtual void dbgPrint(std::ostream& os) const = 0;

    // If a BoolTerm owns a list of BoolTerms, this can be used to render the list.
    void renderList(QueryTemplate& qt, BoolTerm::PtrVector const& terms, std::string const& sep) const;
    // If a BoolTerm owns a list of BoolFactorTerms, this can be used to render the list.
    void renderList(QueryTemplate& qt, std::vector<std::shared_ptr<BoolFactorTerm>> const& terms,
                    std::string const& sep) const;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_BOOLTERM_H
