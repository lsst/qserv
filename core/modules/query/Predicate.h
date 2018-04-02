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
#ifndef LSST_QSERV_QUERY_PREDICATE_H
#define LSST_QSERV_QUERY_PREDICATE_H
/**
  * @file
  *
  * @brief Predicate is a representation of a boolean term in a WHERE clause
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <string>

// Local headers
#include "query/typedefs.h"
#include "query/BoolTerm.h"
#include "query/ValueExpr.h"

namespace lsst {
namespace qserv {
namespace query {

// Forward
class QueryTemplate;

///  Predicate is a representation of a SQL predicate.
/// predicate :
///       row_value_constructor
///         ( comp_predicate
///         | ("not")? ( between_predicate
///                    | in_predicate
///                    | like_predicate
///                    )
///         | null_predicate
///         | quantified_comp_predicate
///         | match_predicate
///         | overlaps_predicate
///         ) {#predicate = #([PREDICATE, "PREDICATE"],predicate);}
///     | exists_predicate
///     | unique_predicate
class Predicate : public BoolFactorTerm {
public:
    typedef std::shared_ptr<Predicate> Ptr;

    virtual ~Predicate() {}
    virtual char const* getName() const { return "Predicate"; }

    friend std::ostream& operator<<(std::ostream& os, Predicate const& bt);
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;

    virtual BoolFactorTerm::Ptr copySyntax() const { return BoolFactorTerm::Ptr(); }
};

/// GenericPredicate is a Predicate whose structure whose semantic meaning
/// is unimportant for qserv
class GenericPredicate : public Predicate {
public:
    typedef std::shared_ptr<GenericPredicate> Ptr;

    virtual ~GenericPredicate() {}
    virtual char const* getName() const { return "GenericPredicate"; }

    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone(); }
};

/// CompPredicate is a Predicate involving a row value compared to another row value.
/// (literals can be row values)
class CompPredicate : public Predicate {
public:
    typedef std::shared_ptr<CompPredicate> Ptr;

    virtual ~CompPredicate() {}
    virtual char const* getName() const { return "CompPredicate"; }

    virtual void findValueExprs(ValueExprPtrVector& vector);
    virtual void findColumnRefs(ColumnRef::Vector& vector);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone(); }

    static int lookupOp(char const* op);

    std::ostream& dump(std::ostream& os) const override;

    bool equal(const BoolFactorTerm& rhs) const override {
        auto rhsCompPredicate = dynamic_cast<CompPredicate const * const>(&rhs);
        if (nullptr == rhsCompPredicate) {
            return false;
        }
        return util::pointerCompare(left, rhsCompPredicate->left) &&
               op == rhsCompPredicate->op &&
               util::pointerCompare(right, rhsCompPredicate->right);
    }

    ValueExprPtr left;
    int op; // Parser token type of operator
    ValueExprPtr right;
};

/// InPredicate is a Predicate comparing a row value to a set
class InPredicate : public Predicate {
public:
    typedef std::shared_ptr<InPredicate> Ptr;

    virtual ~InPredicate() {}
    virtual char const* getName() const { return "InPredicate"; }

    virtual void findValueExprs(ValueExprPtrVector& vector);
    virtual void findColumnRefs(ColumnRef::Vector& vector);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone();}

    std::ostream& dump(std::ostream& os) const override;

    bool equal(const BoolFactorTerm& rhs) const override {
        auto rhsInPredicate = dynamic_cast<InPredicate const * const>(&rhs);
        if (nullptr == rhsInPredicate) {
            return false;
        }
        return util::pointerCompare(value, rhsInPredicate->value) &&
               util::vectorPointerCompare(cands, rhsInPredicate->cands);
    }

    ValueExprPtr value;
    ValueExprPtrVector cands;
};

/// BetweenPredicate is a Predicate comparing a row value to a range
class BetweenPredicate : public Predicate {
public:
    BetweenPredicate() {}
    BetweenPredicate(ValueExprPtr iValue, ValueExprPtr iMinValue, ValueExprPtr iMaxValue)
    : value(iValue), minValue(iMinValue), maxValue(iMaxValue) {}
    typedef std::shared_ptr<BetweenPredicate> Ptr;

    virtual ~BetweenPredicate() {}
    virtual char const* getName() const { return "BetweenPredicate"; }

    virtual void findValueExprs(ValueExprPtrVector& vector);
    virtual void findColumnRefs(ColumnRef::Vector& vector);
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone(); }

    std::ostream& dump(std::ostream& os) const override;

    bool equal(const BoolFactorTerm& rhs) const override {
        auto rhsBetweenPredicate = dynamic_cast<BetweenPredicate const * const>(&rhs);
        if (nullptr == rhsBetweenPredicate) {
            return false;
        }
        return util::pointerCompare(value, rhsBetweenPredicate->value) &&
               util::pointerCompare(minValue, rhsBetweenPredicate->minValue) &&
               util::pointerCompare(maxValue, rhsBetweenPredicate->maxValue);
    }

    ValueExprPtr value;
    ValueExprPtr minValue;
    ValueExprPtr maxValue;
};

/// LikePredicate is a Predicate involving a row value compared to a pattern
/// (pattern is a char-valued value expression
class LikePredicate : public Predicate {
public:
    typedef std::shared_ptr<LikePredicate> Ptr;

    virtual ~LikePredicate() {}
    virtual char const* getName() const { return "LikePredicate"; }

    virtual void findValueExprs(ValueExprPtrVector& vector);
    virtual void findColumnRefs(ColumnRef::Vector& vector);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone(); }

    std::ostream& dump(std::ostream& os) const override;

    bool equal(const BoolFactorTerm& rhs) const override {
        auto rhsLikePredicate = dynamic_cast<LikePredicate const * const>(&rhs);
        if (nullptr == rhsLikePredicate) {
            return false;
        }
        return util::pointerCompare(value, rhsLikePredicate->value) &&
               util::pointerCompare(charValue, rhsLikePredicate->charValue);
    }

    ValueExprPtr value;
    ValueExprPtr charValue;
};

/// NullPredicate is a Predicate involving a row value compared to NULL
class NullPredicate : public Predicate {
public:
    typedef std::shared_ptr<NullPredicate> Ptr;

    virtual ~NullPredicate() {}
    virtual char const* getName() const { return "NullPredicate"; }

    virtual void findValueExprs(ValueExprPtrVector& vector);
    virtual void findColumnRefs(ColumnRef::Vector& vector);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual BoolFactorTerm::Ptr clone() const;
    virtual BoolFactorTerm::Ptr copySyntax() const { return clone(); }

    static int reverseOp(int op); // Reverses operator token

    std::ostream& dump(std::ostream& os) const override;

    bool equal(const BoolFactorTerm& rhs) const override {
        auto rhsNullPredicate = dynamic_cast<NullPredicate const * const>(&rhs);
        if (nullptr == rhsNullPredicate) {
            return false;
        }
        return hasNot == rhsNullPredicate->hasNot && util::pointerCompare(value, rhsNullPredicate->value);
    }

    ValueExprPtr value;
    bool hasNot;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_PREDICATE_H
