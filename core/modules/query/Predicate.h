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
/**
  * @file
  *
  * @brief Predicate is a representation of a boolean term in a WHERE clause
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_PREDICATE_H
#define LSST_QSERV_QUERY_PREDICATE_H


// System headers
#include <memory>
#include <string>

// Local headers
#include "query/BoolFactorTerm.h"
#include "query/typedefs.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {


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

    ~Predicate() override = default;

    virtual char const* getName() const = 0;
    friend std::ostream& operator<<(std::ostream& os, Predicate const& bt);
    BoolFactorTerm::Ptr copySyntax() const override { return BoolFactorTerm::Ptr(); }
};


/// GenericPredicate is a Predicate whose structure whose semantic meaning
/// is unimportant for qserv
class GenericPredicate : public Predicate {
public:
    typedef std::shared_ptr<GenericPredicate> Ptr;

    ~GenericPredicate() override = default;

    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
};


/// CompPredicate is a Predicate involving a row value compared to another row value.
/// (literals can be row values)
class CompPredicate : public Predicate {
public:
    typedef std::shared_ptr<CompPredicate> Ptr;

    ~CompPredicate() override = default;

    char const* getName() const override { return "CompPredicate"; }
    void findValueExprs(ValueExprPtrVector& vector) const override;
    void findColumnRefs(ColumnRef::Vector& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
    bool operator==(const BoolFactorTerm& rhs) const override;

    static int lookupOp(char const* op);

    ValueExprPtr left;
    int op; // Parser token type of operator
    ValueExprPtr right;

protected:
    void dbgPrint(std::ostream& os) const override;
};


/// InPredicate is a Predicate comparing a row value to a set
class InPredicate : public Predicate {
public:
    typedef std::shared_ptr<InPredicate> Ptr;

    InPredicate(ValueExprPtr const & iValue, ValueExprPtrVector const & iCands, bool iHasNot)
            : value(iValue), cands(iCands), hasNot(iHasNot)
    {}

    InPredicate() : hasNot(false) {}

    ~InPredicate() override = default;

    char const* getName() const override { return "InPredicate"; }
    void findValueExprs(ValueExprPtrVector& vector) const override;
    void findColumnRefs(ColumnRef::Vector& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone();}
    bool operator==(const BoolFactorTerm& rhs) const override;

    ValueExprPtr value;
    ValueExprPtrVector cands;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


/// BetweenPredicate is a Predicate comparing a row value to a range
class BetweenPredicate : public Predicate {
public:
    BetweenPredicate() : hasNot(false) {}
    BetweenPredicate(ValueExprPtr iValue, ValueExprPtr iMinValue, ValueExprPtr iMaxValue, bool iHasNot)
    : value(iValue), minValue(iMinValue), maxValue(iMaxValue), hasNot(iHasNot) {}
    typedef std::shared_ptr<BetweenPredicate> Ptr;

    ~BetweenPredicate() override = default;

    char const* getName() const override { return "BetweenPredicate"; }
    void findValueExprs(ValueExprPtrVector& vector) const override;
    void findColumnRefs(ColumnRef::Vector& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
    bool operator==(const BoolFactorTerm& rhs) const override;

    ValueExprPtr value;
    ValueExprPtr minValue;
    ValueExprPtr maxValue;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


/// LikePredicate is a Predicate involving a row value compared to a pattern
/// (pattern is a char-valued value expression
class LikePredicate : public Predicate {
public:
    typedef std::shared_ptr<LikePredicate> Ptr;

    ~LikePredicate()  override = default;

    char const* getName() const override { return "LikePredicate"; }
    void findValueExprs(ValueExprPtrVector& vector) const override;
    void findColumnRefs(ColumnRef::Vector& vector) const override;
    std::ostream& putStream(std::ostream& os) const override;
    void renderTo(QueryTemplate& qt) const override;
    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
    bool operator==(const BoolFactorTerm& rhs) const override;

    ValueExprPtr value;
    ValueExprPtr charValue;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


/// NullPredicate is a Predicate involving a row value compared to NULL
class NullPredicate : public Predicate {
public:
    typedef std::shared_ptr<NullPredicate> Ptr;

    NullPredicate()
    : hasNot(false) {}

    NullPredicate(ValueExprPtr valueExpr, bool hasNotNull)
    : value(valueExpr), hasNot(hasNotNull) {}

    ~NullPredicate() override = default;

    char const* getName() const { return "NullPredicate"; }
    void findValueExprs(ValueExprPtrVector& vector) const;
    void findColumnRefs(ColumnRef::Vector& vector) const;
    std::ostream& putStream(std::ostream& os) const;
    void renderTo(QueryTemplate& qt) const;
    BoolFactorTerm::Ptr clone() const;
    BoolFactorTerm::Ptr copySyntax() const { return clone(); }
    bool operator==(const BoolFactorTerm& rhs) const override;

    static int reverseOp(int op); // Reverses operator token

    ValueExprPtr value;
    bool hasNot;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_PREDICATE_H
