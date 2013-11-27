// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @file Predicate.h
  *
  * @brief Predicate is a representation of a boolean term in a WHERE clause
  *
  * @author Daniel L. Wang, SLAC
  */
//

#include <list>
#include <string>
#include <boost/shared_ptr.hpp>
#include "query/BoolTerm.h"

namespace lsst {
namespace qserv {
namespace query {

// Forward
class QueryTemplate;
class ValueExpr;

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
class Predicate : public BfTerm {
public:
    typedef boost::shared_ptr<Predicate> Ptr;
    typedef std::list<Ptr> PtrList;
    typedef std::list<boost::shared_ptr<ValueExpr> > ValueExprList;
    typedef ValueExprList::iterator ValueExprListIter;

    virtual ~Predicate() {}
    virtual char const* getName() const { return "Predicate"; }

    virtual void cacheValueExprList() {}
    virtual ValueExprList::iterator valueExprCacheBegin() { return ValueExprList::iterator(); }
    virtual ValueExprList::iterator valueExprCacheEnd() { return ValueExprList::iterator(); }

    virtual void findColumnRefs(ColumnRef::List& list) {}

    friend std::ostream& operator<<(std::ostream& os, Predicate const& bt);
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const {
        return BfTerm::Ptr(); }
};

/// GenericPredicate is a Predicate whose structure whose semantic meaning
/// is unimportant for qserv
class GenericPredicate : public Predicate {
public:
    typedef boost::shared_ptr<Predicate> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~GenericPredicate() {}
    virtual char const* getName() const { return "GenericPredicate"; }

    /// @return a mutable list iterator for the contained terms
    //virtual PtrList::iterator iterBegin() { return PtrList::iterator(); }
    /// @return the terminal iterator
    //virtual PtrList::iterator iterEnd() { return PtrList::iterator(); }

    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const {
        return BfTerm::Ptr(); }
};

/// CompPredicate is a Predicate involving a row value compared to another row value.
/// (literals can be row values)
class CompPredicate : public Predicate {
public:
    typedef boost::shared_ptr<CompPredicate> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~CompPredicate() {}
    virtual char const* getName() const { return "CompPredicate"; }

    virtual void cacheValueExprList();
    virtual ValueExprList::iterator valueExprCacheBegin() { return _cache->begin(); }
    virtual ValueExprList::iterator valueExprCacheEnd() { return _cache->end(); }
    virtual void findColumnRefs(ColumnRef::List& list);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const;

    static int reverseOp(int op); // Reverses operator token
    static char const* lookupOp(int op);
    static int lookupOp(char const* op);

    boost::shared_ptr<ValueExpr> left;
    int op; // Parser token type of operator
    boost::shared_ptr<ValueExpr> right;
private:
    boost::shared_ptr<Predicate::ValueExprList> _cache;
};

/// InPredicate is a Predicate comparing a row value to a set
class InPredicate : public Predicate {
public:
    typedef boost::shared_ptr<InPredicate> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~InPredicate() {}
    virtual char const* getName() const { return "InPredicate"; }

    virtual void cacheValueExprList();
    virtual ValueExprList::iterator valueExprCacheBegin() { return _cache->begin(); }
    virtual ValueExprList::iterator valueExprCacheEnd() { return _cache->end(); }
    virtual void findColumnRefs(ColumnRef::List& list);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const;

    boost::shared_ptr<ValueExpr> value;

    std::list<boost::shared_ptr<ValueExpr> > cands;
private:
    boost::shared_ptr<Predicate::ValueExprList> _cache;
};
/// BetweenPredicate is a Predicate comparing a row value to a range
class BetweenPredicate : public Predicate {
public:
    typedef boost::shared_ptr<BetweenPredicate> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~BetweenPredicate() {}
    virtual char const* getName() const { return "BetweenPredicate"; }

    virtual void cacheValueExprList();
    virtual ValueExprList::iterator valueExprCacheBegin() { return _cache->begin(); }
    virtual ValueExprList::iterator valueExprCacheEnd() { return _cache->end(); }
    virtual void findColumnRefs(ColumnRef::List& list);
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const;

    boost::shared_ptr<ValueExpr> value;
    boost::shared_ptr<ValueExpr> minValue;
    boost::shared_ptr<ValueExpr> maxValue;
private:
    boost::shared_ptr<Predicate::ValueExprList> _cache;
};

/// LikePredicate is a Predicate involving a row value compared to a pattern
/// (pattern is a char-valued value expression
class LikePredicate : public Predicate {
public:
    typedef boost::shared_ptr<LikePredicate> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~LikePredicate() {}
    virtual char const* getName() const { return "LikePredicate"; }

    virtual void cacheValueExprList();
    virtual ValueExprList::iterator valueExprCacheBegin() { return _cache->begin(); }
    virtual ValueExprList::iterator valueExprCacheEnd() { return _cache->end(); }
    virtual void findColumnRefs(ColumnRef::List& list);

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    /// Deep copy this term.
    virtual BfTerm::Ptr copySyntax() const;

    static int reverseOp(int op); // Reverses operator token

    boost::shared_ptr<ValueExpr> value;
    boost::shared_ptr<ValueExpr> charValue;
private:
    boost::shared_ptr<Predicate::ValueExprList> _cache;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_PREDICATE_H
