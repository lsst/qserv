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
#ifndef LSST_QSERV_MASTER_BOOLTERM_H
#define LSST_QSERV_MASTER_BOOLTERM_H
/**
  * @file BoolTerm.h
  *
  * @brief BoolTerm, BfTerm, OrTerm, AndTerm, BoolFactor, PassTerm, UnknownTerm,
  * ValueExprTerm declarations.
  *
  * @author Daniel L. Wang, SLAC
  */
//

#include <list>
#include <string>
#include <boost/shared_ptr.hpp>
#include "query/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace master {

class QueryTemplate; // Forward
class ValueExpr;
/// BfTerm is a term in a in a BoolFactor
class BfTerm {
public:
    typedef boost::shared_ptr<BfTerm> Ptr;
    typedef std::list<Ptr> PtrList;
    virtual ~BfTerm() {}
    virtual Ptr copySyntax() const = 0;
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    virtual void findColumnRefs(ColumnRef::List& list) {}
    class ConstOp { public: virtual void operator()(BfTerm const& t) = 0; };
    class Op { public: virtual void operator()(BfTerm& t) = 0; };
};

/// BoolTerm is a representation of a boolean-valued term in a SQL WHERE
class BoolTerm {
public:
    typedef boost::shared_ptr<BoolTerm> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~BoolTerm() {}
    virtual char const* getName() const { return "BoolTerm"; }

    /// @return a mutable list iterator for the contained terms
    virtual PtrList::iterator iterBegin() { return PtrList::iterator(); }
    /// @return the terminal iterator
    virtual PtrList::iterator iterEnd() { return PtrList::iterator(); }

    virtual void visitBfTerm(BfTerm::ConstOp& o) const {}
    virtual void visitBfTerm(BfTerm::Op& o) {}

    /// @return the reduced form of this term, or null if no reduction is
    /// possible.
    virtual boost::shared_ptr<BoolTerm> getReduced() { return Ptr(); }

    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    /// Deep copy this term.
    virtual boost::shared_ptr<BoolTerm> copySyntax() const {
        return boost::shared_ptr<BoolTerm>(); }
};
/// OrTerm is a set of OR-connected BoolTerms
class OrTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<OrTerm> Ptr;

    virtual char const* getName() const { return "OrTerm"; }
    virtual PtrList::iterator iterBegin() { return _terms.begin(); }
    virtual PtrList::iterator iterEnd() { return _terms.end(); }

    virtual boost::shared_ptr<BoolTerm> getReduced();

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual boost::shared_ptr<BoolTerm> copySyntax() const;

    BoolTerm::PtrList _terms;
};
/// AndTerm is a set of AND-connected BoolTerms
class AndTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<AndTerm> Ptr;

    virtual char const* getName() const { return "AndTerm"; }

    virtual PtrList::iterator iterBegin() { return _terms.begin(); }
    virtual PtrList::iterator iterEnd() { return _terms.end(); }

    virtual boost::shared_ptr<BoolTerm> getReduced();

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;

    virtual boost::shared_ptr<BoolTerm> copySyntax() const;
    BoolTerm::PtrList _terms;
};
/// BoolFactor is a plain factor in a BoolTerm
class BoolFactor : public BoolTerm {
public:
    typedef boost::shared_ptr<BoolFactor> Ptr;
    virtual char const* getName() const { return "BoolFactor"; }


    virtual boost::shared_ptr<BoolTerm> getReduced();

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual boost::shared_ptr<BoolTerm> copySyntax() const;
    virtual void findColumnRefs(ColumnRef::List& list);

    BfTerm::PtrList _terms;
private:
    bool _reduceTerms(BfTerm::PtrList& newTerms, BfTerm::PtrList& oldTerms);
    bool _checkParen(BfTerm::PtrList& terms);
};
/// UnknownTerm is a catch-all term intended to help the framework pass-through
/// syntax that is not analyzed, modified, or manipulated in Qserv.
class UnknownTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<UnknownTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
};
/// PassTerm is a catch-all boolean factor term that can be safely passed
/// without further analysis or manipulation.
class PassTerm : public BfTerm {
public: // text
    typedef boost::shared_ptr<PassTerm> Ptr;
    virtual BfTerm::Ptr copySyntax() const;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    std::string _text;
};
/// PassListTerm is like a PassTerm, but holds a list of passing strings
class PassListTerm : public BfTerm {
public: // ( term, term, term )
    typedef std::list<std::string> StringList;
    typedef boost::shared_ptr<PassListTerm> Ptr;
    virtual BfTerm::Ptr copySyntax() const;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    StringList _terms;
};

/// BoolTermFactor is a bool factor term that contains a bool term. Occurs often
/// when parentheses are used within a bool term. The parenthetical group is an
/// entire factor, and it contains bool terms.
class BoolTermFactor : public BfTerm {
public:
    typedef boost::shared_ptr<BoolTermFactor> Ptr;
    virtual BfTerm::Ptr copySyntax() const;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual void findColumnRefs(ColumnRef::List& list);
    boost::shared_ptr<BoolTerm> _term;
};
}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_BOOLTERM_H
