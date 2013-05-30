// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @brief BoolTerm is a representation of a boolean term in a WHERE clause
  *
  * @author Daniel L. Wang, SLAC
  */
// 

#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
class QueryTemplate; // Forward
class ValueExpr;

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

    friend std::ostream& operator<<(std::ostream& os, BoolTerm const& bt);
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
    /// Deep copy this term.
    virtual boost::shared_ptr<BoolTerm> copySyntax() {
        return boost::shared_ptr<BoolTerm>(); }
    class render;
};
/// BfTerm is a term in a in a BoolFactor
class BfTerm {
public:
    typedef boost::shared_ptr<BfTerm> Ptr;
    typedef std::list<Ptr> PtrList;
    virtual ~BfTerm() {}
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void renderTo(QueryTemplate& qt) const = 0;
};
/// OrTerm is a set of OR-connected BoolTerms
class OrTerm : public BoolTerm {
public:    
    typedef boost::shared_ptr<OrTerm> Ptr;

    virtual char const* getName() const { return "OrTerm"; }
    virtual PtrList::iterator iterBegin() { return _terms.begin(); }
    virtual PtrList::iterator iterEnd() { return _terms.end(); }

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual boost::shared_ptr<BoolTerm> copySyntax();

    class render;
    BoolTerm::PtrList _terms;
};
/// OrTerm is a set of AND-connected BoolTerms
class AndTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<AndTerm> Ptr;

    virtual char const* getName() const { return "AndTerm"; }

    virtual PtrList::iterator iterBegin() { return _terms.begin(); }
    virtual PtrList::iterator iterEnd() { return _terms.end(); }

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    virtual boost::shared_ptr<BoolTerm> copySyntax();
    BoolTerm::PtrList _terms;
};
/// BoolFactor is a plain factor in a BoolTerm 
class BoolFactor : public BoolTerm {
public:
    typedef boost::shared_ptr<BoolFactor> Ptr;
    virtual char const* getName() const { return "BoolFactor"; }

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    BfTerm::PtrList _terms;
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
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    std::string _text;
};
/// PassListTerm is like a PassTerm, but holds a list of passing strings
class PassListTerm : public BfTerm {
public: // ( term, term, term )
    typedef std::list<std::string> StringList;
    typedef boost::shared_ptr<PassListTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    StringList _terms;
};
/// ValueExprTerm is a bool factor term that contains a value expression
class ValueExprTerm : public BfTerm {
public:
    typedef boost::shared_ptr<ValueExprTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<ValueExpr> _expr;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_BOOLTERM_H

