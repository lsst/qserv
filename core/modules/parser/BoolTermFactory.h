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
#ifndef LSST_QSERV_MASTER_BOOLTERMFACTORY_H
#define LSST_QSERV_MASTER_BOOLTERMFACTORY_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

#include <iostream>
#include <boost/shared_ptr.hpp>
#include <antlr/AST.hpp>

namespace lsst { 
namespace qserv { 
namespace master {
class ValueExprFactory; // Forward
class BoolFactor;
class BoolTerm;
class BoolTermFactor;
class OrTerm;
class AndTerm;
class UnknownTerm;
class PassTerm;
class ValueExprFactory;

/// BoolTermFactory is a factory class for BoolTerm objects that get
/// placed (typically) in WhereClause objects.
class BoolTermFactory {
public:
    BoolTermFactory(boost::shared_ptr<ValueExprFactory> vf);

    /// Apply a functor, unless the reject function returns true.
    template <class Apply, class Reject>
    class applyExcept {
    public:
        applyExcept(Apply& af, Reject& rf) : _af(af), _rf(rf)  {}
        void operator()(antlr::RefAST a) {
            if(!_rf(a)) _af(a);
        }
    private:
        Apply& _af;
        Reject& _rf;
    };
    /// Construct BoolTerm and add it to another term.
    template <typename Term>
    class multiImport {
    public:
        multiImport(BoolTermFactory& bf, Term& t) : _bf(bf), _t(t)  {}
        void operator()(antlr::RefAST a) {
            _t._terms.push_back(_bf.newBoolTerm(a));
        }
    private:
        BoolTermFactory& _bf;
        Term& _t;
    };
    /// Import a node into the factory
    class bfImport {
    public:
        bfImport(BoolTermFactory& bf, BoolFactor& bfr) : _bf(bf), _bfr(bfr)  {}
        void operator()(antlr::RefAST a);
    private:
        BoolTermFactory& _bf;
        BoolFactor& _bfr;
    };
    boost::shared_ptr<BoolTerm> newBoolTerm(antlr::RefAST a);
    boost::shared_ptr<OrTerm> newOrTerm(antlr::RefAST a);
    boost::shared_ptr<AndTerm> newAndTerm(antlr::RefAST a);
    boost::shared_ptr<BoolFactor> newBoolFactor(antlr::RefAST a);
    boost::shared_ptr<UnknownTerm> newUnknown(antlr::RefAST a);
    boost::shared_ptr<PassTerm> newPassTerm(antlr::RefAST a);
    boost::shared_ptr<BoolTermFactor> newBoolTermFactor(antlr::RefAST a);

    boost::shared_ptr<ValueExprFactory> _vFactory;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_BOOLTERMFACTORY_H

