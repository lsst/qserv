// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
  * @brief WhereFactory is responsible for constructing WhereList (and
  * SelectList, etc.) from an ANTLR parse tree.  For now, the code for
  * other factories is included here as well.  WhereClause is a parse
  * element construct for SQL WHERE.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/WhereFactory.h"

// System headers
#include <algorithm>
#include <iterator>
#include <sstream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/stringTypes.h"
#include "parser/BoolTermFactory.h"
#include "parser/parserBase.h" // Handler base classes
#include "parser/parseTreeUtil.h"
#include "parser/ParseException.h"
#include "parser/SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "query/WhereClause.h"


// Anonymous helpers
namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.WhereFactory");

class ParamGenerator {
public:
    struct Check {
        bool operator()(RefAST r) {
            return (r->getType() == SqlSQL2TokenTypes::RIGHT_PAREN)
                || (r->getType() == SqlSQL2TokenTypes::COMMA);
        }
    };
    class Iter  {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef std::string value_type;
        typedef int difference_type;
        typedef std::string* pointer;
        typedef std::string& reference;

        RefAST start;
        RefAST current;
        RefAST nextCache;
        Iter operator++(int) {
            Iter tmp = *this;
            ++*this;
            return tmp;
        }
        Iter& operator++() {
            Check c;
            if (nextCache.get()) {
                current = nextCache;
            } else {
                current = lsst::qserv::parser::findSibling(current, c);
                if (current.get()) {
                    // Move to next value
                    current = current->getNextSibling();
                }
            }
            return *this;
        }

        std::string operator*() {
            Check c;
            if (!current) {
                throw std::logic_error("Corrupted ParamGenerator::Iter");
            }
            lsst::qserv::parser::CompactPrintVisitor<antlr::RefAST> p;
            for(;current.get() && !c(current);
                current = current->getNextSibling()) {
                p(current);
            }
            return p.result;
        }
        bool operator==(Iter const& rhs) const {
            return (start == rhs.start) && (current == rhs.current);
        }
        bool operator!=(Iter const& rhs) const {
            return !(*this == rhs);
        }

    };
    ParamGenerator(RefAST a) {
        _beginIter.start = a;
        if (a.get() && (a->getType() == SqlSQL2TokenTypes::LEFT_PAREN)) {
            _beginIter.current = a->getNextSibling(); // Move past paren.
        } else { // else, set current as end.
            _beginIter.current = RefAST();
        }
        _endIter.start = a;
        _endIter.current = RefAST();
    }

    Iter begin() {
        return _beginIter;
    }

    Iter end() {
        return _endIter;
    }
private:
    Iter _beginIter;
    Iter _endIter;
};
} // anonymouse namespace


namespace lsst {
namespace qserv {
namespace parser {


////////////////////////////////////////////////////////////////////////
// WhereFactory::WhereCondH
////////////////////////////////////////////////////////////////////////
class WhereFactory::WhereCondH : public VoidOneRefFunc {
public:
    WhereCondH(WhereFactory& wf) : _wf(wf) {}
    virtual ~WhereCondH() {}
    virtual void operator()(antlr::RefAST where) {
        _wf._import(where);
    }
private:
    WhereFactory& _wf;
};

class FromWhereH : public VoidOneRefFunc {
public:
    FromWhereH() {}
    virtual ~FromWhereH() {}
    virtual void operator()(antlr::RefAST fw) {
        if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
            std::stringstream ss;
            printDigraph("fromwhere", ss, fw);
            LOGS(_log, LOG_LVL_DEBUG, "fromwhere " << ss.str());
        }
    }
};
////////////////////////////////////////////////////////////////////////
// WhereFactory
////////////////////////////////////////////////////////////////////////

WhereFactory::WhereFactory(std::shared_ptr<ValueExprFactory> vf)
    : _vf(vf) {
}

std::shared_ptr<query::WhereClause>
WhereFactory::getProduct() {
    return _clause;
}

std::shared_ptr<query::WhereClause>
WhereFactory::newEmpty() {
    std::shared_ptr<query::WhereClause> w = std::make_shared<query::WhereClause>();
    return w;
}

void
WhereFactory::attachTo(SqlSQL2Parser& p) {
    std::shared_ptr<WhereCondH> wch(new WhereCondH(*this));
    p._whereCondHandler = wch;
}

void
WhereFactory::_import(antlr::RefAST a) {
    _clause = std::make_shared<query::WhereClause>();
    if (a->getType() != SqlSQL2TokenTypes::SQL2RW_where) {
        throw ParseException("Bug: _import expected WHERE node", a);
    }
    RefAST first = a->getFirstChild();
    if (!first.get()) {
        throw ParseException("Missing subtree from WHERE node", a);
    }
    while(first.get()
          && (first->getType() == SqlSQL2TokenTypes::QSERV_FCT_SPEC)) {
        _addQservRestrictor(first->getFirstChild());
        first = first->getNextSibling();
        if (first.get() && (first->getType() == SqlSQL2TokenTypes::SQL2RW_and)) {
            first = first->getNextSibling();
        }
    }
    if (first.get()
       && (first->getType() == SqlSQL2TokenTypes::OR_OP)) {
        _addOrSibs(first->getFirstChild());
    }
}

void
WhereFactory::addQservRestrictor(std::shared_ptr<query::WhereClause>& whereClause,
                                 const std::string& function,
                                 const std::vector<std::string>& parameters) {
    auto restrictor = std::make_shared<query::QsRestrictor>();

    std::copy(parameters.begin(), parameters.end(), std::back_inserter(restrictor->_params));
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        std::stringstream ss;
        std::copy(restrictor->_params.begin(), restrictor->_params.end(),
                  std::ostream_iterator<std::string>(ss, ", "));
    }
    // Add case insensitive behavior
    // in order to mimic MySQL functions/procedures
    std::string insensitiveFunction(function);
    if (insensitiveFunction != "sIndex") {
        std::transform(insensitiveFunction.begin(), insensitiveFunction.end(), insensitiveFunction.begin(),
                ::tolower);
        LOGS(_log, LOG_LVL_DEBUG, "Qserv restrictor changed to lower-case: " << insensitiveFunction);
    }
    restrictor->_name = insensitiveFunction;
    whereClause->_restrs->push_back(restrictor);
}

void
WhereFactory::_addQservRestrictor(antlr::RefAST a) {
    std::string r(a->getText()); // e.g. qserv_areaspec_box
    ParamGenerator pg(a->getNextSibling());

    query::QsRestrictor::Ptr restr = std::make_shared<query::QsRestrictor>();
    StringVector& params = restr->_params;

    std::copy(pg.begin(), pg.end(), std::back_inserter(params));
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        std::stringstream ss;
        std::copy(params.begin(), params.end(),
                  std::ostream_iterator<std::string>(ss, ", "));
        LOGS(_log, LOG_LVL_DEBUG, "Adding from " << r << ": " << ss.str());
    }
    if (!_clause->_restrs) {
        throw std::logic_error("Invalid WhereClause._restrs");
    }
    // Add case insensitive behavior
    // in order to mimic MySQL functions/procedures
    if (r != "sIndex") {
        std::transform(r.begin(), r.end(), r.begin(), ::tolower);
        LOGS(_log, LOG_LVL_DEBUG, "Qserv restrictor changed to lower-case: " << r);
    }
    restr->_name = r;
    _clause->_restrs->push_back(restr);
}

template <typename Check>
struct PrintExcept : public PrintVisitor<antlr::RefAST> {
public:
    PrintExcept(Check& c_) : c(c_) {}
    void operator()(antlr::RefAST a) {
        if (!c(a)) PrintVisitor<antlr::RefAST>::operator()(a);
    }
    Check& c;
};

struct MetaCheck {
    bool operator()(antlr::RefAST a) {
        if (!a.get()) return false;
        switch(a->getType()) {
        case SqlSQL2TokenTypes::OR_OP:
        case SqlSQL2TokenTypes::AND_OP:
        case SqlSQL2TokenTypes::BOOLEAN_FACTOR:
        case SqlSQL2TokenTypes::VALUE_EXP:
            return true;
        default:
            return false;
        }
        return false;
    }
};

void
WhereFactory::_addOrSibs(antlr::RefAST a) {
    MetaCheck mc;
    PrintExcept<MetaCheck> p(mc);

    if (!_clause.get()) {
        throw std::logic_error("Expected valid WhereClause");
    }

    walkTreeVisit(a, p);
    BoolTermFactory f(_vf);
    _clause->_tree = f.newOrTerm(a);
    // FIXME: Store template.
    // Template must allow table substitution.
    // For now, reuse old templating scheme.
}

}}} // namespace lsst::qserv::parser
