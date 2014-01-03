/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file FromFactory.cc
  *
  * @brief Implementation of FromFactory, which is responsible for
  * constructing FromList from an ANTLR parse tree.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/FromFactory.h"
#include "lsst/qserv/master/FromList.h" // for class FromList
// C++
#include <deque>
#include <iterator>

// Package
#include "SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/ParseAliasMap.h"
#include "lsst/qserv/master/ParseException.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/TableRefN.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/Logger.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;

////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
inline RefAST walkToSiblingBefore(RefAST node, int typeId) {
    RefAST last = node;
    for(; node.get(); node = node->getNextSibling()) {
        if(node->getType() == typeId) return last;
        last = node;
    }
    return RefAST();
}

inline std::string getSiblingStringBounded(RefAST left, RefAST right) {
    qMaster::CompactPrintVisitor<RefAST> p;
    for(; left.get(); left = left->getNextSibling()) {
        p(left);
        if(left == right) break;
    }
    return p.result;
}

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
            //LOGGER_INF << "advancingX..: " << current->getText() << std::endl;
            Iter tmp = *this;
            ++*this;
            return tmp;
        }
        Iter& operator++() {
            //LOGGER_INF << "advancing..: " << current->getText() << std::endl;
            Check c;
            if(nextCache.get()) {
                current = nextCache;
            } else {
                current = qMaster::findSibling(current, c);
                if(current.get()) {
                    // Move to next value
                    current = current->getNextSibling();
                }
            }
            return *this;
        }

        std::string operator*() {
            Check c;
            if(!current) {
                throw std::invalid_argument("Invalid _current in iteration");
            }
            qMaster::CompactPrintVisitor<antlr::RefAST> p;
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
        if(a.get() && (a->getType() == SqlSQL2TokenTypes::LEFT_PAREN)) {
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
} // anonymous
////////////////////////////////////////////////////////////////////////
// ParseAliasMap misc impl. (to be placed in ParseAliasMap.cc later)
////////////////////////////////////////////////////////////////////////
std::ostream& qMaster::operator<<(std::ostream& os,
                                  qMaster::ParseAliasMap const& m) {
    using qMaster::ParseAliasMap;
    typedef ParseAliasMap::Miter Miter;
    os << "AliasMap fwd(";
    for(Miter it=m._map.begin(); it != m._map.end(); ++it) {
        os << it->first->getText() << "->" << it->second->getText()
           << ", ";
    }
    os << ")    rev(";
    for(Miter it=m._rMap.begin(); it != m._rMap.end(); ++it) {
        os << it->first->getText() << "->" << it->second->getText()
           << ", ";
    }
    os << ")";
    return os;
}

////////////////////////////////////////////////////////////////////////
// FromFactory
////////////////////////////////////////////////////////////////////////
using qMaster::FromList;
using qMaster::FromFactory;
////////////////////////////////////////////////////////////////////////
// FromFactory::FromClauseH
////////////////////////////////////////////////////////////////////////
class FromFactory::TableRefListH : public VoidTwoRefFunc {
public:
    TableRefListH(FromFactory& f) : _f(f) {}
    virtual ~TableRefListH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b) {
        _f._import(a); // Trigger from list construction
    }
private:
    FromFactory& _f;
};
////////////////////////////////////////////////////////////////////////
// FromFactory::TableRefAuxH
////////////////////////////////////////////////////////////////////////
class FromFactory::TableRefAuxH : public VoidFourRefFunc {
public:
    TableRefAuxH(boost::shared_ptr<qMaster::ParseAliasMap> map)
        : _map(map) {}
    virtual ~TableRefAuxH() {}
    virtual void operator()(antlr::RefAST name, antlr::RefAST sub,
                            antlr::RefAST as, antlr::RefAST alias)  {
        using lsst::qserv::master::getSiblingBefore;
        using qMaster::tokenText;
        if(alias.get()) {
            _map->addAlias(alias, name);
        }
        // Save column ref for pass/fixup computation,
        // regardless of alias.
    }
private:
    boost::shared_ptr<qMaster::ParseAliasMap> _map;
};
class QualifiedName {
public:
    QualifiedName(antlr::RefAST qn) {
        for(; qn.get(); qn = qn->getNextSibling()) {
            if(qn->getType() == SqlSQL2TokenTypes::PERIOD) continue;
            names.push_back(qn->getText());
        }
    }
    std::string getQual(int i) const {
        return names[names.size() -1 - i];
    }
    std::string getName() const { return getQual(0); }
    std::deque<std::string> names;
};
////////////////////////////////////////////////////////////////////////
// FromFactory::ListIterator
////////////////////////////////////////////////////////////////////////
class FromFactory::RefGenerator {
public:
    RefGenerator(antlr::RefAST firstRef,
                 boost::shared_ptr<ParseAliasMap> aliases)
        : _cursor(firstRef), _aliases(aliases) {
        LOGGER_INF << *_aliases << std::endl;

    }
    TableRefN::Ptr get() const {
        if(_cursor->getType() != SqlSQL2TokenTypes::TABLE_REF) {
            throw std::logic_error("_cursor is not a TABLE_REF");
        }
        RefAST node = _cursor->getFirstChild();
        RefAST child;

        TableRefN::Ptr tn;
        switch(node->getType()) {
        case SqlSQL2TokenTypes::TABLE_REF_AUX:
            child = node->getFirstChild();
            switch(child->getType()) {
            case SqlSQL2TokenTypes::QUALIFIED_NAME:
                tn.reset(_processQualifiedName(child));
                break;
            case SqlSQL2TokenTypes::SUBQUERY:
                tn.reset(_processSubquery(child));
                break;
            default:
                break;
            }
            break;
            // FIXME
        default:
            break;
        }
        return tn;
    }
    void next() {
        _cursor = _cursor->getNextSibling();
        if(!_cursor.get()) return; // Iteration complete
        switch(_cursor->getType()) {
        case SqlSQL2TokenTypes::COMMA:
            next();
            break;
        default:
            // LOGGER_INF << "next type is:" << _cursor->getType()
            //           << " and text is:" << _cursor->getText() << std::endl;
            break;
        }
    }
    bool isDone() const {
        return !_cursor.get();
    }
private:
    void _setup() {
        // Sanity check: make sure we were constructed with a TABLE_REF
        if(_cursor->getType() == SqlSQL2TokenTypes::TABLE_REF) {
            // Nothing else to do
        } else {
            _cursor = RefAST(); // Clear out cursor so that isDone == true
        }
    }
    SimpleTableN* _processQualifiedName(RefAST n) const {
        RefAST qnStub = n;
        RefAST aliasN = _aliases->getAlias(qnStub);
        std::string alias;
        if(aliasN.get()) alias = aliasN->getText();
        QualifiedName qn(n->getFirstChild());
        if(qn.names.size() > 1) {
            return new SimpleTableN(qn.getQual(1), qn.getName(), alias);
        } else {
            return new SimpleTableN("", qn.getName(), alias);
        }
    }
    TableRefN* _processSubquery(RefAST n) const {
        throw ParseException("Subqueries unsupported", n->getFirstChild());
    }

    // Fields
    antlr::RefAST _cursor;
    boost::shared_ptr<ParseAliasMap> _aliases;

};
////////////////////////////////////////////////////////////////////////
// FromFactory (impl)
////////////////////////////////////////////////////////////////////////
FromFactory::FromFactory(boost::shared_ptr<ParseAliasMap> aliases) :
        _aliases(aliases) {
}

boost::shared_ptr<FromList>
FromFactory::getProduct() {
    return _list;
}

void
FromFactory::attachTo(SqlSQL2Parser& p) {
    boost::shared_ptr<TableRefListH> lh(new TableRefListH(*this));
    p._tableListHandler = lh;
    boost::shared_ptr<TableRefAuxH> ah(new TableRefAuxH(_aliases));
    p._tableAliasHandler = ah;
}

void
FromFactory::_import(antlr::RefAST a) {
    _list.reset(new FromList());
    _list->_tableRefns.reset(new TableRefnList());

    // LOGGER_INF << "FROM starts with: " << a->getText()
    //           << " (" << a->getType() << ")" << std::endl;
    std::stringstream ss;
    //LOGGER_INF << "FROM indented: " << walkIndentedString(a) << std::endl;
    for(RefGenerator refGen(a, _aliases); !refGen.isDone(); refGen.next()) {
        TableRefN::Ptr p = refGen.get();
        ss << "Found ref:" ;
        TableRefN& tn = *p;
        ss << tn;
        _list->_tableRefns->push_back(p);
    }
    std::string s(ss.str());
    if(s.size() > 0) { LOGGER_INF << s << std::endl; }
}
