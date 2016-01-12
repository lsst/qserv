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
  * @brief Implementation of FromFactory, which is responsible for
  * constructing FromList from an ANTLR parse tree.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/FromFactory.h"

// System headers
#include <deque>
#include <iterator>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/BoolTermFactory.h"
#include "parser/ColumnRefH.h"
#include "parser/ParseAliasMap.h"
#include "parser/ParseException.h"
#include "parser/parseTreeUtil.h"
#include "parser/SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/FromList.h" // for class FromList
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/QueryTemplate.h"
#include "query/TableRef.h"


////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.FromFactory");

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
                throw std::invalid_argument("Invalid _current in iteration");
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
} // anonymous


namespace lsst {
namespace qserv {
namespace parser {


////////////////////////////////////////////////////////////////////////
// ParseAliasMap misc impl. (to be placed in ParseAliasMap.cc later)
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, ParseAliasMap const& m) {
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
    TableRefAuxH(std::shared_ptr<ParseAliasMap> map)
        : _map(map) {}
    virtual ~TableRefAuxH() {}
    virtual void operator()(antlr::RefAST name, antlr::RefAST sub,
                            antlr::RefAST as, antlr::RefAST alias)  {
        if (alias.get()) {
            _map->addAlias(alias, name);
        }
        // Save column ref for pass/fixup computation,
        // regardless of alias.
    }
private:
    std::shared_ptr<ParseAliasMap> _map;
};
class QualifiedName {
public:
    QualifiedName(antlr::RefAST qn) {
        for(; qn.get(); qn = qn->getNextSibling()) {
            if (qn->getType() == SqlSQL2TokenTypes::PERIOD) continue;
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
                 std::shared_ptr<ParseAliasMap> aliases,
                 BoolTermFactory& bFactory)
        : _cursor(firstRef),
          _aliases(aliases),
          _bFactory(bFactory) {
        //std::cout << *_aliases << "\n";
    }
    query::TableRef::Ptr get() const {
        if (_cursor->getType() != SqlSQL2TokenTypes::TABLE_REF) {
            throw std::logic_error("_cursor is not a TABLE_REF");
        }
        RefAST node = _cursor->getFirstChild();
        return _generate(node);
    }
    void next() {
        _cursor = _cursor->getNextSibling();
        if (!_cursor.get()) return; // Iteration complete
        switch(_cursor->getType()) {
        case SqlSQL2TokenTypes::COMMA:
            next();
            break;
        default:
            break;
        }
    }
    bool isDone() const {
        return !_cursor.get();
    }
private:
    inline query::TableRef::Ptr _generate(RefAST node) const {
        query::TableRef::Ptr tn;
        if (node->getType() == SqlSQL2TokenTypes::TABLE_REF_AUX) {
            tn = _processTableRefAux(node->getFirstChild());
        } else {
            throw ParseException("Expected TABLE_REF_AUX, got", node);
        }
        node = node->getNextSibling();
        query::JoinRef::Ptr jr;
        while(node.get()) {
            //std::cout << "GENERATE join from " << walkIndentedString(node);
            switch(node->getType()) {
            case SqlSQL2TokenTypes::JOIN_WITH_SPEC:
                jr = _makeJoinWithSpec(node->getFirstChild());
                break;
            case SqlSQL2TokenTypes::JOIN_NO_SPEC:
                jr = _makeJoinNoSpec(node->getFirstChild());
                break;
            case SqlSQL2TokenTypes::CROSS_JOIN:
                jr = _makeCrossJoin(node->getFirstChild());
                break;
            case SqlSQL2TokenTypes::UNION_JOIN:
                jr = _makeUnionJoin(node->getFirstChild());
                break;
            default:
                throw ParseException("Unknown (non-join) node", node);
            }
            tn->getJoins().push_back(jr);
            node = node->getNextSibling();
        }
        return tn;
    }
    void _setup() {
        // Sanity check: make sure we were constructed with a TABLE_REF
        if (_cursor->getType() == SqlSQL2TokenTypes::TABLE_REF) {
            // Nothing else to do
        } else {
            _cursor = RefAST(); // Clear out cursor so that isDone == true
        }
    }
    query::TableRef::Ptr _processTableRefAux(RefAST firstSib) const {
        switch(firstSib->getType()) {
        case SqlSQL2TokenTypes::QUALIFIED_NAME:
            return _processQualifiedName(firstSib);
        case SqlSQL2TokenTypes::SUBQUERY:
            return _processSubquery(firstSib);
        default:
            throw ParseException("No TABLE_REF_AUX", firstSib);
        }
        return query::TableRef::Ptr();
    }
    /// ( "inner" | outer_join_type ("outer")? )? "join" table_ref join_spec
    // Takes ownership of @param left
    query::JoinRef::Ptr _makeJoinWithSpec(RefAST sib) const {
        if (!sib.get()) {
            throw ParseException("Null JOIN_WITH_SPEC sibling", sib); }

        query::JoinRef::Type j = _convertToJoinType(sib);
        // Fast forward past the join types we already processed
        while(sib->getType() != SqlSQL2TokenTypes::SQL2RW_join) {
            sib = sib->getNextSibling();
        }
        sib = sib->getNextSibling();
        RefAST tableChild = sib->getFirstChild();
        query::TableRef::Ptr right = _generate(tableChild);
        sib = sib->getNextSibling();
        std::shared_ptr<query::JoinSpec> js = _processJoinSpec(sib);

        std::shared_ptr<query::JoinRef> p =
                std::make_shared<query::JoinRef>(right, j, false, js);
        return p;
    }
    /// "natural" ( "inner" | outer_join_type ("outer")? )? "join" table_ref
    query::JoinRef::Ptr _makeJoinNoSpec(RefAST sib) const {
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::SQL2RW_natural) {
            throw ParseException("Invalid NATURAL token", sib); }
        sib = sib->getNextSibling();
        query::JoinRef::Type j = _convertToJoinType(sib);
        // Fast forward past the join types we already processed
        while(sib->getType() != SqlSQL2TokenTypes::SQL2RW_join) {
            sib = sib->getNextSibling();
        }
        sib = sib->getNextSibling(); // next is TABLE_REF
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::TABLE_REF) {
            throw ParseException("Invalid token, expected TABLE_REF", sib);
        }
        RefAST tableChild = sib->getFirstChild();
        query::TableRef::Ptr right = _generate(tableChild);
        std::shared_ptr<query::JoinRef> p =
                std::make_shared<query::JoinRef>(
                        right,
                        j,
                        true, // Natural join, no conditions
                        query::JoinSpec::Ptr());
        return p;
    }
    /// "union" "join" table_ref
    query::JoinRef::Ptr _makeUnionJoin(RefAST sib) const {
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::SQL2RW_union) {
            throw ParseException("Invalid UNION token", sib); }
        sib = sib->getNextSibling();
        if (!sib.get()
               || sib->getType() != SqlSQL2TokenTypes::SQL2RW_join) {
            throw ParseException("Invalid token, expected JOIN", sib); }
        sib = sib->getNextSibling();
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::TABLE_REF) {
            throw ParseException("Invalid token, expected TABLE_REF", sib);
        }
        RefAST tableChild = sib->getFirstChild();
        query::TableRef::Ptr right = _generate(tableChild);
        std::shared_ptr<query::JoinRef> p =
                std::make_shared<query::JoinRef>(right, query::JoinRef::UNION,
                        false, // union join: no condititons
                        query::JoinSpec::Ptr());
        return p;
    }
    /// "cross" "join" table_ref
    query::JoinRef::Ptr _makeCrossJoin(RefAST sib) const {
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::SQL2RW_cross) {
            throw ParseException("Invalid CROSS token", sib); }
        sib = sib->getNextSibling();
        if (!sib.get()
               || sib->getType() != SqlSQL2TokenTypes::SQL2RW_join) {
            throw ParseException("Invalid token, expected JOIN", sib); }
        sib = sib->getNextSibling();
        if (!sib.get()
           || sib->getType() != SqlSQL2TokenTypes::TABLE_REF) {
            throw ParseException("Invalid token, expected TABLE_REF", sib);
        }
        RefAST tableChild = sib->getFirstChild();
        query::TableRef::Ptr right = _generate(tableChild);
        std::shared_ptr<query::JoinRef> p =
                std::make_shared<query::JoinRef>(
                        right,
                        query::JoinRef::CROSS,
                        false, // cross join: no conditions
                        query::JoinSpec::Ptr());
        return p;
    }

    /// USING_SPEC:
    /// "using" LEFT_PAREN column_name_list  RIGHT_PAREN
    std::shared_ptr<query::JoinSpec>  _processJoinSpec(RefAST specToken) const {
        std::shared_ptr<query::JoinSpec> js;
        // std::cout << "remaining join spec: " << walkIndentedString(specToken)
        // << "\n";
        if (!specToken.get()) {
            throw ParseException("Null join spec", specToken);
        }
        RefAST token = specToken;
        std::shared_ptr<query::BoolTerm> bt;
        switch(specToken->getType()) {
        case SqlSQL2TokenTypes::USING_SPEC:
            token = specToken->getFirstChild(); // Descend to "using"
            token = token->getNextSibling(); // Advance to LEFT_PAREN
            // Some parse tree verification is unnecessary because the
            // grammer will enforce it. Should we trim it?
            if (!token.get()
               || token->getType() != SqlSQL2TokenTypes::LEFT_PAREN) {
                break;
            }
            token = token->getNextSibling();
            if (!token.get()
               || token->getType() != SqlSQL2TokenTypes::COLUMN_NAME_LIST) {
                break;
            }
            js = std::make_shared<query::JoinSpec>(
                    _processColumn(token->getFirstChild()));
            token = token->getNextSibling();
            if (!token.get()
               || token->getType() != SqlSQL2TokenTypes::RIGHT_PAREN) {
                break;
            }
            return js;
        case SqlSQL2TokenTypes::JOIN_CONDITION:
            token = specToken->getFirstChild();
            if (!token.get()
               || token->getType() != SqlSQL2TokenTypes::SQL2RW_on) {
                throw ParseException("Expected ON in join condition", specToken);
            }
            token = token->getNextSibling();
             if (!token.get()
               || token->getType() != SqlSQL2TokenTypes::OR_OP) {
                throw ParseException("Expected OR_OP in join condition", specToken);
            }
            bt = _bFactory.newOrTerm(token);
            js = std::make_shared<query::JoinSpec>(bt->getReduced());
            return js;

        default:
            break;
        }
        throw ParseException("Invalid join spec token", token);
        return std::shared_ptr<query::JoinSpec>(); // should never reach
    }
    void  _processJoinCondition(RefAST joinCondition) const {
        std::cout << "Join condition: " << walkIndentedString(joinCondition) << "\n";
    }

    query::JoinRef::Type _convertToJoinType(RefAST joinSequence) const {
        while(1) {
            if (!joinSequence.get()) {
                throw ParseException("Null token for join type", joinSequence);
            }
            switch(joinSequence->getType()) {
            case SqlSQL2TokenTypes::SQL2RW_inner:
                return query::JoinRef::INNER;
            case SqlSQL2TokenTypes::SQL2RW_left:
                return query::JoinRef::LEFT;
            case SqlSQL2TokenTypes::SQL2RW_right:
                return query::JoinRef::RIGHT;
            case SqlSQL2TokenTypes::SQL2RW_full:
                return query::JoinRef::FULL;
            case SqlSQL2TokenTypes::SQL2RW_outer:
                continue; // Implied by left, right, full
            case SqlSQL2TokenTypes::SQL2RW_join:
                return query::JoinRef::DEFAULT; // No join type specified
            default:
                throw ParseException("Unexpected token for join type",
                                     joinSequence);
            } // case
        }
    }
    /// Import a column ref
    /// Bail out if multiple column refs
    /// column_name_list :
    ///    column_name (COMMA column_name)*
    /// ;
    std::shared_ptr<query::ColumnRef> _processColumn(RefAST sib) const {
        if (!sib.get()) {
            throw ParseException("NULL column node", sib); }
        if (sib->getType() != SqlSQL2TokenTypes::REGULAR_ID) {
            throw ParseException("Bad column node for USING", sib); }
        std::shared_ptr<query::ColumnRef> c =
                std::make_shared<query::ColumnRef>("", "", tokenText(sib));
        return c;
    }
    query::TableRef::Ptr _processQualifiedName(RefAST n) const {
        RefAST qnStub = n;
        RefAST aliasN = _aliases->getAlias(qnStub);
        std::string alias;
        if (aliasN.get()) alias = aliasN->getText();
        QualifiedName qn(n->getFirstChild());
        std::string db;
        if (qn.names.size() > 1) db = qn.getQual(1);
        return std::make_shared<query::TableRef>(
                db,
                qn.getName(),
                alias);
    }
    query::TableRef::Ptr _processSubquery(RefAST n) const {
        throw ParseException("Subqueries unsupported", n->getFirstChild());
    }

    // Fields
    antlr::RefAST _cursor;
    std::shared_ptr<ParseAliasMap> _aliases;
    BoolTermFactory& _bFactory;
};
////////////////////////////////////////////////////////////////////////
// FromFactory (impl)
////////////////////////////////////////////////////////////////////////
FromFactory::FromFactory(std::shared_ptr<ParseAliasMap> aliases,
                         std::shared_ptr<ValueExprFactory> vf)
    : _aliases(aliases),
      _bFactory(std::make_shared<BoolTermFactory>(vf)) {
}

std::shared_ptr<query::FromList>
FromFactory::getProduct() {
    return _list;
}

void
FromFactory::attachTo(SqlSQL2Parser& p) {
    std::shared_ptr<TableRefListH> lh(new TableRefListH(*this));
    p._tableListHandler = lh;
    std::shared_ptr<TableRefAuxH> ah = std::make_shared<TableRefAuxH>(_aliases);
    p._tableAliasHandler = ah;
}

void
FromFactory::_import(antlr::RefAST a) {
    std::shared_ptr<query::TableRefList> r = std::make_shared<query::TableRefList>();
    _list = std::make_shared<query::FromList>(r);

    assert(_bFactory);
    for(RefGenerator refGen(a, _aliases, *_bFactory);
        !refGen.isDone();
        refGen.next()) {
        query::TableRef::Ptr p = refGen.get();
        _list->_tableRefs->push_back(p);
    }
}

}}} // namespace lsst::qserv::parser
