// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

/// \file
/// \brief Implementation of parallel query validation/rewriting.

// Class header
#include "qana/RelationGraph.h"

// System headers
#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/QueryNotEvaluableError.h"
#include "qana/TableInfoPool.h"

#include "parser/SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes
#include "query/ColumnRef.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/Predicate.h"
#include "query/QueryContext.h"
#include "query/QueryTemplate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "global/constants.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.RelationGraph");
}

namespace lsst {
namespace qserv {
namespace qana {

using query::AndTerm;
using query::BoolFactor;
using query::BoolTerm;
using query::ColumnRef;
using query::CompPredicate;
using query::FuncExpr;
using query::JoinRef;
using query::JoinRefPtrVector;
using query::JoinSpec;
using query::OrTerm;
using query::QueryContext;
using query::QueryTemplate;
using query::SelectStmt;
using query::TableRef;
using query::TableRefList;
using query::ValueExprPtrVector;
using query::ValueExprPtr;
using query::ValueFactor;
using query::ValueFactorPtr;


// ----------------------------------------------------------------
// Vertex implementation

void Vertex::insert(Edge const& e) {
    typedef std::vector<Edge>::iterator Iter;
    // Look for an existing edge incident to the same vertex as e using
    // binary search.
    Iter i = std::lower_bound(edges.begin(), edges.end(), e);
    if (i == edges.end() || *i != e) {
        // There isn't one, so insert e into edges, making sure to maintain
        // the sortedness of edges.
        edges.insert(i, e);
    } else {
        // There is one. Keeping both edges around isn't useful for the query
        // validation algorithm, so we look at both e and the existing edge,
        // and retain the one that results in the smallest required overlap
        // increase when traversed by the query validation algorithm.
        bool si = i->isSpatial();
        bool se = e.isSpatial();
        if (si && se) {
            // Both edges are spatial - retain the one with the smaller angular
            // separation threshold.
            i->angSep = std::min(e.angSep, i->angSep);
        } else {
            // Either both edges are non-spatial (and identical), or we
            // have both a spatial constraint and an equality predicate.
            // Spatial edges are only admissible between director tables,
            // and equality predicates between different directors are not
            // admissible. So, a couple sample queries that can lead to this
            // corner case are:
            //
            // SELECT ... FROM Object AS o1 INNER JOIN Object AS o2 ON
            //     scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.1 AND
            //     o1.objectId = o2.objectId;
            //
            // or
            //
            // SELECT ... FROM Object AS o, Source AS s WHERE
            //     o.objectId = s.objectId AND o.objectId = s.objectId;
            i->angSep = std::numeric_limits<double>::quiet_NaN();
        }
    }
}


// ----------------------------------------------------------------
// RelationGraph implementation

namespace {

/// `findFirstNonTrivialChild` returns the first node in `tree` that is not an
/// AndTerm or OrTerm with a single child. The return value can be an AndTerm
/// or OrTerm with multiple children, a BoolFactor, or an UnknownTerm, and may
/// just be the input tree.
BoolTerm::Ptr findFirstNonTrivialChild(BoolTerm::Ptr tree) {
    while (true) {
        AndTerm* at = dynamic_cast<AndTerm *>(tree.get());
        OrTerm* ot = dynamic_cast<OrTerm *>(tree.get());
        if (at && at->_terms.size() == 1) {
            tree = at->_terms.front();
        } else if (ot && ot->_terms.size() == 1) {
            tree = ot->_terms.front();
        } else {
            break;
        }
    }
    return tree;
}

bool isOuterJoin(JoinRef::Type jt) {
    return jt == JoinRef::LEFT || jt == JoinRef::RIGHT || jt == JoinRef::FULL;
}

/// `getColumnRef` returns the ColumnRef in `ve` if there is one.
ColumnRef::Ptr getColumnRef(ValueExprPtr const& ve) {
    ColumnRef::Ptr cr;
    if (!ve) {
        return cr;
    }
    return ve->getColumnRef();
}

/// `verifyColumnRef` checks that a column reference has a column name and an
/// empty database name (because at this stage, fully qualified names should
/// have been rewritten to use a table alias).
void verifyColumnRef(ColumnRef const& c) {
    if (c.column.empty()) {
        throw std::logic_error(
            "Parser/query analysis bug: "
            "ColumnRef with an empty column name.");
    } else if (!c.db.empty()) {
        if (c.table.empty()) {
            throw std::logic_error(
                "Parser/query analysis bug: ColumnRef has an empty "
                "table/alias name but a non-empty database name.");
        }
        throw std::logic_error(
            "Query analysis bug: the db.table portion of a fully "
            "qualified column name was not replaced with an alias.");
    }
}

/// `verifyJoin` throws an exception if the given join parameters are invalid
/// or unsupported.
void verifyJoin(JoinRef::Type joinType,
                bool natural,
                JoinSpec::Ptr const& joinSpec)
{
    switch (joinType) {
        case JoinRef::UNION:
            // "table1 UNION JOIN table2" is probably the same thing as
            // "table1 FULL OUTER JOIN table2 ON FALSE". It is deprecated in
            // SQL99 and removed from SQL2003. Bail out because MySQL supports
            // neither union nor full outer joins.
            throw QueryNotEvaluableError(
                "UNION JOIN queries are not currently supported.");
        case JoinRef::FULL:
            // MySQL does not support full outer joins. Though it is possible
            // to rewrite a full outer join as a UNION of a LEFT and RIGHT join
            // (in the absence of aggregation), this is complicated and likely
            // slow, so bail out.
            throw QueryNotEvaluableError(
                "FULL OUTER JOIN queries are not currently supported.");
        case JoinRef::CROSS:
            if (natural || joinSpec) {
                throw std::logic_error(
                    "Parser/query analysis bug: a CROSS JOIN cannot be "
                    "NATURAL or have an ON or USING clause.");
            }
            break;
        case JoinRef::DEFAULT: // fallthrough
        case JoinRef::INNER:   // fallthrough
        case JoinRef::LEFT:    // fallthrough
        case JoinRef::RIGHT:
            if (natural && joinSpec) {
                throw std::logic_error(
                    "Parser/query analysis bug: a JOIN cannot be NATURAL "
                    "and have an ON or USING clause.");
            }
            break;
        default:
            throw std::logic_error(
                "Parser/query analysis bug: unrecognized join type.");
    }
}

/// `addEqEdge` checks whether an equality predicate involving column `ca`
/// from the table reference in `a` and `cb` from `b` is admissible, and
/// adds corresponding `Edge` objects to each vertex if so. The number of
/// edges added, 0 or 1, is returned.
size_t addEqEdge(std::string const& ca,
                 std::string const& cb,
                 bool outer,
                 Vertex* a,
                 Vertex* b)
{
    if (a == b) {
        // Loops are useless for query analysis.
        return 0;
    }
    TableInfo const& ta = *(a->info);
    TableInfo const& tb = *(b->info);
    LOGS(_log, LOG_LVL_DEBUG, "addEqEdge a=" << *a->info << " b=" << *b->info);
    if (ta.isEqPredAdmissible(tb, ca, cb, outer)) {
        // Add a pair of Edge objects, a → b and b → a.
        LOGS(_log, LOG_LVL_DEBUG, "addEqEdge true for (" << ca << "," << cb << ")");
        a->insert(Edge(b, std::numeric_limits<double>::quiet_NaN()));
        b->insert(Edge(a, std::numeric_limits<double>::quiet_NaN()));
        return 1;
    }
    LOGS(_log, LOG_LVL_DEBUG, "addEqEdge false for (" << ca << "," << cb << ")");
    return 0;
}

/// `getNumericConst` returns the numeric constant embedded in the given
/// value expression if there is one, and NaN otherwise.
double getNumericConst(ValueExprPtr const& ve) {
    if (!ve || ve->getFactorOps().size() != 1) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    ValueFactorPtr vf = ve->getFactorOps().front().factor;
    if (!vf || vf->getType() != ValueFactor::CONST) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    char *e = nullptr;
    double a = std::strtod(vf->getTableStar().c_str(), &e);
    if (e == vf->getTableStar().c_str()) {
        // conversion error - non-numeric constant
        return std::numeric_limits<double>::quiet_NaN();
    }
    return a;
}

/// `getAngSepFunc` returns a pointer to the IR node for the `scisql_angSep`
/// call embedded in the given value expression if there is one, and null
/// otherwise.
FuncExpr::Ptr getAngSepFunc(ValueExprPtr const& ve) {
    FuncExpr::Ptr fe;
    if (!ve || ve->getFactorOps().size() != 1) {
        return fe;
    }
    ValueFactorPtr vf = ve->getFactorOps().front().factor;
    if (!vf || vf->getType() != ValueFactor::FUNCTION) {
        return fe;
    }
    fe = vf->getFuncExpr();
    if (!fe || fe->name != "scisql_angSep" || fe->params.size() != 4) {
        return FuncExpr::Ptr();
    }
    return fe;
}

/// `getEqColumnRefs` returns the pair of column references in the equality
/// predicate embedded in the given boolean factor. If that is not what
/// the given boolean term corresponds to, a pair of nulls is returned instead.
std::pair<ColumnRef::Ptr, ColumnRef::Ptr> const getEqColumnRefs(
    BoolTerm::Ptr const& bt)
{
    std::pair<ColumnRef::Ptr, ColumnRef::Ptr> p;
    // Look for a BoolFactor containing a single CompPredicate.
    BoolFactor::Ptr bf = std::dynamic_pointer_cast<BoolFactor>(bt);
    if (!bf || bf->_terms.size() != 1) {
        return p;
    }
    CompPredicate::Ptr cp =
        std::dynamic_pointer_cast<CompPredicate>(bf->_terms.front());
    if (!cp || cp->op != SqlSQL2TokenTypes::EQUALS_OP) {
        return p;
    }
    // Extract column references (if they exist)
    ColumnRef::Ptr l = getColumnRef(cp->left);
    ColumnRef::Ptr r = getColumnRef(cp->right);
    if (!l || !r) {
        return p;
    }
    verifyColumnRef(*l);
    verifyColumnRef(*r);
    return std::make_pair(l, r);
}

} // anonymous namespace


/// `_addOnEqEdges` adds a graph edge for each admissible top-level equality
/// predicate extracted from the ON clause of the join between table references
/// in this graph and `g`. The number of admissible predicates is returned.
size_t RelationGraph::_addOnEqEdges(BoolTerm::Ptr on,
                                    bool outer,
                                    RelationGraph& g)
{
    size_t numEdges = 0;
    on = findFirstNonTrivialChild(on);
    AndTerm::Ptr at = std::dynamic_pointer_cast<AndTerm>(on);
    if (at) {
        // Recurse to the children.
        typedef BoolTerm::PtrVector::const_iterator BtIter;
        for (BtIter i = at->_terms.begin(), e = at->_terms.end(); i != e; ++i) {
            numEdges += _addOnEqEdges(*i, outer, g);
        }
        return numEdges;
    }
    std::pair<ColumnRef::Ptr, ColumnRef::Ptr> c = getEqColumnRefs(on);
    if (!c.first) {
        // on is not an equality predicate between two column references
        return 0;
    }
    // Lookup column references in graphs being joined together
    std::vector<Vertex*> const& a1 = _map.find(*c.first);
    std::vector<Vertex*> const& b1 = g._map.find(*c.first);
    std::vector<Vertex*> const& a2 = _map.find(*c.second);
    std::vector<Vertex*> const& b2 = g._map.find(*c.second);
    if ((!a1.empty() && !b1.empty()) || (!a2.empty() && !b2.empty())) {
        // At least one column reference was found in both graphs
        QueryTemplate qt;
        if (a1.empty()) { c.first->renderTo(qt); }
        else { c.second->renderTo(qt); }
        throw QueryNotEvaluableError("Column reference " + qt.sqlFragment() +
                                     " is ambiguous");
    }
    if ((a1.empty() && b1.empty()) || (a2.empty() && b2.empty())) {
        // At least one column reference wasn't found
        return 0;
    }
    if ((!a1.empty() && !a2.empty()) || (!b1.empty() && !b2.empty())) {
        // Both column references were found in the same graph. The predicate
        // cannot be used for partition inference if it comes from the ON
        // clause of an outer join. To see why, consider the following query:
        //
        // SELECT * FROM (A JOIN B) LEFT JOIN C ON A.id = B.id AND B.id = C.id;
        //
        // This query can return rows with A.id != B.id, in which case columns
        // from C will be filled in with NULLs. On the other hand, if the query
        // is:
        //
        // SELECT * FROM A LEFT JOIN B ON A.id = B.id;
        //
        // then the predicate is usable for partition inference, since all
        // results will satisfy A.id = B.id OR B.id IS NULL, and checking
        // whether or not a row r from A matches any rows in B only requires
        // looking at rows from B that have the same partition as r.
        if (outer) {
            return 0;
        }
    }
    // Both column references were found in different graphs, or they were
    // found in the same graph but the equality predicate was not extracted
    // from the ON clause of an outer join.
    //
    // Get the list of vertices that each column reference maps to, and add
    // edges between each possible vertex pair.
    std::vector<Vertex*> const& v1 = a1.empty() ? b1 : a1;
    std::vector<Vertex*> const& v2 = a2.empty() ? b2 : a2;
    typedef std::vector<Vertex*>::const_iterator VertIter;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += addEqEdge(
                c.first->column, c.second->column, outer, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_addNaturalEqEdges` adds an edge for each (implicit) admissible equality
/// predicate in the natural join between table references from this graph and
/// `g`. The number of admissible predicates is returned.
size_t RelationGraph::_addNaturalEqEdges(bool outer, RelationGraph& g)
{
    typedef std::vector<std::string>::const_iterator ColIter;
    typedef std::vector<Vertex*>::const_iterator VertIter;

    // Find interesting unqualified column names that are shared between
    // the vertices of this graph and g.
    std::vector<std::string> const cols = _map.computeCommonColumns(g._map);
    std::string const _; // an empty string
    size_t numEdges = 0;
    for (ColIter c = cols.begin(), e = cols.end(); c != e; ++c) {
        // Lookup the vertices for each shared column, and add edges between
        // each possible vertex pair.
        ColumnRef const cr(_, _, *c);
        std::vector<Vertex*> const& v1 = _map.find(cr);
        std::vector<Vertex*> const& v2 = g._map.find(cr);
        for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
            for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
                numEdges += addEqEdge(*c, *c, outer, *i1, *i2);
            }
        }
    }
    return numEdges;
}

/// `_addUsingEqEdges` adds an edge for each admissible equality predicate
/// implied by the USING clause of a join between table references from this
/// graph and `g`. The number of admissible predicates is returned.
size_t RelationGraph::_addUsingEqEdges(ColumnRef const& c,
                                       bool outer,
                                       RelationGraph& g)
{
    typedef std::vector<Vertex*>::const_iterator VertIter;

    if (!c.db.empty() || !c.table.empty()) {
        throw QueryNotEvaluableError(
            "USING clause contains qualified column name");
    }
    // Lookup the vertices for the unqualified column reference in both graphs,
    // and add edges for each possible vertex pair.
    std::vector<Vertex*> const& v1 = _map.find(c);
    std::vector<Vertex*> const& v2 = g._map.find(c);
    size_t numEdges = 0;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += addEqEdge(c.column, c.column, outer, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_addWhereEqEdges` adds an edge for each admissible top-level equality
/// predicate extracted from the WHERE clause of a query. The number of
/// admissible predicates is returned.
size_t RelationGraph::_addWhereEqEdges(BoolTerm::Ptr where)
{
    size_t numEdges = 0;
    where = findFirstNonTrivialChild(where);
    AndTerm::Ptr at = std::dynamic_pointer_cast<AndTerm>(where);
    if (at) {
        // Recurse to the children.
        typedef BoolTerm::PtrVector::const_iterator BtIter;
        for (BtIter i = at->_terms.begin(), e = at->_terms.end(); i != e; ++i) {
            numEdges += _addWhereEqEdges(*i);
        }
        return numEdges;
    }
    std::pair<ColumnRef::Ptr, ColumnRef::Ptr> c = getEqColumnRefs(where);
    if (!c.first) {
        // where is not an equality predicate between two column references
        return 0;
    }
    LOGS(_log, LOG_LVL_DEBUG, "_addWhereEqEdges first=" << *(c.first) << " second=" << *(c.second));
    // Lookup the vertices for each column reference,
    // and add edges for each possible vertex pair.
    std::vector<Vertex*> const& v1 = _map.find(*c.first);
    std::vector<Vertex*> const& v2 = _map.find(*c.second);
    typedef std::vector<Vertex*>::const_iterator VertIter;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += addEqEdge(
                c.first->column, c.second->column, false, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_addSpEdges` creates a graph edge for each admissible top-level spatial
/// predicate extracted from the given boolean term. The number of admissible
/// predicates is returned.
size_t RelationGraph::_addSpEdges(BoolTerm::Ptr bt)
{
    size_t numEdges = 0;
    bt = findFirstNonTrivialChild(bt);
    AndTerm::Ptr at = std::dynamic_pointer_cast<AndTerm>(bt);
    if (at) {
        // Recurse to the children.
        typedef BoolTerm::PtrVector::const_iterator BtIter;
        for (BtIter i = at->_terms.begin(), e = at->_terms.end(); i != e; ++i) {
            numEdges += _addSpEdges(*i);
        }
        return numEdges;
    }
    // Look for a BoolFactor containing a single CompPredicate.
    BoolFactor::Ptr bf = std::dynamic_pointer_cast<BoolFactor>(bt);
    if (!bf || bf->_terms.size() != 1) {
        return 0;
    }
    CompPredicate::Ptr cp =
        std::dynamic_pointer_cast<CompPredicate>(bf->_terms.front());
    if (!cp) {
        return 0;
    }
    // Try to extract a scisql_angSep() call and a numeric constant
    // from the comparison predicate.
    FuncExpr::Ptr fe;
    double angSep = std::numeric_limits<double>::quiet_NaN();
    switch (cp->op) {
        case SqlSQL2TokenTypes::LESS_THAN_OP: // fallthrough
        case SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP:
            fe = getAngSepFunc(cp->left);
            angSep = getNumericConst(cp->right);
            break;
        case SqlSQL2TokenTypes::GREATER_THAN_OP: // fallthrough
        case SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP:
            angSep = getNumericConst(cp->left);
            fe = getAngSepFunc(cp->right);
            break;
        case SqlSQL2TokenTypes::EQUALS_OP:
            // While this doesn't make much sense numerically (floating
            // point numbers are being tested for equality), it is
            // technically evaluable.
            fe = getAngSepFunc(cp->left);
            if (!fe) {
                angSep = getNumericConst(cp->left);
                fe = getAngSepFunc(cp->right);
            } else {
                angSep = getNumericConst(cp->right);
            }
            break;
    }
    if (!fe || boost::math::isnan(angSep)) {
        // The scisql_angSep() call and/or numeric constant is missing,
        // or the comparison operator is invalid (e.g.
        // "angSep < scisql_angSep(...)")
        return 0;
    }
    // Extract column references from fe
    ValueExprPtrVector::const_iterator j = fe->params.begin();
    ColumnRef::Ptr cr[4];
    Vertex* v[4];
    for (int i = 0; i < 4; ++i) {
        cr[i] = getColumnRef(*j++);
        if (!cr[i]) {
            // Argument i is not a column reference
            return 0;
        }
        std::vector<Vertex*> const& vv = _map.find(*cr[i]);
        if (vv.size() != 1) {
            // Column reference not found, or references multiple vertices.
            return 0;
        }
        v[i] = vv.front();
    }
    // For the predicate to be admissible, the columns in each coordinate
    // pair must come from the same table reference. Additionally, the two
    // coordinate pairs must come from different table references.
    if (v[0] != v[1] || v[2] != v[3] || v[0] == v[2]) {
        return 0;
    }
    // Check that both column pairs were found in director tables
    DirTableInfo const* d1 = dynamic_cast<DirTableInfo const*>(v[0]->info);
    DirTableInfo const* d2 = dynamic_cast<DirTableInfo const*>(v[2]->info);
    if (!d1 || !d2) {
        return 0;
    }
    // Check that the arguments map to the proper director spatial columns
    if (cr[0]->column != d1->lon || cr[1]->column != d1->lat ||
        cr[2]->column != d2->lon || cr[3]->column != d2->lat) {
        return 0;
    }
    // Check that both directors have the same partitioning
    if (d1->partitioningId != d2->partitioningId) {
        return 0;
    }
    // Finally, add an edge between v[0] and v[2].
    v[0]->insert(Edge(v[2], angSep));
    v[2]->insert(Edge(v[0], angSep));
    return 1;
}

/// `_fuse` fuses the relation graph `g` into this one, adding edges
/// for all admissible join predicates extracted from the given join
/// parameters. `g` is emptied as a result.
void RelationGraph::_fuse(JoinRef::Type joinType,
                          bool natural,
                          JoinSpec::Ptr const& joinSpec,
                          RelationGraph& g)
{
    if (this == &g) {
        throw std::logic_error(
            "A RelationGraph cannot be join()ed with itself.");
    }
    verifyJoin(joinType, natural, joinSpec);
    // Deal with unpartitioned relations
    if (empty()) {
        if (g.empty()) {
            // Arbitrary joins are allowed between unpartitioned relations,
            // and there is no need to store any information about them.
            return;
        }
        // In general, "A LEFT JOIN B" is not evaluable if A is unpartitioned
        // and B is partitioned. While there are specific cases that do work
        // (e.g. "A LEFT JOIN B ON FALSE"), the effort to detect them does not
        // seem worthwhile.
        if (joinType == JoinRef::LEFT) {
            throw QueryNotEvaluableError(
                "Query contains a LEFT JOIN between unpartitioned and "
                "partitioned tables.");
        }
        swap(g);
        return;
    } else if (g.empty()) {
        // In general, "A RIGHT JOIN B" is not evaluable if A is partitioned
        // and B is unpartitioned.
        if (joinType == JoinRef::RIGHT) {
            throw QueryNotEvaluableError(
                "Query contains a RIGHT JOIN between partitioned and "
                "unpartitioned tables.");
        }
        return;
    }
    bool const outer = isOuterJoin(joinType);
    size_t numEdges = 0;
    std::vector<std::string> usingCols;
    if (natural) {
        numEdges += _addNaturalEqEdges(outer, g);
    } else if (joinSpec && joinSpec->getUsing()) {
        ColumnRef const& c = *joinSpec->getUsing();
        numEdges += _addUsingEqEdges(c, outer, g);
        usingCols.push_back(c.column);
    } else if (joinSpec && joinSpec->getOn()) {
        numEdges += _addOnEqEdges(joinSpec->getOn(), outer, g);
    }
    if (outer && numEdges == 0) {
        // For outer joins, require the presence of at least one admissible
        // join predicate. Doing this means that determining whether or not
        // a row from the left and/or right relation of an outer join has a
        // match on the right/left only requires looking at data from the
        // same partition. For inner joins, admissible predicates can be
        // provided later (e.g. in the WHERE clause).
        throw QueryNotEvaluableError(
            "Unable to evaluate query by joining only partition-local data");
    }
    // Splice g into this graph.
    _vertices.splice(_vertices.end(), g._vertices);
    _map.fuse(g._map, natural, usingCols);
    // Add spatial edges
    if (!outer && joinSpec && joinSpec->getOn()) {
        _addSpEdges(joinSpec->getOn());
    }
}

/// This constructor creates a relation graph for a single partitioned
/// table reference.
/// `matchAngSep` - assumed angular separation for match tables
RelationGraph::RelationGraph(TableRef& tr, TableInfo const* info) :
    _query(0)
{
    typedef std::vector<ColumnRefConstPtr>::const_iterator Iter;

    if (!info) {
        return;
    } else if (info->kind != TableInfo::MATCH) {
        LOGS(_log, LOG_LVL_DEBUG, "RG: non-match table tr=\"" << tr << "\" info=" << *info);
        _vertices.push_back(Vertex(tr, info));
        ColumnVertexMap m(_vertices.front());
        _map.swap(m);
    } else {
        double matchAngSep = static_cast<MatchTableInfo const&>(*info).angSep;
        LOGS(_log, LOG_LVL_DEBUG, "RG: match table tr=\"" << tr << "\" info=" << *info
                << " matchAngSep=" << matchAngSep);
        // Decompose match table references into a pair of vertices - one for
        // each foreign key in the match table.
        _vertices.push_back(Vertex(tr, info));
        _vertices.push_back(Vertex(tr, info));
        // Create a spatial edge between the vertex pair. Note that if the
        // match table metadata included the maximum angular separation between
        // matched entities, it could be used instead of the partition overlap
        // below (the latter is an upper bound on the former).
        _vertices.front().insert(Edge(&_vertices.back(), matchAngSep));
        _vertices.back().insert(Edge(&_vertices.front(), matchAngSep));
        // Split column references for the match table reference across vertices.
        std::vector<ColumnRefConstPtr> refs =
            info->makeColumnRefs(tr.getAlias());
        std::sort(refs.begin(), refs.end(), ColumnRefLt());
        Iter begin = refs.begin();
        Iter middle = begin;
        Iter end = refs.end();
        for (; middle != end; ++middle) {
            if ((*middle)->column != refs.front()->column) {
                break;
            }
        }
        ColumnVertexMap m1(_vertices.front(), begin, middle);
        ColumnVertexMap m2(_vertices.back(), middle, end);
        std::vector<std::string> _; // an empty column name list
        m1.fuse(m2, false, _);
        _map.swap(m1);
    }
}

/// This constructor creates a relation graph for a `TableRef`
/// and its constituent joins.
RelationGraph::RelationGraph(TableRef::Ptr const& tr, TableInfoPool& pool) :
    _query(0)
{
    if (!tr) {
        throw std::logic_error(
            "Parser/query analysis bug: NULL TableRef pointer "
            "passed to RelationGraph constructor.");
    }
    LOGS(_log, LOG_LVL_DEBUG, "RG: tr=" << *tr);

    // Create a graph for the left-most table in a join sequence.
    RelationGraph g(*tr, pool.get(tr->getDb(), tr->getTable()));
    // Process remaining tables in the JOIN sequence. Note that joins are
    // left-associative in the absence of parentheses, i.e. "A JOIN B JOIN C"
    // is equivalent to "(A JOIN B) JOIN C", and that relation graphs are
    // built in join precedence order. This is important for proper column
    // reference resolution - for instance, an unqualified column reference
    // "foo" might be unambiguous in the ON clause of "A JOIN B", but ambiguous
    // in the ON clause for "(A JOIN B) JOIN C".
    JoinRefPtrVector const& joins = tr->getJoins();
    typedef JoinRefPtrVector::const_iterator Iter;
    for (Iter i = joins.begin(), e = joins.end(); i != e; ++i) {
        JoinRef& j = **i;
        RelationGraph tmp(j.getRight(), pool);
        g._fuse(j.getJoinType(), j.isNatural(), j.getSpec(), tmp);
    }
    swap(g);
}


namespace {

/// A singly-linked list of vertices, with link storage embedded directly
/// in the Vertex struct. This allows relation graph traversal to proceed
/// without memory allocation.
struct VertexQueue {
    Vertex* head; // unowned
    Vertex* tail; // unowned

    VertexQueue() : head(0), tail(0) {}

    /// `dequeue` removes and returns a vertex from the queue. If the queue is
    /// empty, a null pointer is returned.
    Vertex* dequeue() {
        if (head) {
            Vertex* v = head;
            head = head->next;
            if (!head) {
                tail = 0;
            }
            v->next = 0;
            return v;
        }
        return 0;
    }

    /// `enqueue` inserts a vertex into the queue. If the vertex is already in
    /// the queue, there is no effect.
    void enqueue(Vertex* v) {
        if (v->next || v == tail) {
            // v is already in the queue
            return;
        }
        if (!head) {
            head = tail = v;
        } else {
            tail->next = v;
            tail = v;
        }
    }
};

/// `computeMinimumOverlap` visits every vertex linked to `v` via one or more
/// paths and computes its minimum required overlap.
void computeMinimumOverlap(Vertex& vtx)
{
    typedef std::vector<Edge>::const_iterator Iter;

    VertexQueue q;
    // The required overlap for the initial vertex is 0.
    vtx.overlap = 0;
    for (Vertex* v = &vtx; v != nullptr; v = q.dequeue()) {
        // Loop over edges incident to v.
        for (Iter e = v->edges.begin(), end = v->edges.end(); e != end; ++e) {
            Vertex* u = e->vertex;
            double const prevRequiredOverlap = u->overlap;
            // Child tables have no available overlap and directors have
            // available overlap equal to the partition overlap. Match table
            // joins require no overlap on one side of a 3-way equi-join. We
            // enforce this by only allowing overlap if we are reaching a match
            // table vertex from another match table vertex. This works because
            // we never create relation graph edges between different match
            // table references, i.e. a match → match edge will always be
            // between the pair of vertices created for a single match table
            // reference.
            double availableOverlap = 0.0;
            if (u->info->kind == TableInfo::DIRECTOR) {
                availableOverlap = static_cast<DirTableInfo const&>(*u->info).overlap;
            } else if (u->info->kind == TableInfo::MATCH &&
                 v->info->kind == TableInfo::MATCH) {
                availableOverlap = static_cast<MatchTableInfo const&>(*u->info).angSep;
            }
            // The overlap required for u is the overlap required for v plus
            // the angular separation threshold of the edge between them.
            double requiredOverlap = v->overlap;
            if (e->isSpatial()) {
                requiredOverlap += e->angSep;
            }
            // If requiredOverlap is greater than or equal to the previously
            // computed required overlap for u, then there is no need to visit
            // u again. This is because the current path between the initial
            // vertex and u does not have a strictly smaller sum of angular
            // separations, so any path to vertices reachable from u containing
            // the current path as a prefix will have an angular sum greater
            // than or equal to the one obtained by substituting the previous
            // path to u as the prefix. Note that the required overlap for an
            // unvisited vertex is ∞.
            //
            // If requiredOverlap is greater than the available overlap for u,
            // then either the query is not evaluable or we will reach u via
            // some other path that has smaller required overlap, so again
            // there is no reason to visit u.
            if (requiredOverlap <= availableOverlap &&
                requiredOverlap < prevRequiredOverlap) {
                // Set the required overlap for u and add it to the
                // vertex visitation queue.
                u->overlap = requiredOverlap;
                q.enqueue(u);
            }
        }
    }
}

/// `isEvaluable` returns `true` if no graph vertex requires infinite overlap.
bool isEvaluable(std::list<Vertex> const& vertices)
{
    typedef std::list<Vertex>::const_iterator Iter;
    for (Iter v = vertices.begin(), e = vertices.end(); v != e; ++v) {
        if ((boost::math::isinf)(v->overlap)) {
            return false;
        }
    }
    return true;
}

/// `resetVertices` sets the required overlap of all graph vertices to ∞.
void resetVertices(std::list<Vertex>& vertices)
{
    typedef std::list<Vertex>::iterator Iter;
    for (Iter v = vertices.begin(), e = vertices.end(); v != e; ++v) {
        v->overlap = std::numeric_limits<double>::infinity();
    }
}

} // anonymous namespace


/// `_validate` searches for a graph traversal that proves the input query
/// is evaluable.
bool RelationGraph::_validate()
{
    typedef std::list<Vertex>::iterator Iter;
    size_t numStarts = 0;
    for (Vertex& vtx: _vertices) {
        if (vtx.info->kind != TableInfo::MATCH) {
            ++numStarts;
            resetVertices(_vertices);
            computeMinimumOverlap(vtx);
            if (isEvaluable(_vertices)) {
                return true;
            }
            // At least one vertex still has infinite required overlap, so
            // the graph is disconnected or too much overlap is required.
            // Try again with another starting vertex.
        }
    }
    if (numStarts == 0) {
        // If the input query involves only unpartitioned tables, or just a
        // single match table, it can be evaluated. If it involves more than
        // one match table, its relation graph must be disconnected.
        return _vertices.empty() || _vertices.size() == 2;
    }
    return false;
}

RelationGraph::RelationGraph(SelectStmt& stmt, TableInfoPool& pool) :
    _query(&stmt)
{
    LOGS(_log, LOG_LVL_DEBUG, "RG: stmt=" << stmt);

    // Check that at least one thing is being selected.
    if (!stmt.getSelectList().getValueExprList() ||
        stmt.getSelectList().getValueExprList()->empty()) {
        throw QueryNotEvaluableError("Query has no select list");
    }
    // Check that the FROM clause isn't empty.
    TableRefList const& refs = stmt.getFromList().getTableRefList();
    if (refs.empty()) {
        throw QueryNotEvaluableError(
            "Query must include at least one table reference");
    }
    // Build a graph for the first entry in the from list
    RelationGraph g(refs.front(), pool);
    // "SELECT ... FROM A, B, C, ..." is equivalent to
    // "SELECT ... FROM ((A CROSS JOIN B) CROSS JOIN C) ..."
    typedef TableRefList::const_iterator Iter;
    for (Iter i = ++refs.begin(), e = refs.end(); i != e; ++i) {
        RelationGraph tmp(*i, pool);
        g._fuse(JoinRef::CROSS, false, JoinSpec::Ptr(), tmp);
    }
    // Add edges for admissible join predicates extracted from the
    // WHERE clause
    if (stmt.hasWhereClause()) {
        BoolTerm::Ptr where = stmt.getWhereClause().getRootTerm();
        g._addWhereEqEdges(where);
        g._addSpEdges(where);
    }

    g._dumpGraph();

    if (!g._validate()) {
        throw QueryNotEvaluableError(
            "Query involves partitioned table joins that Qserv does not "
            "know how to evaluate using only partition-local data");
    }

    swap(g);
}

void RelationGraph::_dumpGraph() const {
    // dump graph
    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        LOGS(_log, LOG_LVL_DEBUG, "RelationGraph:");
        std::map<Vertex const*, int> vtxId;
        for (auto&& vtx: _vertices) {
            int id = vtxId.size();
            vtxId[&vtx] = id;
            LOGS(_log, LOG_LVL_DEBUG, "   vertex " << id << " info=" << *vtx.info);
        }
        for (auto&& vtx: _vertices) {
            for (auto&& edge: vtx.edges) {
                LOGS(_log, LOG_LVL_DEBUG, "   edge " << vtxId[&vtx] << " <-> "
                        << vtxId[edge.vertex] << " angSep=" << edge.angSep);
            }
        }
    }
}

void RelationGraph::rewrite(SelectStmtPtrVector& outputs,
                            QueryMapping& mapping)
{
    typedef std::list<Vertex>::iterator ListIter;

    if (!_query) {
        return;
    }
    if (empty()) {
        LOGS(_log, LOG_LVL_TRACE, "Input query only involves unpartitioned tables");
        // The input query only involves unpartitioned tables -
        // there is nothing to do.
        outputs.push_back(_query->clone());
        return;
    }

    LOGS(_log, LOG_LVL_TRACE, "Inserting chunk entry in QueryMapping");
    mapping.insertChunkEntry(CHUNK_TAG);
    // Find directors for which overlap is required. At the same time, rewrite
    // all table references as their corresponding chunk templates.
    std::vector<Vertex*> overlapRefs;
    for (ListIter i = _vertices.begin(), e = _vertices.end(); i != e; ++i) {
        i->rewriteAsChunkTemplate();
        if (i->info->kind == TableInfo::DIRECTOR && i->overlap > 0.0) {
            overlapRefs.push_back(&(*i));
        }
    }
    if (overlapRefs.empty()) {
        // There is no need for sub-chunking, so leave it off for now.
        //
        // Note though that it is not clear that leaving it turned off is
        // better (faster), especially since another query participating in a
        // shared scan over a particular director might require overlap,
        // meaning that creating/loading sub-chunk tables is essentially free.
        //
        // Also, if the graph contains a spatial edge that is a bridge (which
        // would have to have an angular separation threshold of zero) then this
        // strategy can require the evaluation of full chunk-chunk table cross
        // products. Though zero-distance near neighbor queries don't seem to be
        // of much use in practice, they are a vector for DOS attacks, so
        // perhaps we should reconsider.
        outputs.push_back(_query->clone());
        return;
    }
    if (overlapRefs.size() > MAX_TABLE_REFS_WITH_OVERLAP) {
        throw QueryNotEvaluableError("Query contains too many table "
                                     "references that require overlap");
    }
    // At least one table requires overlap, so sub-chunking must be turned on.
    mapping.insertSubChunkEntry(SUBCHUNK_TAG);
    // Rewrite director table references not requiring overlap as their
    // corresponding sub-chunk templates, and record the names of all
    // sub-chunked tables.
    for (ListIter i = _vertices.begin(), e = _vertices.end(); i != e; ++i) {
        if (i->info->kind == TableInfo::DIRECTOR) {
            if (i->overlap == 0.0) {
                i->rewriteAsSubChunkTemplate();
            }
            DbTable dbTable(i->info->database, i->info->table);
            LOGS(_log, LOG_LVL_DEBUG, "rewrite db=" << dbTable.db << " table=" << dbTable.table);
            mapping.insertSubChunkTable(dbTable);
        }
    }
    unsigned n = static_cast<unsigned>(overlapRefs.size());
    unsigned numPermutations = 1 << n;
    // Each director requiring overlap must be rewritten as both a sub-chunk
    // template and an overlap sub-chunk template. There are 2ⁿ different
    // template permutations for n directors requiring overlap; generate them
    // all.
    for (unsigned p = 0; p < numPermutations; ++p) {
        for (unsigned i = 0; i < n; ++i) {
            if ((p & (1 << i)) != 0) {
                overlapRefs[i]->rewriteAsOverlapTemplate();
            } else {
                overlapRefs[i]->rewriteAsSubChunkTemplate();
            }
        }
        // Given the use of shared_ptr by the IR classes, we could shallow
        // copy everything except the FromList as an optimization. But then
        // code which mutates a particular SelectStmt might in fact mutate
        // many SelectStmt objects. If the IR classes were copy-on-write,
        // this wouldn't be an issue.
        outputs.push_back(_query->clone());
    }
}

}}} // namespace lsst::qserv::qana
