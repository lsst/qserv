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
/**
  * @file
  *
  * @brief ScanTablePlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// No public interface
#include "lsst/qserv/master/QueryPlugin.h" // Parent class

#include "lsst/qserv/master/common.h"
#include "lsst/qserv/master/ColumnRefMap.h"
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/QsRestrictor.h"
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/WhereClause.h"

namespace lsst {
namespace qserv {
namespace master {
////////////////////////////////////////////////////////////////////////
// ScanTablePlugin declaration
////////////////////////////////////////////////////////////////////////
/// ScanTablePlugin is a query plugin that detects the "scan tables"
/// of a query. A scan table is a partitioned table that must be
/// scanned in order to answer the query. If the number of chunks
/// involved is less than a threshold number (2, currently), then the
/// scan table annotation is removed--the query is no longer
/// considered a "scanning" query because it involves a small piece of
/// the data set.
class ScanTablePlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<ScanTablePlugin> Ptr;

    virtual ~ScanTablePlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(SelectStmt& stmt, QueryContext&);
    virtual void applyFinal(QueryContext& context);

private:
    StringPairList _findScanTables(SelectStmt& stmt, QueryContext& context);
    StringPairList _scanTables;
};

////////////////////////////////////////////////////////////////////////
// ScanTablePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class ScanTablePluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<ScanTablePluginFactory> Ptr;
    ScanTablePluginFactory() {}
    virtual ~ScanTablePluginFactory() {}

    virtual std::string getName() const { return "ScanTable"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new ScanTablePlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registerScanTablePlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        ScanTablePluginFactory::Ptr f(new ScanTablePluginFactory());
        QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerScanTablePlugin;
}

////////////////////////////////////////////////////////////////////////
// ScanTablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
ScanTablePlugin::applyLogical(SelectStmt& stmt, QueryContext& context) {
    _scanTables = _findScanTables(stmt, context);
    context.scanTables = _scanTables;
}

void
ScanTablePlugin::applyFinal(QueryContext& context) {
    int const scanThreshold = 2;
    if(context.chunkCount < scanThreshold) {
        context.scanTables.clear();
        std::cout << "Squash scan tables: <" << scanThreshold
                  << " chunks." << std::endl;
    }
}

struct getPartitioned : public TableRefN::Func {
    getPartitioned(StringPairList& sList_) : sList(sList_) {}
    virtual void operator()(TableRefN& t) {
        (*this)(const_cast<TableRefN const&>(t));
    }
    virtual void operator()(TableRefN const& tRef) {
        SimpleTableN const* t = dynamic_cast<SimpleTableN const*>(&tRef);
        if(t) {
            StringPair entry(t->getDb(), t->getTable());
            if(found.end() != found.find(entry)) return;
            sList.push_back(entry);
            found.insert(entry);
        } else {
            throw std::logic_error("Unexpected non-simple table in apply()");
        }
    }
    std::set<StringPair> found;
    StringPairList& sList;
};

// helper
StringPairList filterPartitioned(TableRefnList const& tList) {
    StringPairList list;
    getPartitioned gp(list);
    for(TableRefnList::const_iterator i=tList.begin(), e=tList.end();
        i != e; ++i) {
        (**i).apply(gp);
    }
    return list;
}

StringPairList
ScanTablePlugin::_findScanTables(SelectStmt& stmt, QueryContext& context) {
    // Might be better as a separate plugin

    // All tables of a query are scan tables if the statement both:
    // a. has non-trivial spatial scope (all chunks? >1 chunk?)
    // b. requires column reading

    // a. means that the there is a spatial scope specification in the
    // WHERE clause or none at all (everything matches). However, an
    // objectId specification counts as a trivial spatial scope,
    // because it evaluates to a specific set of subchunks. We limit
    // the objectId specification, but the limit can be large--each
    // concrete objectId incurs at most the cost of one subchunk.

    // b. means that columns are needed to process the query.
    // In the SelectList, count(*) does not need columns, but *
    // does. So do ra_PS and iFlux_SG*10
    // In the WhereClause, this means that we have expressions that
    // require columns to evaluate.

    // When there is no WHERE clause that requires column reading,
    // the presence of a small-valued LIMIT should be enough to
    // de-classify a query as a scanning query.

    bool hasSelectColumnRef = false; // Requires row-reading for
                                     // results
    bool hasSelectStar = false; // Requires reading all columns
    bool hasSpatialSelect = false; // Recognized chunk restriction
    bool hasWhereColumnRef = false; // Makes count(*) non-trivial
    bool hasSecondaryKey = false; // Using secondaryKey to restrict
                                  // coverage, e.g., via objectId=123
                                  // or objectId IN (123,133) ?

    if(stmt.hasWhereClause()) {
        WhereClause& wc = stmt.getWhereClause();
        // Check WHERE for spatial select
        boost::shared_ptr<QsRestrictor::List const> restrs = wc.getRestrs();
        hasSpatialSelect = restrs && !restrs->empty();


        // Look for column refs
        boost::shared_ptr<ColumnRefMap::List const> crl = wc.getColumnRefs();
        if(crl) {
            hasWhereColumnRef = !crl->empty();
#if 0
            // FIXME: Detect secondary key reference by Qserv
            // restrictor detection, not by WHERE clause.
            // The qserv restrictor must be a condition on the
            // secondary key--spatial selects can still be part of
            // scans if they involve >k chunks.
            boost::shared_ptr<AndTerm> aterm = wc.getRootAndTerm();
            if(aterm) {
                // Look for secondary key matches
                typedef BoolTerm::PtrList PtrList;
                for(PtrList::iterator i = aterm->iterBegin();
                    i != aterm->iterEnd(); ++i) {
                    if(testIfSecondary(**i)) {
                        hasSecondaryKey = true;
                        break;
                    }
                }
            }
#endif
        }
    }
    SelectList& sList = stmt.getSelectList();
    boost::shared_ptr<ValueExprList> sVexpr = sList.getValueExprList();

    if(sVexpr) {
        ColumnRef::List cList; // For each expr, get column refs.

        typedef ValueExprList::const_iterator Iter;
        for(Iter i=sVexpr->begin(), e=sVexpr->end(); i != e; ++i) {
            (*i)->findColumnRefs(cList);
        }
        // Resolve column refs, see if they include partitioned
        // tables.
        typedef ColumnRef::List::const_iterator ColIter;
        for(ColIter i=cList.begin(), e=cList.end(); i != e; ++i) {
            // FIXME: Need to resolve and see if it's a partitioned table.
            hasSelectColumnRef = true;
        }
    }
    // FIXME hasSelectStar is not populated right now. Do we need it?

    StringPairList scanTables;
    // Right now, queries involving less than a threshold number of
    // chunks have their scanTables squashed as non-scanning in the
    // plugin's applyFinal
    if(hasSelectColumnRef || hasSelectStar) {
        if(hasSecondaryKey) {
            std::cout << "**** Not a scan ****" << std::endl;
            // Not a scan? Leave scanTables alone
        } else {
            std::cout << "**** SCAN (column ref, non-spatial-idx)****" << std::endl;
            // Scan tables = all partitioned tables
            scanTables = filterPartitioned(stmt.getFromList().getTableRefnList());
        }
    } else if(hasWhereColumnRef) {
        // No column ref in SELECT, still a scan for non-trivial WHERE
        // count(*): still a scan with a non-trivial where.
        std::cout << "**** SCAN (filter) ****" << std::endl;
        scanTables = filterPartitioned(stmt.getFromList().getTableRefnList());
    }
    return scanTables;
}

}}} // namespace lsst::qserv::master
