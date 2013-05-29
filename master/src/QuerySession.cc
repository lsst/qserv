/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
  * @file QueryMapping.cc
  *
  * @brief Implementation of the class QuerySession, which is a
  * container for input query state (and related state available prior
  * to execution). Also includes QuerySession::Iter and initQuerySession()
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/QuerySession.h"

#include <algorithm>
#include "lsst/qserv/master/PlanWriter.h"
#include "lsst/qserv/master/SelectParser.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/QueryMapping.h"
#include "lsst/qserv/master/QueryPlugin.h"
#include "lsst/qserv/master/AggregatePlugin.h"
#include "lsst/qserv/master/SpatialSpecPlugin.h"
#include "lsst/qserv/master/TablePlugin.h"
#include "lsst/qserv/master/PostPlugin.h"
#include "lsst/qserv/master/ifaceMeta.h" // Retrieve metadata object

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
struct printConstraintHelper {
    printConstraintHelper(std::ostream& os_) : os(os_) {}
    void operator()(qMaster::Constraint const& c) {
        os << "Constraint " << c.name << " ";
        std::copy(c.params.begin(), c.params.end(), 
                  std::ostream_iterator<std::string>(os, ","));
        os << "[" << c.params.size() << "]";
    }
    std::ostream& os;
};
void printConstraints(qMaster::ConstraintVector const& cv) {
    std::for_each(cv.begin(), cv.end(), printConstraintHelper(std::cout));
}

} // anonymous namespace

namespace lsst { namespace qserv  { namespace master {

////////////////////////////////////////////////////////////////////////
// class QuerySession
////////////////////////////////////////////////////////////////////////
QuerySession::QuerySession(int metaCacheSession) 
    : _metaCacheSession(metaCacheSession) {
} 

void QuerySession::setQuery(std::string const& q) {
    _original = q;
    _initContext();
    assert(_context.get());
    SelectParser::Ptr p;
    p = SelectParser::newInstance(q);
    p->setup();
    _stmt = p->getSelectStmt();
    _preparePlugins();
    _applyLogicPlugins();
    _generateConcrete();
    _applyConcretePlugins();
    _showFinal(); // DEBUG    
}

bool QuerySession::getHasAggregate() const {
    // Aggregate: having an aggregate fct spec in the select list.
    // Stmt itself knows whether aggregation is present. More
    // generally, aggregation is a separate pass. In computing a
    // multi-pass execution, the statement makes use of a (proper,
    // probably) subset of its components to compose each pass. Right
    // now, the only goal is to support aggregation using two passes.
    
    // FIXME
    return false;
}

boost::shared_ptr<ConstraintVector> QuerySession::getConstraints() const {
    boost::shared_ptr<ConstraintVector> cv;
    boost::shared_ptr<QsRestrictor::List const> p = _context->restrictors;

    if(p.get()) {
        cv.reset(new ConstraintVector(p->size()));
        int i=0;
        QsRestrictor::List::const_iterator li;
        for(li = p->begin(); li != p->end(); ++li) {
            Constraint c;
            QsRestrictor const& r = **li;
            c.name = r._name;
            StringList::const_iterator si;
            for(si = r._params.begin(); si != r._params.end(); ++si) {
                c.params.push_back(*si);
            }
            (*cv)[i] = c;
            ++i;
        }
        printConstraints(*cv);
        return cv;
    } else {
        std::cout << "No constraints." << std::endl;
    }
    // No constraint vector
    return cv;
}

void QuerySession::addChunk(qMaster::ChunkSpec const& cs) {
    _chunks.push_back(cs);
}


void QuerySession::setResultTable(std::string const& resultTable) {
    _resultTable = resultTable;
}

std::string const& QuerySession::getDominantDb() const {
    return _context->dominantDb; // parsed query's dominant db (via TablePlugin)
}

MergeFixup QuerySession::makeMergeFixup() const {
    // Make MergeFixup to adapt new query parser/generation framework
    // to older merging code. 
    assert(_stmt.get());
    SelectList const& mergeSelect = _stmtMerge->getSelectList();
    QueryTemplate t;
    mergeSelect.renderTo(t);
    std::string select = t.generate();
    t.clear();
    std::string post; // TODO: handle GroupBy, etc.
    std::string orderBy; // TODO
    bool needsMerge = _context->needsMerge;
    return MergeFixup(select, post, orderBy, 
                      _stmtMerge->getLimit(), needsMerge);
}

QuerySession::Iter QuerySession::cQueryBegin() {
    return Iter(*this, _chunks.begin());
}
QuerySession::Iter QuerySession::cQueryEnd() {
    return Iter(*this, _chunks.end());
}


void QuerySession::_initContext() {
    _context.reset(new QueryContext()); 
    _context->defaultDb = "LSST";
    _context->username = "default";
    _context->needsMerge = false;
    MetadataCache* metadata = getMetadataCache(_metaCacheSession).get();
    _context->metadata = metadata;
    assert(metadata);
}
void QuerySession::_preparePlugins() {
    _plugins.reset(new PluginList);

    _plugins->push_back(QueryPlugin::newInstance("Aggregate"));
    _plugins->push_back(QueryPlugin::newInstance("Table"));
    _plugins->push_back(QueryPlugin::newInstance("SpatialSpec"));
    _plugins->push_back(QueryPlugin::newInstance("Post"));
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).prepare();
    }
}
void QuerySession::_applyLogicPlugins() {
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyLogical(*_stmt, *_context);
    }
}
void QuerySession::_generateConcrete() {
    _hasMerge = false;
    // In making a statement concrete, the query's execution is split
    // into a parallel portion and a merging/aggregation portion. 
    // In many cases, not much needs to be done, since nearly all of
    // it can be parallelized.
    // If the query requires aggregation, the select list needs to get
    // converted into a parallel portion, and the merging includes the
    // post-parallel steps to merge sub-results.  When the statement
    // results in merely a collection of unordered concatenated rows,
    // the merge statement can be left empty, signifying that the sub
    // results can be concatenated directly into the output table.
    //
    // Important parts of the merge statement are 
    _stmtParallel = _stmt->copyDeep(); // Needs to copy SelectList, since the
                           // parallel statement's version will get
                           // updated by plugins. Plugins probably
                           // need access to the original as a
                           // reference.
    _stmtMerge = _stmt->copyMerge(); // Copies SelectList and Mods,
                                      // but not FROM, and perhaps not
                                      // WHERE(???)
    
    
    // Compute default merge predicate:
}


void QuerySession::_applyConcretePlugins() {
    QueryPlugin::Plan p(*_stmt, *_stmtParallel, *_stmtMerge, _hasMerge);
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyPhysical(p, *_context);
    }
}


/// Some code useful for debugging. 
void QuerySession::_showFinal() {
    // Print out the end result.
    QueryTemplate par = _stmtParallel->getTemplate();
    QueryTemplate mer = _stmtMerge->getTemplate();
    
    std::cout << "parallel: " << par.dbgStr() << std::endl;
    std::cout << "merge: " << mer.dbgStr() << std::endl;
}

std::vector<std::string> QuerySession::_buildChunkQueries(ChunkSpec const& s) { 
    std::vector<std::string> q;
    // This logic may be pushed over to the qserv worker in the future.
    assert(_stmtParallel.get());
    QueryTemplate cqTemp = _stmtParallel->getTemplate();
    assert(_context->queryMapping.get());
    QueryMapping const& queryMapping = *_context->queryMapping;
    if(!queryMapping.hasSubChunks()) { // Non-subchunked?
        q.push_back(_context->queryMapping->apply(s, cqTemp));
    } else { // subchunked:
        ChunkSpecSingle::List sList = ChunkSpecSingle::makeList(s);
        ChunkSpecSingle::List::const_iterator i;
        for(i=sList.begin(); i != sList.end(); ++i) {
            q.push_back(_context->queryMapping->apply(*i, cqTemp));
        }
    }
    return q;
}

////////////////////////////////////////////////////////////////////////
// QuerySession::Iter
////////////////////////////////////////////////////////////////////////
QuerySession::Iter::Iter(QuerySession& qs, ChunkSpecList::iterator i)
    : _qs(&qs), _pos(i), _dirty(true) {
    assert(qs._context.get());
    _hasChunks = qs._context->hasChunks();
    _hasSubChunks = qs._context->hasSubChunks();
}

ChunkQuerySpec& QuerySession::Iter::dereference() const {
    if(_dirty) { _updateCache(); }
    return _cache;
}

void QuerySession::Iter::_buildCache() const {
    assert(_qs != NULL);
    _cache.db = _qs->_context->defaultDb;
    _cache.queries = _qs->_buildChunkQueries(*_pos);
    _cache.chunkId = _pos->chunkId;
    _cache.nextFragment.reset();
    _cache.subChunkTables.clear();
    QueryMapping const& queryMapping = *(_qs->_context->queryMapping);
    QueryMapping::StringSet const& sTables = queryMapping.getSubChunkTables();
    _cache.subChunkTables.insert(_cache.subChunkTables.begin(),
                                 sTables.begin(), sTables.end());    
    if(_hasSubChunks) {
        if(_pos->shouldSplit()) {
            ChunkSpecFragmenter frag(*_pos);
            ChunkSpec s = frag.get();
            _cache.queries = _qs->_buildChunkQueries(s);
            _cache.subChunkIds.assign(s.subChunks.begin(), s.subChunks.end());
            frag.next();
            _cache.nextFragment = _buildFragment(frag);                
        } else {
            _cache.subChunkIds.assign(_pos->subChunks.begin(), 
                                      _pos->subChunks.end());
        }
    }
}
boost::shared_ptr<ChunkQuerySpec> 
QuerySession::Iter::_buildFragment(ChunkSpecFragmenter& f) const {
    boost::shared_ptr<ChunkQuerySpec> first;
    boost::shared_ptr<ChunkQuerySpec> last;
    while(!f.isDone()) {
        if(last.get()) {
            last->nextFragment.reset(new ChunkQuerySpec);
            last = last->nextFragment;
        } else {
            last.reset(new ChunkQuerySpec);
            first = last;
        }
        ChunkSpec s = f.get();
        last->subChunkIds.assign(s.subChunks.begin(), s.subChunks.end());
        last->queries = _qs->_buildChunkQueries(s);
        f.next();
    }
    return first;
}


////////////////////////////////////////////////////////////////////////
// initQuerySession
////////////////////////////////////////////////////////////////////////
void initQuerySession() {
    // Plugins should probably be registered once, at startup.
    registerAggregatePlugin(); 
    registerTablePlugin();
    registerSpatialSpecPlugin();
    registerPostPlugin();
}

}}} // namespace lsst::qserv::master
