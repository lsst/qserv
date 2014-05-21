// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
  * @brief Implementation of the class QuerySession, which is a
  * container for input query state (and related state available prior
  * to execution). Also includes QuerySession::Iter and initQuerySession()
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/QuerySession.h"

// System headers
#include <algorithm>
#include <iostream>
#include <stdexcept>

// Third-party headers
#include <antlr/NoViableAltException.hpp>

// Local headers
#include "css/Facade.h"
#include "global/constants.h"
#include "log/Logger.h"
#include "parser/ParseException.h"
#include "parser/parseExceptions.h"
#include "parser/SelectParser.h"
#include "qana/AnalysisError.h"
#include "qana/QueryMapping.h"
#include "qana/QueryPlugin.h"
#include "query/Constraint.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/SelectList.h"

#define DEBUG 0

namespace lsst {
namespace qserv {
namespace qproc {

void printConstraints(query::ConstraintVector const& cv) {
    std::copy(cv.begin(), cv.end(),
              std::ostream_iterator<query::Constraint>(LOG_STRM(Info), ","));
}

////////////////////////////////////////////////////////////////////////
// class QuerySession
////////////////////////////////////////////////////////////////////////
QuerySession::QuerySession(boost::shared_ptr<css::Facade> cssFacade) :
    _cssFacade(cssFacade) {
}

void QuerySession::setDefaultDb(std::string const& defaultDb) {
    _defaultDb = defaultDb;
}

void QuerySession::setQuery(std::string const& inputQuery) {
    _original = inputQuery;
    _isFinal = false;
    _initContext();
    assert(_context.get());

    parser::SelectParser::Ptr p;
    try {
        p = parser::SelectParser::newInstance(inputQuery);
        p->setup();
        _stmt = p->getSelectStmt();
        _preparePlugins();
        _applyLogicPlugins();
        _generateConcrete();
        _applyConcretePlugins();
        //_showFinal(std::cout); // DEBUG
    } catch(qana::AnalysisError& e) {
        _error = std::string("AnalysisError:") + e.what();
    } catch(parser::ParseException& e) {
        _error = std::string("ParseException:") + e.what();
    } catch(antlr::NoViableAltException& e) {
        _error = std::string("ANTLR exception:") + e.getMessage();
    } catch(parser::UnknownAntlrError& e) {
        _error = e.what();
    }
}

bool QuerySession::hasAggregate() const {
    // Aggregate: having an aggregate fct spec in the select list.
    // Stmt itself knows whether aggregation is present. More
    // generally, aggregation is a separate pass. In computing a
    // multi-pass execution, the statement makes use of a (proper,
    // probably) subset of its components to compose each pass. Right
    // now, the only goal is to support aggregation using two passes.
    return _context->needsMerge;
}
bool QuerySession::hasChunks() const {
    return _context->hasChunks();
}

boost::shared_ptr<query::ConstraintVector> QuerySession::getConstraints() const {
    boost::shared_ptr<query::ConstraintVector> cv;
    boost::shared_ptr<query::QsRestrictor::List const> p = _context->restrictors;

    if(p.get()) {
        cv.reset(new query::ConstraintVector(p->size()));
        int i=0;
        query::QsRestrictor::List::const_iterator li;
        for(li = p->begin(); li != p->end(); ++li) {
            query::Constraint c;
            query::QsRestrictor const& r = **li;
            c.name = r._name;
            util::StringList::const_iterator si;
            for(si = r._params.begin(); si != r._params.end(); ++si) {
                c.params.push_back(*si);
            }
            (*cv)[i] = c;
            ++i;
        }
        //printConstraints(*cv);
        return cv;
    } else {
        //LOGGER_INF << "No constraints." << std::endl;
    }
    // No constraint vector
    return cv;
}

void QuerySession::addChunk(ChunkSpec const& cs) {
    _context->chunkCount += 1;
    _chunks.push_back(cs);
}

void QuerySession::setResultTable(std::string const& resultTable) {
    _resultTable = resultTable;
}

std::string const& QuerySession::getDominantDb() const {
    return _context->dominantDb; // parsed query's dominant db (via TablePlugin)
}

bool QuerySession::containsDb(std::string const& dbName) const {
    return _context->containsDb(dbName);
}

css::StripingParams
QuerySession::getDbStriping() {
    return _context->getDbStriping();
}

rproc::MergeFixup
QuerySession::makeMergeFixup() const {
    // Make MergeFixup to adapt new query parser/generation framework
    // to older merging code.
    if(!_stmt) {
        throw std::invalid_argument("Cannot makeMergeFixup() with NULL _stmt");
    }
    query::SelectList const& mergeSelect = _stmtMerge->getSelectList();
    query::QueryTemplate t;
    mergeSelect.renderTo(t);
    std::string select = t.generate();
    t = _stmtMerge->getPostTemplate();
    std::string post = t.generate();
    std::string orderBy; // TODO
    bool needsMerge = _context->needsMerge;
    return rproc::MergeFixup(select, post, orderBy,
                              _stmtMerge->getLimit(), needsMerge);
}

void QuerySession::finalize() {
    if(_isFinal) {
        return;
    }
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyFinal(*_context);
    }
    // Make up for no chunks (chunk-less query): add the dummy chunk.
    if(_chunks.empty()) {
        ChunkSpec cs;
        cs.chunkId = DUMMY_CHUNK;
        addChunk(cs);
    }
}

QuerySession::Iter QuerySession::cQueryBegin() {
    return Iter(*this, _chunks.begin());
}
QuerySession::Iter QuerySession::cQueryEnd() {
    return Iter(*this, _chunks.end());
}
QuerySession::QuerySession(Test& t)
    : _cssFacade(t.cssFacade), _defaultDb(t.defaultDb) {
    _initContext();
}

void QuerySession::_initContext() {
    _context.reset(new query::QueryContext());
    _context->defaultDb = _defaultDb;
    _context->username = "default";
    _context->needsMerge = false;
    _context->chunkCount = 0;
    _context->cssFacade = _cssFacade;
}

void QuerySession::_preparePlugins() {
    _plugins.reset(new PluginList);

    _plugins->push_back(qana::QueryPlugin::newInstance("Where"));
    _plugins->push_back(qana::QueryPlugin::newInstance("Aggregate"));
    _plugins->push_back(qana::QueryPlugin::newInstance("Table"));
    _plugins->push_back(qana::QueryPlugin::newInstance("QservRestrictor"));
    _plugins->push_back(qana::QueryPlugin::newInstance("Post"));
    _plugins->push_back(qana::QueryPlugin::newInstance("ScanTable"));
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
    // into a parallel portion and a merging/aggregation portion.  In
    // many cases, not much needs to be done for the latter, since
    // nearly all of the query can be parallelized.
    // If the query requires aggregation, the select list needs to get
    // converted into a parallel portion, and the merging includes the
    // post-parallel steps to merge sub-results.  When the statement
    // results in merely a collection of unordered concatenated rows,
    // the merge statement can be left empty, signifying that the sub
    // results can be concatenated directly into the output table.
    //

    // Needs to copy SelectList, since the parallel statement's
    // version will get updated by plugins. Plugins probably need
    // access to the original as a reference.
    _stmtParallel.push_back(_stmt->clone());

    // Copy SelectList and Mods, but not FROM, and perhaps not
    // WHERE(???). Conceptually, we want to copy the parts that are
    // needed during merging and aggregation.
    _stmtMerge = _stmt->copyMerge();

    // TableMerger needs to be integrated into this design.
}


void QuerySession::_applyConcretePlugins() {
    qana::QueryPlugin::Plan p(*_stmt, _stmtParallel, *_stmtMerge, _hasMerge);
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyPhysical(p, *_context);
    }
}

/// Some code useful for debugging.
void QuerySession::_showFinal(std::ostream& os) {
    // Print out the end result.
    query::QueryTemplate par = _stmtParallel.front()->getTemplate();
    query::QueryTemplate mer = _stmtMerge->getTemplate();

    os << "QuerySession::_showFinal() : parallel: " << par.dbgStr() << std::endl;
    os << "QuerySession::_showFinal() : merge: " << mer.dbgStr() << std::endl;
    if(!_context->scanTables.empty()) {
        util::StringPairList::const_iterator i,e;
        for(i=_context->scanTables.begin(), e=_context->scanTables.end();
            i != e; ++i) {
            os << "ScanTable: " << i->first << "." << i->second
                       << std::endl;
        }
    }
}

std::vector<std::string> QuerySession::_buildChunkQueries(ChunkSpec const& s) const {
    std::vector<std::string> q;
    // This logic may be pushed over to the qserv worker in the future.
    if(_stmtParallel.empty() || !_stmtParallel.front()) {
        throw std::logic_error("Attempted buildChunkQueries without _stmtParallel");
    }

    if(!_context->queryMapping) {
        throw std::logic_error("Missing QueryMapping in _context");
    }
    qana::QueryMapping const& queryMapping = *_context->queryMapping;

    typedef std::list<boost::shared_ptr<query::SelectStmt> >::const_iterator Iter;
    typedef std::list<query::QueryTemplate> Tlist;
    typedef Tlist::const_iterator TlistIter;
    Tlist tlist;

    for(Iter i=_stmtParallel.begin(), e=_stmtParallel.end();
        i != e; ++i) {
        tlist.push_back((**i).getTemplate());
    }
    if(!queryMapping.hasSubChunks()) { // Non-subchunked?
        LOGGER_INF << "QuerySession::_buildChunkQueries() : Non-subchunked" << std::endl;
        for(TlistIter i=tlist.begin(), e=tlist.end(); i != e; ++i) {
            q.push_back(_context->queryMapping->apply(s, *i));
        }
    } else { // subchunked:
        LOGGER_INF << "QuerySession::_buildChunkQueries() : subchunked " << std::endl;
        ChunkSpecSingle::List sList = ChunkSpecSingle::makeList(s);

        LOGGER_DBG << "QuerySession::_buildChunkQueries() : subchunks :";
        std::copy(sList.begin(), sList.end(),
            std::ostream_iterator<ChunkSpecSingle>(LOG_STRM(Debug), ","));
        LOGGER_DBG << std::endl;
        typedef ChunkSpecSingle::List::const_iterator ChunkIter;
        for(ChunkIter i=sList.begin(), e=sList.end(); i != e; ++i) {
            for(TlistIter j=tlist.begin(), je=tlist.end(); j != je; ++j) {

	      LOGGER_DBG << "QuerySession::_buildChunkQueries() : adding query "
                         << _context->queryMapping->apply(*i, *j) << std::endl;
              q.push_back(_context->queryMapping->apply(*i, *j));
            }
        }
    }

    LOGGER_DBG << "QuerySession::_buildChunkQueries() : returning  queries : " << std::endl;
    for(unsigned int t=0;t<q.size();t++){
        LOGGER_DBG << q.at(t) << std::endl;
    }

    return q;
}

////////////////////////////////////////////////////////////////////////
// QuerySession::Iter
////////////////////////////////////////////////////////////////////////
QuerySession::Iter::Iter(QuerySession& qs, ChunkSpecList::iterator i)
    : _qs(&qs), _pos(i), _dirty(true) {
    if(!qs._context) {
        throw std::invalid_argument("NULL QuerySession");
    }
    _hasChunks = qs._context->hasChunks();
    _hasSubChunks = qs._context->hasSubChunks();
}

ChunkQuerySpec& QuerySession::Iter::dereference() const {
    if(_dirty) { _updateCache(); }
    return _cache;
}

void QuerySession::Iter::_buildCache() const {
    assert(_qs != NULL);
    _cache.db = _qs->_context->dominantDb;
    // LOGGER_INF << "scantables "
    //            << (_qs->_context->scanTables.empty() ? "is " : "is not ")
    //            << " empty" << std::endl;

    _cache.scanTables = _qs->_context->scanTables;
    _cache.chunkId = _pos->chunkId;
    _cache.nextFragment.reset();
    // Reset subChunkTables
    _cache.subChunkTables.clear();
    qana::QueryMapping const& queryMapping = *(_qs->_context->queryMapping);
    qana::QueryMapping::StringSet const& sTables = queryMapping.getSubChunkTables();
    _cache.subChunkTables.insert(_cache.subChunkTables.begin(),
                                 sTables.begin(), sTables.end());
    // Build queries.
    if(!_hasSubChunks) {
        _cache.queries = _qs->_buildChunkQueries(*_pos);
    } else {
        if(_pos->shouldSplit()) {
            ChunkSpecFragmenter frag(*_pos);
            ChunkSpec s = frag.get();
            _cache.queries = _qs->_buildChunkQueries(s);
            _cache.subChunkIds.assign(s.subChunks.begin(), s.subChunks.end());
            frag.next();
            _cache.nextFragment = _buildFragment(frag);
        } else {
            _cache.queries = _qs->_buildChunkQueries(*_pos);
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

}}} // namespace lsst::qserv::qproc
