// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 AURA/LSST.
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

// Class header
#include "qproc/QuerySession.h"

// System headers
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include <antlr/NoViableAltException.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "global/constants.h"
#include "global/stringTypes.h"
#include "parser/ParseException.h"
#include "parser/parseExceptions.h"
#include "parser/SelectParser.h"
#include "qana/AggregatePlugin.h"
#include "qana/AnalysisError.h"
#include "qana/DuplSelectExprPlugin.h"
#include "qana/MatchTablePlugin.h"
#include "qana/PostPlugin.h"
#include "qana/QservRestrictorPlugin.h"
#include "qana/QueryMapping.h"
#include "qana/QueryPlugin.h"
#include "qana/ScanTablePlugin.h"
#include "qana/TablePlugin.h"
#include "qana/WherePlugin.h"
#include "qproc/QueryProcessingBug.h"
#include "query/Constraint.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/SelectList.h"
#include "query/typedefs.h"
#include "util/IterableFormatter.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.QuerySession");
}

namespace lsst {
namespace qserv {
namespace qproc {

////////////////////////////////////////////////////////////////////////
// class QuerySession
////////////////////////////////////////////////////////////////////////
void QuerySession::setDefaultDb(std::string const& defaultDb) {
    _defaultDb = defaultDb;
}

// Analyze SQL query issued by user
void QuerySession::analyzeQuery(std::string const& sql) {
    _original = sql;
    _isFinal = false;
    _initContext();
    assert(_context.get());

    parser::SelectParser::Ptr p;
    try {
        p = parser::SelectParser::newInstance(sql);
        p->setup();
        _stmt = p->getSelectStmt();
        _preparePlugins();
        _applyLogicPlugins();
        _generateConcrete();
        _applyConcretePlugins();

        LOGS(_log, LOG_LVL_DEBUG, "Query Plugins applied:\n " << *this);
        LOGS(_log, LOG_LVL_TRACE, "ORDER BY clause for mysql-proxy: " << getProxyOrderBy());

    } catch(QueryProcessingBug& b) {
        _error = std::string("QuerySession bug:") + b.what();
    } catch(qana::AnalysisError& e) {
        _error = std::string("AnalysisError:") + e.what();
    } catch(css::NoSuchDb& e) {
        _error = std::string("NoSuchDb:") + e.what();
    } catch(css::NoSuchTable& e) {
        _error = std::string("NoSuchTable:") + e.what();
    } catch(parser::ParseException& e) {
        _error = std::string("ParseException:") + e.what();
    } catch(antlr::NoViableAltException& e) {
        _error = std::string("ANTLR exception:") + e.getMessage();
    } catch(parser::UnknownAntlrError& e) {
        _error = e.what();
    } catch(Bug& b) {
        _error = std::string("Qserv bug:") + b.what();
    } catch(std::exception const& e) {
        _error = std::string("analyzeQuery unexpected:") + e.what();
    }
}

bool QuerySession::needsMerge() const {
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

std::shared_ptr<query::ConstraintVector> QuerySession::getConstraints() const {
    std::shared_ptr<query::ConstraintVector> cv;
    std::shared_ptr<query::QsRestrictor::PtrVector const> p = _context->restrictors;

    if (p.get()) {
        cv = std::make_shared<query::ConstraintVector>(p->size());
        LOGS(_log, LOG_LVL_TRACE, "Size of query::QsRestrictor::PtrVector: " << p->size());
        int i=0;
        query::QsRestrictor::PtrVector::const_iterator li;
        for(li = p->begin(); li != p->end(); ++li) {
            query::Constraint c;
            query::QsRestrictor const& r = **li;
            c.name = r._name;
            StringVector::const_iterator si;
            for(si = r._params.begin(); si != r._params.end(); ++si) {
                c.params.push_back(*si);
            }
            (*cv)[i] = c;
            ++i;
        }
        LOGS(_log, LOG_LVL_TRACE, "Constraints: " << util::printable(*cv));
    } else {
        LOGS(_log, LOG_LVL_TRACE, "No constraints.");
    }
    return cv;
}

// return the ORDER BY clause to run on mysql-proxy at result retrieval
std::string QuerySession::getProxyOrderBy() const {
    std::string orderBy;
    if (_stmt->hasOrderBy()) {
        orderBy = _stmt->getOrderBy().sqlFragment();
    }
    return orderBy;
}

void QuerySession::addChunk(ChunkSpec const& cs) {
    LOGS(_log, LOG_LVL_TRACE, "Add chunk: " << cs);
    _context->chunkCount += 1;
    _chunks.push_back(cs);
}


void QuerySession::setScanInteractive() {
    // Default is for interactive scan.
    if (_context->chunkCount > _interactiveChunkLimit) {
        _scanInteractive = false;
    }
}

void QuerySession::setDummy() {
    _isDummy = true;
    // Clear out chunk counts and _chunks, and replace with dummy chunk.
    _context->chunkCount = 1;
    _chunks.clear();
    IntVector v;
    v.push_back(1); // Dummy subchunk
    _chunks.push_back(ChunkSpec(DUMMY_CHUNK, v));
}

std::string const& QuerySession::getDominantDb() const {
    return _context->dominantDb; // parsed query's dominant db (via TablePlugin)
}

bool QuerySession::containsDb(std::string const& dbName) const {
    return _context->containsDb(dbName);
}

bool QuerySession::containsTable(std::string const& dbName, std::string const& tableName) const {
    return _context->containsTable(dbName, tableName);
}

bool QuerySession::validateDominantDb() const {
    return _context->containsDb(_context->dominantDb);
}

css::StripingParams
QuerySession::getDbStriping() {
    return _context->getDbStriping();
}

std::shared_ptr<IntSet const>
QuerySession::getEmptyChunks() {
    // FIXME: do we need to catch an exception here?
    return _css->getEmptyChunks().getEmpty(_context->dominantDb);
}

/// Returns the merge statment, if appropriate.
/// If a post-execution merge fixup is not needed, return a NULL pointer.
std::shared_ptr<query::SelectStmt>
QuerySession::getMergeStmt() const {
    if (_context->needsMerge) {
        return _stmtMerge;
    } else {
        return std::shared_ptr<query::SelectStmt>();
    }
}

void QuerySession::finalize() {
    if (_isFinal) {
        return;
    }
    QueryPluginPtrVector::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyFinal(*_context);
    }
    // Make up for no chunks (chunk-less query): add the dummy chunk.
    if (_chunks.empty()) {
        setDummy();
    }
}


QuerySession::QuerySession(Test& t)
    : _css(t.css), _defaultDb(t.defaultDb) {
    _initContext();
}


void QuerySession::_initContext() {
    _context = std::make_shared<query::QueryContext>(_defaultDb, _css, _mysqlSchemaConfig);
}


void QuerySession::_preparePlugins() {
    _plugins = std::make_shared<QueryPluginPtrVector>();

    _plugins->push_back(std::make_shared<qana::DuplSelectExprPlugin>());
    _plugins->push_back(std::make_shared<qana::WherePlugin>());
    _plugins->push_back(std::make_shared<qana::AggregatePlugin>());
    _plugins->push_back(std::make_shared<qana::TablePlugin>());
    _plugins->push_back(std::make_shared<qana::MatchTablePlugin>());
    _plugins->push_back(std::make_shared<qana::QservRestrictorPlugin>());
    _plugins->push_back(std::make_shared<qana::PostPlugin>());
    _plugins->push_back(std::make_shared<qana::ScanTablePlugin>());

    QueryPluginPtrVector::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).prepare();
    }
}

void QuerySession::_applyLogicPlugins() {
    QueryPluginPtrVector::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyLogical(*_stmt, *_context);
    }
}

void QuerySession::_generateConcrete() {
    _hasMerge = false;
    _isDummy = false;
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
    LOGS(_log, LOG_LVL_TRACE, "Merge statement initialized with: \""
         << _stmtMerge->getQueryTemplate() << "\"");

    // TableMerger needs to be integrated into this design.
}

void QuerySession::_applyConcretePlugins() {
    qana::QueryPlugin::Plan p(*_stmt, _stmtParallel, *_stmtMerge, _hasMerge);
    QueryPluginPtrVector::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyPhysical(p, *_context);
    }
}

/// Some code useful for debugging.
void QuerySession::print(std::ostream& os) const {
    query::QueryTemplate par = _stmtParallel.front()->getQueryTemplate();
    query::QueryTemplate mer = _stmtMerge->getQueryTemplate();
    os << "QuerySession description:\n";
    os << "  original: " << this->_original << "\n";
    os << "  has chunks: " << this->hasChunks() << "\n";
    os << "  chunks: " << util::printable(this->_chunks) << "\n";
    os << "  needs merge: " << this->needsMerge() << "\n";
    os << "  1st parallel statement: " << par << "\n";
    os << "  merge statement: " << mer << "\n";
    os << "  scanRating:" << _context->scanInfo.scanRating;
    for (auto const& tbl : _context->scanInfo.infoTables) {
        os << "  ScanTable: " << tbl.db << "." << tbl.table
           << " lock=" << tbl.lockInMemory << " rating=" << tbl.scanRating << "\n";
    }
}


std::vector<query::QueryTemplate> QuerySession::makeQueryTemplates() {
    std::vector<query::QueryTemplate> queryTemplates;
    for(auto stmtIter=_stmtParallel.begin(), e=_stmtParallel.end(); stmtIter != e; ++stmtIter) {
        queryTemplates.push_back((*stmtIter)->getQueryTemplate());
    }
    return queryTemplates;
}


std::vector<std::string> QuerySession::_buildChunkQueries(query::QueryTemplate::Vect const& queryTemplates,
                                                          ChunkSpec const& chunkSpec) const {
    std::vector<std::string> chunkQueries;
    // This logic may be pushed over to the qserv worker in the future.
    if (_stmtParallel.empty() || !_stmtParallel.front()) {
        throw QueryProcessingBug("Attempted buildChunkQueries without _stmtParallel");
    }

    if (!_context->queryMapping) {
        throw QueryProcessingBug("Missing QueryMapping in _context");
    }
    qana::QueryMapping const& queryMapping = *_context->queryMapping;

    if (!queryMapping.hasSubChunks()) { // Non-subchunked
        for(auto tupleIter=queryTemplates.begin(), e=queryTemplates.end(); tupleIter != e; ++tupleIter) {
            std::string str = _context->queryMapping->apply(chunkSpec, *tupleIter);
            chunkQueries.push_back(str);
        }
    } else { // subchunked:
        ChunkSpecSingle::Vector sVector = ChunkSpecSingle::makeVector(chunkSpec);
        for(auto& chunkStr : sVector) {
            for(auto& qTemplate : queryTemplates) {
                std::string str = _context->queryMapping->apply(chunkStr, qTemplate);
                LOGS(_log, LOG_LVL_DEBUG, "adding query " << str);
                chunkQueries.push_back(str);
            }
        }
    }
    return chunkQueries;
}


std::ostream& operator<<(std::ostream& out, QuerySession const& querySession) {
    querySession.print(out);
    return out;
}


ChunkQuerySpec QuerySession::buildChunkQuerySpec(query::QueryTemplate::Vect const& queryTemplates,
                                                 ChunkSpec const& chunkSpec) const {
    ChunkQuerySpec cQSpec;
    cQSpec.db = _context->dominantDb;
    cQSpec.scanInfo = _context->scanInfo;
    cQSpec.scanInteractive = _scanInteractive;
    cQSpec.chunkId = chunkSpec.chunkId;
    // Reset subChunkTables
    qana::QueryMapping const& queryMapping = *(_context->queryMapping);
    qana::QueryMapping::StringSet const& sTables = queryMapping.getSubChunkTables();
    cQSpec.subChunkTables.insert(cQSpec.subChunkTables.begin(),
                                 sTables.begin(), sTables.end());
    // Build queries.
    if (!_context->hasSubChunks()) {
        cQSpec.queries = _buildChunkQueries(queryTemplates, chunkSpec);
    } else {
        if (chunkSpec.shouldSplit()) {
            ChunkSpecFragmenter frag(chunkSpec);
            ChunkSpec s = frag.get();
            cQSpec.queries = _buildChunkQueries(queryTemplates, s);
            cQSpec.subChunkIds.assign(s.subChunks.begin(), s.subChunks.end());
            frag.next();
            cQSpec.nextFragment = _buildFragment(queryTemplates, frag);
        } else {
            cQSpec.queries = _buildChunkQueries(queryTemplates, chunkSpec);
            cQSpec.subChunkIds.assign(chunkSpec.subChunks.begin(),
                                      chunkSpec.subChunks.end());
        }
    }
    return cQSpec;
}


std::shared_ptr<ChunkQuerySpec>
QuerySession::_buildFragment(query::QueryTemplate::Vect const& queryTemplates,
                             ChunkSpecFragmenter& f) const {
    std::shared_ptr<ChunkQuerySpec> first;
    std::shared_ptr<ChunkQuerySpec> last;
    while(!f.isDone()) {
        if (last.get()) {
            last->nextFragment = std::make_shared<ChunkQuerySpec>();
            last = last->nextFragment;
        } else {
            last = std::make_shared<ChunkQuerySpec>();
            first = last;
        }
        ChunkSpec s = f.get();
        last->subChunkIds.assign(s.subChunks.begin(), s.subChunks.end());
        last->queries = _buildChunkQueries(queryTemplates, s);
        f.next();
    }
    return first;
}

}}} // namespace lsst::qserv::qproc
