// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QPROC_QUERYSESSION_H
#define LSST_QSERV_QPROC_QUERYSESSION_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <string>
#include <vector>

// Third-party headers
#include "boost/iterator/iterator_facade.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/Facade.h"
#include "global/intTypes.h"
#include "qana/QueryPlugin.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"
#include "query/typedefs.h"
#include "rproc/mergeTypes.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace css {
    class StripingParams;
}
namespace query {
    class SelectStmt;
    class QueryContext;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qproc {

///  QuerySession contains state and behavior for operating on user queries. It
///  contains much of the query analysis-side responsibility, including the text
///  of the original query, a parsed query  tree, and other user state/context.
class QuerySession {
public:
    class Iter;
    friend class Iter;
    typedef std::shared_ptr<QuerySession> Ptr;

    explicit QuerySession(std::shared_ptr<css::Facade>);

    std::string const& getOriginal() const { return _original; }
    void setDefaultDb(std::string const& db);
    void setQuery(std::string const& q);
    bool needsMerge() const;
    bool hasChunks() const;

    std::shared_ptr<query::ConstraintVector> getConstraints() const;
    void addChunk(ChunkSpec const& cs);
    void addChunk(ChunkSpecVector const& cs);
    void setDummy();

    query::SelectStmt const& getStmt() const { return *_stmt; }

    // Resulttable concept will be obsolete after we implement query result
    // marshalling/transfer (at which point, table dump and restore will also be
    // obsolete).
    void setResultTable(std::string const& resultTable);
    std::string const& getResultTable() const { return _resultTable; }

    /// Dominant database is the database that will be used for query
    /// dispatch. This is distinct from the default database, which is what is
    /// used for unqualified table and column references
    std::string const& getDominantDb() const;
    bool containsDb(std::string const& dbName) const;
    bool validateDominantDb() const;
    css::StripingParams getDbStriping();
    std::shared_ptr<IntSet const> getEmptyChunks();
    std::string const& getError() const { return _error; }

    rproc::MergeFixup makeMergeFixup() const; ///< as obsolete as TableMerger
    std::shared_ptr<query::SelectStmt> getMergeStmt() const;

    /// Finalize a query after chunk coverage has been updated
    void finalize();
    // Iteration
    Iter cQueryBegin();
    Iter cQueryEnd();

    // For test harnesses.
    struct Test {
        int cfgNum;
        std::shared_ptr<css::Facade> cssFacade;
        std::string defaultDb;
    };
    explicit QuerySession(Test& t); ///< Debug constructor
    std::shared_ptr<query::QueryContext> dbgGetContext() { return _context; }

private:
    typedef std::vector<qana::QueryPlugin::Ptr> QueryPluginPtrVector;

    // Pipeline helpers
    void _initContext();
    void _preparePlugins();
    void _applyLogicPlugins();
    void _generateConcrete();
    void _applyConcretePlugins();
    std::string toString(); // Debug

    // Iterator help
    std::vector<std::string> _buildChunkQueries(ChunkSpec const& s) const;

    // Fields
    std::shared_ptr<css::Facade> _cssFacade; ///< Metadata access facade
    std::string _defaultDb; ///< User db context
    std::string _original; ///< Original user query
    std::shared_ptr<query::QueryContext> _context; ///< Analysis context
    std::shared_ptr<query::SelectStmt> _stmt; ///< Logical query statement

    /// Group of parallel statements (not a sequence)
    /**
    * Store the template used to generate queries on the workers
    * Example:
    *    - input user query:
    *    select sum(pm_declErr), chunkId as f1, chunkId AS f1, avg(pm_declErr)
    *    from LSST.Object where bMagF > 20.0 GROUP BY chunkId;
    *    - template for worker queries:
    *    SELECT sum(pm_declErr) AS QS1_SUM,chunkId AS f1,chunkId AS f1,
    *    COUNT(pm_declErr) AS QS2_COUNT,SUM(pm_declErr) AS QS3_SUM
    *    FROM LSST.Object_%CC% AS QST_1_ WHERE bMagF>20.0 GROUP BY chunkId
    *
    */
    query::SelectStmtPtrVector _stmtParallel;

    /**
    * Store the query used to aggregate results on the czar.
    * Aggregation is optional, so this variable may be empty
    * It will run against a table named: result_ID_m, where ID is an integer
    * Example:
    *    - input user query:
    *    select sum(pm_declErr), chunkId as f1, chunkId AS f1, avg(pm_declErr)
    *    from LSST.Object where bMagF > 20.0 GROUP BY chunkId;
    *    - merge query:
    *    SELECT SUM(QS1_SUM),f1 AS f1,f1 AS f1,(SUM(QS3_SUM)/SUM(QS2_COUNT))
    *    GROUP BY chunkId
    *
    */
    query::SelectStmtPtr _stmtMerge;
    bool _hasMerge;
    bool _isDummy; ///< Use dummy chunk, disabling subchunks or any real chunks
    std::string _tmpTable;
    std::string _resultTable;
    std::string _error;
    int _isFinal; ///< Has query analysis/optimization completed?

    ChunkSpecVector _chunks; ///< Chunk coverage
    std::shared_ptr<QueryPluginPtrVector> _plugins; ///< Analysis plugin chain

    static LOG_LOGGER _logger;
};

/// Iterates over a ChunkSpecList to return ChunkQuerySpecs for execution
class QuerySession::Iter : public boost::iterator_facade <
    QuerySession::Iter, ChunkQuerySpec, boost::forward_traversal_tag> {
public:
    Iter() : _qs(NULL) {}

private:
    Iter(QuerySession& qs, ChunkSpecVector::iterator i);
    friend class QuerySession;
    friend class boost::iterator_core_access;

    void increment() { ++_pos; _dirty = true; }

    bool equal(Iter const& other) const {
        return (this->_qs == other._qs) && (this->_pos == other._pos);
    }

    ChunkQuerySpec& dereference() const;

    void _buildCache() const;
    void _updateCache() const {
        if(_dirty) {
            _buildCache();
            _dirty = false;
        }
    }
    std::shared_ptr<ChunkQuerySpec> _buildFragment(ChunkSpecFragmenter& f) const;

    QuerySession* _qs;
    ChunkSpecVector::const_iterator _pos;
    bool _hasChunks;
    bool _hasSubChunks;
    mutable ChunkQuerySpec _cache; ///< Query generation cache
    mutable bool _dirty; ///< Does cache need updating/refreshing?
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_QUERYSESSION_H
