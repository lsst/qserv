// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_QUERYMAPPING_H
#define LSST_QSERV_QANA_QUERYMAPPING_H
/**
 * @file
 *
 * @author Daniel L. Wang, SLAC
 */

// System headers
#include <map>
#include <memory>
#include <set>
#include <string>

// Qserv headers
#include "global/DbTable.h"

// Forward declarations
namespace lsst::qserv {
namespace query {
class QueryTemplate;
}
namespace qproc {
struct ChunkSpec;
class ChunkSpecSingle;
}  // namespace qproc
}  // namespace lsst::qserv

namespace lsst::qserv::qana {

/// QueryMapping is a value class that stores a mapping that can be
/// consulted for a partitioning-strategy-agnostic query generation
/// stage that substitutes real table names for placeholders, according
/// to a query's specified partition coverage.
///
/// This class helps abstract the concept of mapping partitioned table name
/// templates to concrete table names. Name templates will use a text markup to
/// specify where chunk numbers should be substituted, and then a ChunkSpec,
/// with the help of a QueryMapping can be applied on a QueryTemplate to produce
/// a concrete query. The abstraction is intended to provide some space between
/// the spherical box partitioning code and the query mapping code.
///
/// _subs stores the mapping from text-markup to partition number.
/// _subChunkTables was intended to aid subchunked query mapping, and will be
/// refined or removed when near-neighbor subchunked queries are done and
/// tested.
///
/// QueryMapping facilitates mapping a QueryTemplate to a concrete
/// queries for executing on workers. In the future, this responsibility may be
/// moved to the worker.
class QueryMapping {
public:
    enum Parameter { INVALID, CHUNK = 100, HTM1 = 200 };
    typedef std::map<std::string, Parameter> ParameterMap;

    QueryMapping();

    std::string apply(qproc::ChunkSpec const& s, query::QueryTemplate const& t) const;
    std::string apply(qproc::ChunkSpecSingle const& s, query::QueryTemplate const& t) const;

    // Modifiers
    /// Set the DbTable(s) that will be used with the subchunk query.
    void insertSubChunkTable(DbTable const& dbTable) { _subChunkTables.insert(dbTable); }
    /// Set the string that should be replaced by the chunk number in the query string.
    void insertChunkEntry(std::string const& tag) { _subs[tag] = CHUNK; }

    // Accessors
    bool hasChunks() const { return hasParameter(CHUNK); }
    bool hasSubChunks() const { return false == _subChunkTables.empty(); }
    bool hasParameter(Parameter p) const;
    DbTableSet const& getSubChunkTables() const { return _subChunkTables; }

private:
    ParameterMap _subs;
    DbTableSet _subChunkTables;
};

}  // namespace lsst::qserv::qana

#endif  // LSST_QSERV_QANA_QUERYMAPPING_H
