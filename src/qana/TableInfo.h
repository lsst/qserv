// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

#ifndef LSST_QSERV_QANA_TABLEINFO_H
#define LSST_QSERV_QANA_TABLEINFO_H

/// \file
/// \brief Table metadata classes

/// \page table_types Table Types
///
/// There are four different kinds of tables in the Qserv system. The first
/// and simplest is the "unpartitioned" table. These are available in their
/// entirety to every worker. Arbitrary joins are allowed between them,
/// and there is no need to validate or rewrite such joins in any way.
///
/// The second kind is the "director" table. Director tables are spatially
/// partitioned into chunks (based on longitude and latitude) that are
/// distributed across the Qserv workers. Each chunk can be subdivided into
/// sub-chunks to make near-neighbor joins tractable (more on this later).
/// Additionally, the rows in close spatial proximity to each sub-chunk
/// are stored in an "overlap" table, itself broken into chunks. This allows
/// near-neighbor queries to look outside of the spatial boundaries of a
/// sub-chunk for matches to a position inside it without consulting other
/// workers and incurring the attendant network and implementation costs.
/// Currently, director tables with composite primary keys are not supported.
///
/// "Child" tables are partitioned into chunks according to a director table.
/// A child table contains (at least conceptually) a foreign key into a
/// director table, and each of its rows is assigned to the same chunk as the
/// corresponding director table row. Overlap is not stored for child tables,
/// nor is it possible to create sub-chunks for them on the fly.
///
/// Finally, "match" tables provide an N-to-M mapping between two director
/// tables that have been partitioned in the same way, i.e. that have chunks
/// and sub-chunks which line up exactly in superposition. A match table
/// contains a pair of foreign keys into two director tables `A` and `B`,
/// and matches between `a` ∈ `A` and `b` ∈ `B` are stored in the chunks
/// of both `a` and `b`. A match can relate director table rows `a` and `b`
/// from different chunks so long as `a` falls into the overlap of the
/// chunk containing `b` (and vice versa).

// System headers
#include <cstddef>
#include <memory>
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

// Qserv headers
#include "global/constants.h"

// Forward declarations
namespace lsst::qserv::query {
class ColumnRef;
}  // namespace lsst::qserv::query

namespace lsst::qserv::qana {

typedef std::shared_ptr<query::ColumnRef const> ColumnRefConstPtr;

struct DirTableInfo;
struct ChildTableInfo;
struct MatchTableInfo;

/// `TableInfo` is a base class for table metadata. A subclass is provided
/// for each [kind of table](\ref table_types) supported by Qserv except
/// unpartitioned tables, which are omitted because they are uninteresting
/// for query analysis.
struct TableInfo {
    enum Kind { DIRECTOR = 0, CHILD, MATCH, NUM_KINDS };

    std::string const database;
    std::string const table;
    Kind const kind;

    TableInfo(std::string const& db, std::string const& t, Kind k) : database(db), table(t), kind(k) {}

    virtual ~TableInfo() {}

    bool operator==(TableInfo const& t) const { return database == t.database && table == t.table; }
    bool operator<(TableInfo const& t) const {
        int c = table.compare(t.table);
        return c < 0 || (c == 0 && database < t.database);
    }

    /// `makeColumnRefs` returns all possible references to join columns
    /// from this table.
    virtual std::vector<ColumnRefConstPtr> const makeColumnRefs(std::string const& tableAlias) const {
        return std::vector<ColumnRefConstPtr>();
    }

    /// `isEqPredAdmissible` returns true if the equality predicate `a = b` is
    /// is admissible, where `a` names a column in this table and `b` names a
    /// column in `t`. An admissible join predicate is one that can be used to
    /// infer the partition of rows in one table from the partition of rows in
    /// another. The `outer` flag indicates whether the predicate occurs in the
    /// ON clause of an outer join.
    virtual bool isEqPredAdmissible(TableInfo const& t, std::string const& a, std::string const& b,
                                    bool outer) const {
        return false;
    }
    virtual bool isEqPredAdmissible(DirTableInfo const& t, std::string const& a, std::string const& b,
                                    bool outer) const {
        return false;
    }
    virtual bool isEqPredAdmissible(ChildTableInfo const& t, std::string const& a, std::string const& b,
                                    bool outer) const {
        return false;
    }
    virtual bool isEqPredAdmissible(MatchTableInfo const& t, std::string const& a, std::string const& b,
                                    bool outer) const {
        return false;
    }

    std::string const getChunkTemplate() const { return table + "_" + CHUNK_TAG; }
    std::string const getOverlapTemplate() const { return table + "FullOverlap_" + CHUNK_TAG; }

    virtual void dump(std::ostream& os) const;
};

inline std::ostream& operator<<(std::ostream& os, TableInfo const& ti) {
    ti.dump(os);
    return os;
}

/// `DirTableInfo` contains metadata for director tables.
struct DirTableInfo : TableInfo {
    std::string pk;   ///< `pk` is the name of the director's primary key column.
    std::string lon;  ///< `lon` is the name of the director's longitude column.
    std::string lat;  ///< `lat` is the name of the director's latitude column.
    int32_t partitioningId;
    double overlap;  ///< overlap value for partitioning of this table

    DirTableInfo(std::string const& db, std::string const& t, double o)
            : TableInfo(db, t, DIRECTOR), partitioningId(0), overlap(o) {}

    std::vector<ColumnRefConstPtr> const makeColumnRefs(std::string const& tableAlias) const override;

    bool isEqPredAdmissible(TableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // double dispatch
        return t.isEqPredAdmissible(*this, b, a, outer);
    }
    bool isEqPredAdmissible(DirTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override;
    bool isEqPredAdmissible(ChildTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override;
    bool isEqPredAdmissible(MatchTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override;
    void dump(std::ostream& os) const override;
};

/// `ChildTableInfo` contains metadata for child tables.
struct ChildTableInfo : TableInfo {
    /// `director` is an unowned pointer to the metadata
    /// for the director table referenced by `fk`.
    DirTableInfo const* director;
    /// `fk` is the name of the foreign key column referencing `director->pk`.
    std::string fk;

    ChildTableInfo(std::string const& db, std::string const& t) : TableInfo(db, t, CHILD), director(0) {}

    std::vector<ColumnRefConstPtr> const makeColumnRefs(std::string const& tableAlias) const override;

    bool isEqPredAdmissible(TableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // double dispatch
        return t.isEqPredAdmissible(*this, b, a, outer);
    }
    bool isEqPredAdmissible(DirTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // take advantage of symmetry to avoid code stutter
        return t.isEqPredAdmissible(*this, b, a, outer);
    }
    bool isEqPredAdmissible(ChildTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override;
    bool isEqPredAdmissible(MatchTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override;

    void dump(std::ostream& os) const override;
};

/// `MatchTableInfo` contains metadata for child tables.
struct MatchTableInfo : TableInfo {
    /// `director` is a pair of unowned pointers to the metadata for the
    /// director tables referenced by `fk.first` and `fk.second`.
    std::pair<DirTableInfo const*, DirTableInfo const*> director;
    /// `fk` is the pair of names for the foreign key columns referencing
    /// `director.first->pk` and `director.second->pk`.
    std::pair<std::string, std::string> fk;
    double angSep;  ///< allowed angular separation between objects in director tables

    MatchTableInfo(std::string const& db, std::string const& t, double sep)
            : TableInfo(db, t, MATCH), director(), angSep(sep) {}

    std::vector<ColumnRefConstPtr> const makeColumnRefs(std::string const& tableAlias) const override;

    bool isEqPredAdmissible(TableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // double dispatch
        return t.isEqPredAdmissible(*this, b, a, outer);
    }
    bool isEqPredAdmissible(DirTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // take advantage of symmetry to avoid code stutter
        return t.isEqPredAdmissible(*this, b, a, outer);
    }
    bool isEqPredAdmissible(ChildTableInfo const& t, std::string const& a, std::string const& b,
                            bool outer) const override {
        // take advantage of symmetry to avoid code stutter
        return t.isEqPredAdmissible(*this, b, a, outer);
    }

    void dump(std::ostream& os) const override;
};

}  // namespace lsst::qserv::qana

#endif  // LSST_QSERV_QANA_TABLEINFO_H
