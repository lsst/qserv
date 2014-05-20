// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_QANA_TABLEINFOPOOL_H
#define LSST_QSERV_QANA_TABLEINFOPOOL_H

/// \file
/// \brief A class for creating and pooling table metadata objects.

// System headers
#include <string>
#include <vector>

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryContext;
}
namespace qana {
    struct TableInfo;
}}}


namespace lsst {
namespace qserv {
namespace qana {

/// `TableInfoPool` is a factory and pool of owned, immutable `TableInfo`
/// objects.
///
/// Clients that obtain all `TableInfo` pointers from the same pool
/// can use pointer equality tests to check for `TableInfo` equality. There
/// is no facility for removing pool entries, so the lifetime of all retrieved
/// pointers is that of the pool itself.
///
/// `TableInfoPool` is not currently thread-safe.
class TableInfoPool {
public:
    TableInfoPool() {}
    ~TableInfoPool();

    /// `get` returns a pointer to metadata for the given table, or null if
    /// there is none available. The pool retains pointer ownership.
    TableInfo const* get(std::string const& db,
                         std::string const& table) const;

    /// `get` returns a pointer to metadata for the given table, creating
    /// a metadata object if necessary. The pool retains pointer ownership.
    /// Null pointers are returned for unpartitioned tables, as they have no
    /// metadata and representing them is not worthwhile. Newly created
    /// metadata objects are sanity checked, and an `InvalidTableError` is
    /// thrown if any inconsistencies are found.
    ///
    /// In case of an exception, the pool remains safe to use, but may
    /// contain additional metadata objects that were not present before
    /// the get() call. This is because some metadata objects contain
    /// unowned pointers to other metadata objects - these can sometimes
    /// be successfully created and added to the pool before an exception
    /// is thrown for the directly requested object.
    TableInfo const* get(query::QueryContext const& ctx,
                         std::string const& db,
                         std::string const& table);

private:
    // A set implemented as a sorted vector since the number of entries
    // is expected to be small.
    typedef std::vector<TableInfo const*> Pool;

    // not implemented
    TableInfoPool(TableInfoPool const&);
    TableInfoPool& operator=(TableInfoPool const&);

    void _insert(TableInfo const* t);

    Pool _pool;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_TABLEINFOPOOL_H
