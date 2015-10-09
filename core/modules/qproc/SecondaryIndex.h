// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#ifndef LSST_QSERV_QPROC_SECONDARYINDEX_H
#define LSST_QSERV_QPROC_SECONDARYINDEX_H
/**
  * @file
  *
  * @brief SecondaryIndex to plug into index map to handle lookups
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <stdexcept>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

namespace lsst {
namespace qserv {
namespace qproc {

/**
 *  SecondaryIndex handles lookups into Qserv secondary index.
 *
 *  Only one instance of this is necessary: all user queries
 *  can share a single instance.
 */
class SecondaryIndex {
public:
    explicit SecondaryIndex(mysql::MySqlConfig const& c);

    /** Construct a fake instance
     *
     *  Used for testing purpose
     */
    explicit SecondaryIndex();

    /** Lookup an index constraint.
     *
     *  If no index constraint exists, throw a NoIndexConstraint exception.
     *  Index constraints are combined with OR.
     */
    ChunkSpecVector lookup(query::ConstraintVector const& cv);

    class NoIndexConstraint : public std::invalid_argument {
    public:
        NoIndexConstraint()
            : std::invalid_argument("Missing index constraint")
            {}
    };

    class Backend;
private:
    // change to unique_ptr
    std::shared_ptr<Backend> _backend;
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_SECONDARYINDEX_H
