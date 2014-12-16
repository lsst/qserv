// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// Third-party headers
#include "boost/unique_ptr.hpp"

// Qserv headers
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

namespace lsst {
namespace qserv {
namespace qproc {

/// SecondaryIndex handles lookups into a secondary index. Only one instance of
/// this is necessary: all user queries can share a single instance.
class SecondaryIndex {
public:
    SecondaryIndex(MySqlConfig const& c);
    SecondaryIndex(int); // Construct a fake instance

    ChunkSpecVector lookup(query::ConstraintVector const& cv);
private:
    class Backend;
    boost::unique_ptr<Backend> _backend;
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_SECONDARYINDEX_H
