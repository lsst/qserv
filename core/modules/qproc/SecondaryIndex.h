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
#include "query/typedefs.h"

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
    /**
     * @brief Construct a mysql-backed instance
     */
    explicit SecondaryIndex(mysql::MySqlConfig const& c);

    /**
     * @brief Construct a diridx Redis-backed instance
     *
     * @param name the ip address and port number of a node to connect to separated by a colon
     *        e.g. "192.168.0.1:6379"
     *
     *  TODO a variable called 'name' that takes an ip address + port may not be the best var name. I think
     *  the function can not take a dns name (at least the Redis api can't?), consider wrapping that or
     *  changing the variable name to something more apropriate.
     */
    SecondaryIndex(std::string const& name);

    /** Construct a fake instance
     *
     *  Used for testing purpose
     */
    SecondaryIndex();

    /** Lookup an index restriction.
     *
     *  Index restrictors are combined with OR.
     */
    ChunkSpecVector lookup(query::SecIdxRestrictorVec const& restrictors);

    class Backend;
private:
    // change to unique_ptr
    std::shared_ptr<Backend> _backend;
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_SECONDARYINDEX_H
