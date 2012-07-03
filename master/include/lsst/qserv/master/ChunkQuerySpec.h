// -*- LSST-C++ -*-
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
// ChunkQuerySpec is an value class that contains information pertinent to
// executing a chunk query. 

#ifndef LSST_QSERV_MASTER_CHUNKQUERYSPEC_H
#define LSST_QSERV_MASTER_CHUNKQUERYSPEC_H
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
// This is pretty tentative.
class ChunkQuerySpec {
public:
    std::string db;
    int chunkId;
    std::vector<std::string> subChunkTables;
    std::vector<int> subChunkIds;
    std::vector<std::string> queries;
    boost::shared_ptr<ChunkQuerySpec> nextFragment;
};
// Implementation in TaskMsgFactory2.cc
std::ostream& operator<<(std::ostream& os, ChunkQuerySpec const& c);
}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_CHUNKQUERYSPEC_H

