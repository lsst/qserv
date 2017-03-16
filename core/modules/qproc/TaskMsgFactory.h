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

#ifndef LSST_QSERV_QPROC_TASKMSGFACTORY_H
#define LSST_QSERV_QPROC_TASKMSGFACTORY_H
/**
  * @file
  *
  * @brief TaskMsgFactory is a factory for TaskMsg (protobuf) objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>
#include <memory>

// Qserv headers
#include "proto/worker.pb.h"

namespace lsst {
namespace qserv {
namespace qproc {

class ChunkQuerySpec;

/// TaskMsgFactory is a factory for TaskMsg (protobuf) objects.
/// All member variables must be thread safe.
class TaskMsgFactory {
public:
    TaskMsgFactory(uint64_t session) : _session(session) {};

    /// Construct a TaskMsg and serialize it to a stream
    void serializeMsg(ChunkQuerySpec const& s,
                      std::string const& chunkResultName,
                      uint64_t queryId, int jobId,
                      std::ostream& os);
private:
    std::shared_ptr<proto::TaskMsg> _makeMsg(ChunkQuerySpec const& s,
                                                std::string const& chunkResultName,
                                                uint64_t queryId, int jobId);

    void _addFragment(proto::TaskMsg& taskMsg, std::string const& resultName,
                    std::vector<std::string> const& subChunkTables,
                    std::vector<int> const& subChunkIds,
                    std::vector<std::string> const& queries);

    /// All member variable need to be thread safe.
    uint64_t const _session;
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_TASKMSGFACTORY_H

