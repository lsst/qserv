// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_TASKMSGFACTORY2_H
#define LSST_QSERV_MASTER_TASKMSGFACTORY2_H
/**
  * @file TaskMsgFactory2.h
  *
  * @brief TaskMsgFactory2 is a factory for TaskMsg (protobuf) objects. The "2"
  * differentiates it from the TaskMsgFactory in the Python layer (which has
  * been deprecated).
  *
  * @author Daniel L. Wang, SLAC
  */
#include <iostream>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
class ChunkQuerySpec;

/// TaskMsgFactory2 is a factory for TaskMsg (protobuf) objects. 
/// This functionality exists in the python later as TaskMsgFactory,
/// but we are pushing the functionality to C++ so that we can avoid
/// the Python/C++ for each chunk query. This should dramatically
/// improve query dispatch speed (and also reduce overall user query
/// latency).
class TaskMsgFactory2 {
public:
    TaskMsgFactory2(int session);

    /// Construct a TaskMsg and serialize it to a stream
    void serializeMsg(ChunkQuerySpec const& s, 
                      std::string const& chunkResultName,
                      std::ostream& os);
private:
    class Impl;

    boost::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_TASKMSGFACTORY2_H

