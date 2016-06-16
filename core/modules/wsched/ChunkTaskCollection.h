// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_CHUNKTASKCOLLECTION_H
#define LSST_QSERV_WSCHED_CHUNKTASKCOLLECTION_H

// System headers

// qserv headers
#include "wbase/Task.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// Class defines an interface to store Tasks related to chunks in an ordered manner.
/// The only derived classes are expected to be ChunkDisk and ChunkTasksQueue.
/// With luck, one of them will prove to be superior and the other will be destroyed, thus
/// this class will no longer be needed.
class ChunkTaskCollection {
public:
    /// Queue a Task to be run later.
    virtual void queueTask(wbase::Task::Ptr const& task) = 0;

    /// Return a Task that is ready to run or nullptr if nothing is ready.
    /// A Task is only ready if the conditions in ready() are met.
    virtual wbase::Task::Ptr getTask(bool useFlexibleLock) = 0;
    virtual bool empty() const = 0;
    virtual std::size_t getSize() const = 0;

    /// @return true if there is a Task available and conditions are acceptable.
    /// Conditions include things such as memory resources being available for the Task.
    virtual bool ready(bool useFlexibleLock) = 0;

    /// This function will be called when the Task has completed its first transmit to the czar.
    virtual void taskComplete(wbase::Task::Ptr const& task) = 0;

    /// This is set to true when ready() returns false due to not enough memory available.
    virtual bool setResourceStarved(bool starved) = 0;

    /// @return true if the next Task will come from a different active chunk.
    virtual bool nextTaskDifferentChunkId() = 0;
};

}}} // namespace lsst::qserv::wsched




#endif // LSST_QSERV_WSCHED_CHUNKTASKCOLLECTION_H
