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
#ifndef LSST_QSERV_QDISP_RESPONSEPOOL_H
#define LSST_QSERV_QDISP_RESPONSEPOOL_H

// System headers

// Third-party headers

// Qserv headers
#include "util/ThreadPool.h"

namespace lsst {
namespace qserv {
namespace qdisp {


class ResponsePool {
public:
    typedef std::shared_ptr<ResponsePool> Ptr;

    ResponsePool() {}

    void queCmdHigh(util::Command::Ptr const& cmd) {
        _highQueue->queCmd(cmd);
    }
    void queCmdLow(util::Command::Ptr const& cmd) {
        _lowQueue->queCmd(cmd);
    }

    void shutdownPool() {
        _highPriority->shutdownPool();
        _lowPRiority->shutdownPool();
    }

private:
    util::CommandQueue::Ptr _highQueue{new util::CommandQueue()};
    util::ThreadPool::Ptr _highPriority{util::ThreadPool::newThreadPool(7, _highQueue)};

    util::CommandQueue::Ptr _lowQueue{new util::CommandQueue()};
    util::ThreadPool::Ptr _lowPRiority{util::ThreadPool::newThreadPool(3, _lowQueue)};
};


}}} // namespace lsst::qserv::disp

#endif /* LSST_QSERV_QDISP_RESPONSEPOOL_H_ */
