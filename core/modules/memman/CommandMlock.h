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

#ifndef LSST_QSERV_MEMMAN_COMMANDMLOCK_H
#define LSST_QSERV_MEMMAN_COMMANDMLOCK_H

// System headers
#include <sys/mman.h>

// Qserv headers
#include "util/Command.h"


namespace lsst {
namespace qserv {
namespace memman {

/// Command to call mlock that allows other threads to wait for when it completes.
class CommandMlock : public util::CommandTracked {
public:
    using Ptr = std::shared_ptr<CommandMlock>;

    CommandMlock(void *mAddr, uint64_t mSize) : memAddr{mAddr}, memSize{mSize} {}

    /// Call mlock. waitComplete() will wait until this function is finished.
    void action(util::CmdData*) override {
        if (mlock(memAddr, memSize)) {
            errorCode = (errno == EAGAIN ? ENOMEM : errno);
        }
    }

    int errorCode{0}; ///< Error code if mlock fails.

private:
    void *memAddr;  ///< Memory address for mlock call.
    uint64_t memSize; ///< Size for mlock call.
};


}}} // namespace lsst::qserv::memman

#endif // LSST_QSERV_MEMMAN_COMMANDMLOCK_H
