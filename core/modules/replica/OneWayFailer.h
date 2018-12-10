/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_ONEWAYFAILER_H
#define LSST_QSERV_REPLICA_ONEWAYFAILER_H

// System headers
#include <atomic>

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * This class maintains a boolean state which can go in one direction
 * only: from 'false' to 'true'. The state is thread safe.
 */
class OneWayFailer {

public:

    /**
     * Fail the state.
     *
     * @return  the previous state of the object
     */
    bool fail() {
        return _failed.exchange(true);
    }

    /// @return the current state of the object
    bool operator()() const { return _failed; }

private:
    std::atomic<bool> _failed{false};
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_ONEWAYFAILER_H */
