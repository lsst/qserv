// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2015 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_FIFOSCHEDULER_H
#define LSST_QSERV_WSCHED_FIFOSCHEDULER_H

// System headers
#include <mutex>

// Qserv headers
#include "util/EventThread.h"
#include "wcontrol/Foreman.h"

namespace lsst {
namespace qserv {
namespace wsched {

class FifoScheduler : public wcontrol::Scheduler {
public:
    typedef std::shared_ptr<FifoScheduler> Ptr;

    FifoScheduler() {};
    virtual ~FifoScheduler() {}

    std::string getName() const override { return std::string("FifoSched"); }
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_FIFOSCHEDULER_H
