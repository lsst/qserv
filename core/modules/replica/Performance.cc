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

// Class header
#include "replica/Performance.h"

// System headers
#include <chrono>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "proto/replication.pb.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Performance");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

////////////////////////////////////////////////////////////
///////////////////// PerformanceUtils /////////////////////
////////////////////////////////////////////////////////////

uint64_t PerformanceUtils::now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
}

///////////////////////////////////////////////////////
///////////////////// Performance /////////////////////
///////////////////////////////////////////////////////

Performance::Performance()
    :   c_create_time(PerformanceUtils::now()),
        c_start_time(0),
        w_receive_time(0),
        w_start_time(0),
        w_finish_time(0),
        c_finish_time(0) {
}

void Performance::update(proto::ReplicationPerformance const& workerPerformanceInfo) {
    w_receive_time = workerPerformanceInfo.receive_time();
    w_start_time   = workerPerformanceInfo.start_time();
    w_finish_time  = workerPerformanceInfo.finish_time();
}

uint64_t Performance::setUpdateStart() {
    uint64_t const t = c_start_time;
    c_start_time = PerformanceUtils::now();
    return t;
}

uint64_t Performance::setUpdateFinish() {
    uint64_t const t = c_finish_time;
    c_finish_time = PerformanceUtils::now();
    return t;
}

std::ostream& operator<<(std::ostream& os, Performance const& p) {
    os  << "Performance "
        << " c.create:"   << p.c_create_time
        << " c.start:"    << p.c_start_time
        << " w.receive:"  << p.w_receive_time
        << " w.start:"    << p.w_start_time
        << " w.finish:"   << p.w_finish_time
        << " c.finish:"   << p.c_finish_time
        << " length.sec:" << (p.c_finish_time ? (p.c_finish_time - p.c_start_time)/1000. : '*');
    return os;
}

/////////////////////////////////////////////////////////////
///////////////////// WorkerPerformance /////////////////////
/////////////////////////////////////////////////////////////

WorkerPerformance::WorkerPerformance()
    :   receive_time(PerformanceUtils::now()),
        start_time(0),
        finish_time(0) {
}

uint64_t WorkerPerformance::setUpdateStart() {
    uint64_t const t = start_time;
    start_time = PerformanceUtils::now();
    return t;
}

uint64_t WorkerPerformance::setUpdateFinish() {
    uint64_t const t = finish_time;
    finish_time = PerformanceUtils::now();
    return t;
}

proto::ReplicationPerformance* WorkerPerformance::info() const {
    auto ptr = new proto::ReplicationPerformance();
    ptr->set_receive_time(receive_time);
    ptr->set_start_time(start_time);
    ptr->set_finish_time(finish_time);
    return ptr;
}

std::ostream& operator<<(std::ostream& os, WorkerPerformance const& p) {
    os  << "WorkerPerformance "
        << " receive:"    << p.receive_time
        << " start:"      << p.start_time
        << " finish:"     << p.finish_time
        << " length.sec:" << (p.finish_time ? (p.finish_time - p.receive_time)/1000. : '*');
    return os;
}

}}} // namespace lsst::qserv::replica