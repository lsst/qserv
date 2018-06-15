// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_PERFORMANCE_H
#define LSST_QSERV_REPLICA_PERFORMANCE_H

/// Performance.h declares:
///
/// class PerformanceUtils
/// class Performance
/// class WorkerPerformance
/// (see individual class documentation for more information)

// System headers
#include <iostream>

// Forward declarations

namespace lsst {
namespace qserv {
namespace proto {

/// The protocol class
class ReplicationPerformance;

}}} // namespace lsst::qserv::proto

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Struct PerformanceUtils provides utilities shared by all classes in this scope
 */
struct PerformanceUtils {

    /// @return the current time in milliseconds since Epoch
    static uint64_t now();    
};

/**
 * Class Performance encapculates controller-side performance counters of requests
 * 
 * The counters are meant for tracking requests progression over time.
 * All time counters are expressed in milliseconds since Epoch.
 * Undefined values are set to 0.
 */
class Performance {

public:

    /**
     * The default constructor
     *
     * All (but the request creation one) timestamps will be initialized wih 0.
     */
    Performance();
    
    Performance(Performance const&) = default;
    Performance& operator=(Performance const&) = default;

    ~Performance() = default;

    /**
     * Update object state with counters from the protocol buffer object
     *
     * @param workerPerformanceInfo - counters to be carried over into an internal state
     */
    void update(proto::ReplicationPerformance const& workerPerformanceInfo);

    /**
     * Update the Controller's 'start' time
     *
     * @return the previous state of the counter
     */
    uint64_t setUpdateStart();

    /**
     * Update the Controller's 'finish' time
     *
     * @return the previous state of the counter
     */
    uint64_t setUpdateFinish();

public:

    /// Created by the Controller
    uint64_t c_create_time;

    /// Started by the Controller
    uint64_t c_start_time;

    /// Received by a worker service
    uint64_t w_receive_time;

    /// Execution started by a worker service
    uint64_t w_start_time;

    /// Execution fiished by a worker service
    uint64_t w_finish_time;

    /// A subscriber notified by the Controller
    uint64_t c_finish_time;
};

/// Overloaded streaming operator for class Performance
std::ostream& operator<<(std::ostream& os, Performance const& p);


/**
 * Class WorkerPerformance is worker-side value class with performance counters
 * of a request.
 *
 * All time counters are expressed in milliseconds since Epoch.
 * Undefined values are set to 0.
 */
class WorkerPerformance {

public:

    /**
     * The default constructor
     *
     * All (but the request 'receive' one) timestamps will be initialized wih 0.
     */
    WorkerPerformance();
    
    WorkerPerformance(WorkerPerformance const&) = default;
    WorkerPerformance& operator=(WorkerPerformance const&) = default;

    ~WorkerPerformance() = default;

    /**
     * Update the 'start' time
     *
     * @return the previous state of the counter
     */
    uint64_t setUpdateStart();

    /**
     * Update the 'finish' time
     *
     * @return the previous state of the counter
     */
    uint64_t setUpdateFinish();

    /**
     * @return a protobuf object
     *
     * OWNERSHIP TRANSFER NOTE: this method allocates a new object and
     * returns a pointer along with its ownership.
     */
    proto::ReplicationPerformance* info() const;

public:

    /// Received by a worker service
    uint64_t receive_time;

    /// Execution started by a worker service
    uint64_t start_time;

    /// Execution fiished by a worker service
    uint64_t finish_time;
};

/// Overloaded streaming operator for class WorkerPerformance
std::ostream& operator<<(std::ostream& os, WorkerPerformance const&p);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_PERFORMANCE_H