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
#ifndef LSST_QSERV_REPLICA_CORE_SUCCESSRATEGENERATOR_H
#define LSST_QSERV_REPLICA_CORE_SUCCESSRATEGENERATOR_H

/// SuccessRateGenerator.h declares:
///
/// class SuccessRateGenerator
/// (see individual class documentation for more information)

// System headers

#include <mutex>
#include <random>

// Qserv headers


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
 * The SuccessRateGenerator provides a facility for generating a sequence
 * of random boolean values which can be used for simulating Success/Failure
 * scenarios. An implementation of the class is based on the uniform distribution.
 * True values returned by the genrator are interpreted as 'success'. The probability
 * density ('success rate') is specified through the constructr of
 * the class.
 *
 * THREAD SAFETY: the generator is thread-safe.
 */
class SuccessRateGenerator {

public:
    
    // Default construction and copy semantics are proxibited

    SuccessRateGenerator () = delete;
    SuccessRateGenerator (SuccessRateGenerator const&) = delete;
    SuccessRateGenerator & operator= (SuccessRateGenerator const&) = delete;

    /**
     * Normal constructor
     *
     * Allowed range for the 'successRate' parameter is [0.0,1.0].
     * Note that both ends of the range are inclusive. Choosing a value
     * equal to (witin the small epsilon) 0.0 would result in the 100% failure rate.
     * The opposite scenario will be seen when choosing  1.0.
     *
     * @param successRate - probability density for 'success'
    */
    explicit SuccessRateGenerator (double successRate=0.5);

    /**
     * Generate the next random value.
     *
     * @return 'true' for 'success'
     */
    bool success ();

private:

    double _successRate;

    std::random_device          _rd;    // Will be used to obtain a seed for the random number engine
    std::mt19937                _gen;   // Standard mersenne_twister_engine seeded with rd()
    std::bernoulli_distribution _distr;

    std::mutex _generatorMtx;   // for thread safety
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_SUCCESSRATEGENERATOR_H