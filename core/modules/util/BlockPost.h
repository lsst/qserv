/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_BLOCK_POST_H
#define LSST_QSERV_UTIL_BLOCK_POST_H

/// BlockPost.h declares:
///
/// class BlockPost
/// (see individual class documentation for more information)

// System headers
#include <mutex>
#include <random>

// Qserv headers

// This header declarations

namespace lsst {
namespace qserv {
namespace util {

/**
 * The BlockPost provides a facility for blocking a calling thread
 * for a random number of milliseconds. The numbers are generated
 * by a built-in generator producing a series of uniformally
 * distributed numbers of milliseconds within a range specified upon
 * an object construction.
 *
 * THREAD SAFETY NOTE: this class implementation is thread-safe.
 */
class BlockPost {

public:
    
    // Default construction and copy semantics are proxibited

    BlockPost() = delete;
    BlockPost(BlockPost const&) = delete;
    BlockPost & operator=(BlockPost const&) = delete;

    /**
     * Normal constructor
     *
     * Allowed limits of the range should be positive. And the maximum
     * limit must be strictly higher than the lower one. Otherwise
     * std::invalid_argument will be thrown.
     *
     * @param minMilliseconds - the lower limit of the series
     * @param maxMilliseconds - the upper limit of the series
    */
    BlockPost(int minMilliseconds, int maxMilliseconds);

    /// Destructor
    ~BlockPost() = default;

    /**
     * Block a calling thread for a randomly generated number
     * of milliseconds.
     *
     * @return the number of milliseconds the thread was blocked for
     */
    int wait();

    /**
     * Block a calling thread for the specified number
     * of milliseconds. The number must be positive. Otherwise
     * std::invalid_argument will be thrown.
     *
     * @param milliseconds - the number in a range of 0 to the maximum positive
     *                       integer number.
     * @return the number of milliseconds the thread was blocked for
     */
    int wait(int milliseconds);

private:
    
    /// Return the next random number of milliseconds
    int next();

private:

    std::random_device              _rd;    // Will be used to obtain a seed for the random number engine
    std::mt19937                    _gen;   // Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> _distr;

    std::mutex _generatorMtx;   // for thread safety
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_BLOCK_POST_H
