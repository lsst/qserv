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

// Class header
#include "util/BlockPost.h"

// System headers
#include <chrono>
#include <ratio>  // std::milli
#include <stdexcept>
#include <string>
#include <thread>

namespace lsst::qserv::util {

BlockPost::BlockPost(int minMilliseconds, int maxMilliseconds)
        : _rd(), _gen(_rd()), _distr(minMilliseconds, maxMilliseconds) {
    if ((minMilliseconds < 0) || (minMilliseconds >= maxMilliseconds))
        throw std::invalid_argument(
                "BlockPost::" + std::string(__func__) + " - invalid range of milliseconds provided: [" +
                std::to_string(minMilliseconds) + ".." + std::to_string(maxMilliseconds) + "]");
}

int BlockPost::wait() {
    const int ival = next();
    BlockPost::wait(ival);
    return ival;
}

int BlockPost::wait(int milliseconds) {
    if (milliseconds < 0)
        throw std::invalid_argument("BlockPost::" + std::string(__func__) +
                                    "(milliseconds) - invalid number of milliseconds");
    std::this_thread::sleep_for(
            std::chrono::duration<long, std::milli>(std::chrono::milliseconds(milliseconds)));
    return milliseconds;
}

int BlockPost::next() {
    std::lock_guard<std::mutex> lock(_generatorMtx);
    return _distr(_gen);
}

}  // namespace lsst::qserv::util
