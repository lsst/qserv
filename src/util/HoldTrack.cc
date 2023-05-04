/*
 * LSST Data Management System
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
#include "util/HoldTrack.h"

// System headers
#include <sstream>

// Qserv headers
#include "global/clock_defs.h"
#include "util/Bug.h"

#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.HoldTrack");
}

namespace lsst::qserv::util {

HoldTrack::Ptr HoldTrack::_globalInstance;
std::atomic<uint64_t> HoldTrack::_seq{0};  ///< Sequence number to keep items in order.

void HoldTrack::setup(std::chrono::milliseconds durationLimitMillisec) {
    if (_globalInstance != nullptr) {
        throw util::Bug(ERR_LOC, "HoldTrack::setup called when already setup!");
    }
    _globalInstance = Ptr(new HoldTrack(durationLimitMillisec));
}

void HoldTrack::_addKey(KeyType const& key, Issue::Context const& ctx, string const& note) {
    ostringstream os;
    ctx.print(os);
    os << " " << note;
    string str = os.str();

    lock_guard<mutex> lockG(_mapMtx);
    _keyMap[key] = str;
}

void HoldTrack::_removeKey(KeyType const& key) {
    lock_guard<mutex> lockG(_mapMtx);
    _keyMap.erase(key);
}

HoldTrack::KeyType HoldTrack::makeKey() {
    TIMEPOINT timePoint = CLOCK::now();
    std::chrono::milliseconds tm =
            std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch());
    thread::id tid = this_thread::get_id();
    uint64_t seq = _getSeq();

    KeyType key = make_pair(tid, make_pair(seq, tm));
    return key;
}

std::string HoldTrack::CheckKeySet() {
    ostringstream os;
    os << "HoldTrack::CheckKeySet held keys ";

    auto gi = _globalInstance;
    if (gi == nullptr) {
        os << "diasabled";
        return os.str();
    }
    TIMEPOINT timePoint = CLOCK::now();
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch());
    lock_guard<mutex> lockG(gi->_mapMtx);
    for (auto const& elem : gi->_keyMap) {
        auto const& key = elem.first;
        thread::id tid = key.first;
        uint64_t seq = key.second.first;
        std::chrono::milliseconds tm = key.second.second;
        string const& note = elem.second;

        std::chrono::milliseconds durationMillisec = (now - tm);
        if (durationMillisec > gi->_durationLimitMillisec) {
            os << "NEXT{tid:" << tid << " " << seq << " secs:" << (durationMillisec.count() / 1000.0) << " "
               << note << "}";
        }
    }

    return os.str();
}

}  // namespace lsst::qserv::util
