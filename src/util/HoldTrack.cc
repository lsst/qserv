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

std::atomic<bool> HoldTrack::_enabled{false};
HoldTrack::Ptr HoldTrack::_globalInstance;
std::atomic<uint64_t> HoldTrack::_seq{0};  ///< Sequence number to keep items in order.

void HoldTrack::setup(double durationLimitSeconds) {
    if (_globalInstance != nullptr) {
        throw util::Bug(ERR_LOC, "HoldTrack::setup called when already setup!");
    }
    _globalInstance = Ptr(new HoldTrack(durationLimitSeconds));
}

void HoldTrack::_addKey(KeyType const& key) {
    lock_guard<mutex> lockG(_setMtx);
    _keySet.insert(key);
}

void HoldTrack::_removeKey(KeyType const& key) {
    lock_guard<mutex> lockG(_setMtx);
    _keySet.erase(key);
}

HoldTrack::KeyType HoldTrack::makeKey(Issue::Context const& ctx, std::string const& note) {
    TIMEPOINT timePoint = CLOCK::now();
    uint64_t tm = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
    thread::id tid = this_thread::get_id();
    uint64_t seq = _getSeq();

    ostringstream os;
    ctx.print(os);
    os << " " << note;
    string str = os.str();

    KeyType key = make_pair(make_pair(tid, seq), make_pair(tm, str));
    return key;
}

std::string HoldTrack::CheckKeySet() {
    ostringstream os;
    os << "HoldTrack::CheckKeySet held keys ";

    auto gi = _globalInstance;
    if (!_enabled || gi == nullptr) {
        os << "diasabled";
        return os.str();
    }
    TIMEPOINT timePoint = CLOCK::now();
    uint64_t now =
            std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
    lock_guard<mutex> lockG(gi->_setMtx);
    for (auto const& key : gi->_keySet) {
        thread::id tid = key.first.first;
        uint64_t seq = key.first.second;
        uint64_t tm = key.second.first;
        string const& note = key.second.second;

        double durationMillisec = (now - tm);
        if (durationMillisec > gi->_durationLimitMillisec) {
            // &&& remove sequence from log message ???
            os << "NEXT{tid:" << tid << " " << seq << "secs:" << (durationMillisec / 1000.0) << " " << note
               << "}";
        }
    }

    return os.str();
}

}  // namespace lsst::qserv::util
