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

#ifndef LSST_QSERV_UTIL_HOLDTRACK_H
#define LSST_QSERV_UTIL_HOLDTRACK_H

// System headers
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

// qserv headers
#include "util/Issue.h"

// This header declarations
namespace lsst::qserv::util {

/// This class is used to track where a program is waiting for long periods of time.
/// It is meant to be used to locate blocks on communication or mutex for very long
/// periods of time.
/// For this class to work CheckKeySet() needs to be called on a regular basis, one
/// call every 5 minutes may be adequate.
/// Time precision is limited to milliseconds, which could be an issue in loops or
/// recursion. This can be worked around by changing the note, but that situation
/// is not where this class is meant to be used.
class HoldTrack {
public:
    using Ptr = std::shared_ptr<HoldTrack>;

    /// Keys sorted by thread::id, sequence_number, uint64_t(time in milliseconds).
    /// sequence number is needed to keep items in order as too many items may occur in 1ms.
    using KeyType = std::pair<std::thread::id, std::pair<uint64_t, uint64_t>>;

    HoldTrack() = delete;
    HoldTrack(HoldTrack const&) = delete;
    HoldTrack& operator=(HoldTrack const&) = delete;

    ~HoldTrack() = default;

    /// Setup the global instance for storing hold information.
    /// Items that have existed for longer than `durationLimit` will be logged.
    static void setup(double durationLimitSeconds);

    /// Make a KeyType object given the current information.
    static KeyType makeKey();

    /// Return a string detailing keys that have existed longer than `durationLimit`.
    static std::string CheckKeySet();

    /// RAII helper class for HoldTrack.
    class Mark {
    public:
        using Ptr = std::shared_ptr<Mark>;
        Mark(Issue::Context const& ctx, std::string const& note) {
            if (_enabled && _globalInstance != nullptr) {
                _key = makeKey();
                _globalInstance->_addKey(_key, ctx, note);
                _inserted = true;
            }
        }

        ~Mark() {
            if (_inserted && _globalInstance != nullptr) {
                _globalInstance->_removeKey(_key);
            }
        }

    private:
        bool _inserted = false;  ///< set to true when a mark is inserted into the set.
        KeyType _key;            ///< that matches this Mark in the set.
    };

private:
    HoldTrack(double durationLimitSeconds) : _durationLimitMillisec(durationLimitSeconds * 1000.0) {
        _enabled = true;
    }

    /// Add `key` to _keyMap with ctx prepended to note.
    void _addKey(KeyType const& key, Issue::Context const& ctx, std::string const& note);

    /// Remove `key` from _keyMap.
    void _removeKey(KeyType const& key);

    /// Return the next sequence number.
    static uint64_t _getSeq() { return _seq++; }

    static std::atomic<bool> _enabled;  ///< Set to true to enable tracking.
    static Ptr _globalInstance;         ///< Pointer to the global instance.
    static std::atomic<uint64_t> _seq;  ///< Sequence number to keep items in order.

    double _durationLimitMillisec;           ///< Time that needs to pass before this item should be logged.
    std::map<KeyType, std::string> _keyMap;  ///< Set of all marks sorted by thread id, time, and note.
    std::mutex _mapMtx;                      ///< protects _keyMap;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_HOLDTRACK_H
