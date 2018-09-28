// -*- LSST-C++ -*-
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
 *
 */
#ifndef LSST_QSERV_LOADER_STRINGRANGE_H_
#define LSST_QSERV_LOADER_STRINGRANGE_H_

// system headers
#include <memory>
#include <string>

// Qserv headers
#include "loader/Updateable.h"


namespace lsst {
namespace qserv {
namespace loader {

/// Class for storing the range of a single worker.
/// This is likely to become a template class, hence lots of stuff in the header.
class StringRange {
public:
    using Ptr = std::shared_ptr<StringRange>;

    StringRange() = default;
    StringRange(StringRange const&) = default;
    StringRange& operator=(StringRange const&) = default;

    ~StringRange() = default;

    void setAllInclusiveRange() {
        _min = "";
        _unlimited = true;
        setValid();
    }

    bool setMin(std::string const& val) {
        if (not _unlimited && val >= _maxE) {
            return false;
        }
        _min = val;
    }

    bool setMax(std::string const& val, bool unlimited=false) {
        if (unlimited) {
            _unlimited = true;
            if (val > _maxE) { _maxE = val; }
            return true;
        }
        if (val < _min) {
            return false;
        }
        _maxE = val;
        return true;
    }

    bool setMinMax(std::string const& vMin, std::string const& vMax, bool unlimited=false) {
        if (!unlimited && vMin > vMax) {
            return false;
        }
        if (unlimited) {
            _unlimited = true;
            _min = vMin;
            _maxE = std::max(vMax, _min); // max is irrelevant at this point
        } else {
            _min = vMin;
            _maxE = vMax;
        }
        setValid();
        return true;
    }

    bool setValid() {
        if (!_unlimited && _maxE <= _min) {
            return false;
        }
        _valid = true;
        return true;
    }

    /// Return true if other functionally equivalent.
    bool equal(StringRange const& other) const {
        if (_valid != other._valid) { return false; }
        if (not _valid) { return true; }  // both invalid
        if (_min != other._min) { return false; }
        if (_unlimited != other._unlimited) { return false; }
        if (_unlimited) { return true; } // both same _min and _unlimited
        if (_maxE != other._maxE) { return false; }
        return true;
    }

    bool isInRange(std::string const& str) const {
        if (not _valid) { return false; }
        if (str < _min) { return false; }
        if (not _unlimited && str >= _maxE) { return false; }
        return true;
    }

    bool getValid() const { return _valid; }
    bool getUnlimited() const { return _unlimited; }
    std::string getMin() const { return _min; }
    std::string getMax() const { return _maxE; }

    bool operator<(StringRange const& other) const {
        /// Arbitrarily, invalid are less than valid, but such comparisons should be avoided.
        if (_valid != other._valid) {
            if (not _valid) { return true; }
            return false;
        }
        /// Compare minimums. There should be little if any overlap.
        if (_min < other._min) { return true; }
        return false;
    }

    /// Return a string that would slightly follow the value of the input string 'str'
    /// appendChar is the character appended to a string ending with a character > 'z'
    static std::string advanceString(std::string const& str, char appendChar='a');


    friend std::ostream& operator<<(std::ostream&, StringRange const&);

private:
    bool        _valid{false}; ///< true if range is valid
    bool        _unlimited{false}; ///< true if the range includes largest possible values.
    std::string _min; ///< Smallest value = ""
    std::string _maxE; ///< maximum value exclusive

};


struct NeighborsInfo {
    NeighborsInfo() = default;
    NeighborsInfo(NeighborsInfo const&) = delete;
    NeighborsInfo& operator=(NeighborsInfo const&) = delete;

    typedef std::shared_ptr<Updatable<uint32_t>> NeighborPtr;
    typedef std::weak_ptr<Updatable<uint32_t>> NeighborWPtr;
    NeighborPtr neighborLeft{new Updatable<uint32_t>(0)};   ///< Neighbor with lesser values
    NeighborPtr neighborRight{new Updatable<uint32_t>(0)};  ///< Neighbor with higher values
    uint32_t recentAdds{0}; ///< Number of keys added to this worker recently.
    uint32_t keyCount{0};   ///< Total number of keys stored on the worker.
};


class BufferUdp;

class ProtoHelper {
public:
    static void workerKeysInfoExtractor(BufferUdp& data, uint32_t& name, NeighborsInfo& nInfo, StringRange& strRange);
};

}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_STRINGRANGE_H_

