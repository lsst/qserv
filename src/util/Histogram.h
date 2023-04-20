// -*- LSST-C++ -*-

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
#ifndef LSST_QSERV_UTIL_HISTOGRAM_H
#define LSST_QSERV_UTIL_HISTOGRAM_H

// System headers
#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <queue>
#include <sys/time.h>
#include <time.h>
#include <vector>

#include "util/Mutex.h"

// Third party headers
#include <nlohmann/json.hpp>

// qserv headers
#include "global/clock_defs.h"

namespace lsst::qserv::util {

/// This class is used to help track a value over time.
/// The `getJson()` function returns a json object that has a structure similar to this
/// {"HistogramId":"RunningTaskTimes",
///  "avg":0.002177499999999979,
///  "buckets":[
///     {"count":2,"maxVal":0.1},
///     {"count":0,"maxVal":1.0},
///     {"count":0,,"maxVal":10.0},
///     {"count":0,,"maxVal":100.0},
///     {"count":0,,"maxVal":200.0},
///     {"count":0,,"maxVal":infinity}
///    ],
///  "total":0.004354999999999958,
///  "totalCount":2
/// }
/// `HistogramId` identifies what the Histogram instance is tracking.
/// `avg` is the average value of all entries.
/// `buckets`, each bucket contains the `count` of entries with values <= `maxVal` and > the maxVal of the
/// previous bucket.
///    Where the `index` indicates the buckets position in the series.
/// `totalCount` is the number of entries.
/// `total` is the sum of all entries.
class Histogram {
public:
    using Ptr = std::shared_ptr<Histogram>;

    Histogram(std::string const& label, std::vector<double> const& bucketVals);

    /// This keeps track of the count of entries with values greater than
    /// the previous bucket's _maxVal and less than this bucket's _maxVal.
    class Bucket {
    public:
        Bucket(double maxV) : _maxVal(maxV) {}
        Bucket() = delete;
        Bucket(Bucket const&) = default;
        Bucket& operator=(Bucket const&) = default;
        ~Bucket() = default;

        /// Return the maximum value for the bucket.
        double getMaxVal() const { return _maxVal; }
        int64_t count = 0;

    private:
        double const _maxVal;
    };

    double getAvg() const;    ///< Return the average value of all current entries.
    double getTotal() const;  ///< Return the total value of all entries.

    /// Return number of entries used to make the histogram.
    uint64_t getTotalCount() const { return _totalCount; }

    /// Return the count for bucket `index`, where index=0 is the bucket for the smallest values.
    int getBucketCount(size_t index) const;

    /// Return the _maxVal for the bucket at `index`.
    double getBucketMaxVal(size_t index) const;

    /// Add a time and value to the histogram.
    /// @return If note is not empty, a log worthy string describing the data in the histogram
    ///         is returned.
    virtual std::string addEntry(TIMEPOINT stamp, double val, std::string const& note = std::string());

    /// Add a time entry using CLOCK::now() as the time stamp.
    /// @return If note is not empty, a log worthy string describing the data in the histogram
    ///         is returned.
    virtual std::string addEntry(double val, std::string const& note = std::string());

    nlohmann::json getJson() const;  ///< Return a json version of this object.

    virtual std::string getString(std::string const& note);

protected:
    /// _mtx must be locked, add an entry to the appropriate bucket.
    std::string _addEntry(TIMEPOINT stamp, double val, std::string const& note);

    /// _mtx must be locked, return the average value of all current entries.
    double _getAvg() const;

    /// _mtx must be locked, Change the counts of the appropriate bucket by `incr`.
    /// Normal values of `incr` should be -1 or 1.
    void _changeCountsBy(double val, int incr);

    /// _mtx must be locked, returns a log worthy string.
    std::string _getString(std::string const& note);

    std::string _label;            ///< String to identify what the histogram is for.
    std::vector<Bucket> _buckets;  ///< The ordered array of Buckets
    double _total = 0.0;           ///< Sum of the values used to make the Histogram
    double _lastVal = 0.0;         ///< Last value added to the histogram.
    int64_t _totalCount = 0;       ///< Total number of items used to make the Histogram.
    int64_t _overMaxCount = 0;     ///< number of entries that couldn't fit in a bucket.
    mutable VMutex _mtx;
};

/// The `Histogram` keeps a list of all entries. When there are too many entries, the oldest ones are removed
/// until there are at most `_maxSize` entries. Entries older than `_maxAge` are also removed.
class HistogramRolling : public Histogram {
public:
    using Ptr = std::shared_ptr<HistogramRolling>;

    /// This keeps track of the count of entries with values greater than
    /// the previous bucket's _maxVal and less than this bucket's _maxVal.
    class Bucket {
    public:
        Bucket(double maxV) : _maxVal(maxV) {}
        Bucket() = delete;
        Bucket(Bucket const&) = default;

        double getMaxVal() const { return _maxVal; }
        int64_t count = 0;

    private:
        double const _maxVal;
    };

    class Entry {
    public:
        Entry(TIMEPOINT stamp_, double val_) : stamp(stamp_), val(val_) {}

        TIMEPOINT const stamp;  ///< Time of the entry.
        double const val;       ///< value of the entry.
    };

    /// Constructor
    /// @param label - Name for the histogram
    /// @param bucketVals - a vector indicating the maximum value for each bucket.
    /// @param maxSize - maximum number of entries to keep. Must be greater than 0.
    /// @param maxAge - maximum age in milliseconds, such that entries older than
    ///                       it will be removed from the histogram. Default is 1 hour.
    HistogramRolling(std::string const& label, std::vector<double> const& bucketVals,
                     std::chrono::milliseconds maxAge, size_t maxSize = 1000);
    HistogramRolling() = delete;
    HistogramRolling(HistogramRolling const&) = delete;
    HistogramRolling& operator=(HistogramRolling const&) = delete;

    /// Add a time and value to the histogram.
    std::string addEntry(TIMEPOINT stamp, double val, std::string const& note = std::string()) override;

    /// Add a time entry using CLOCK::now() as the time stamp.
    std::string addEntry(double val, std::string const& note = std::string()) override;

    size_t getSize();                                  ///< Return the number of elements in _entries.
    void setMaxSize(size_t maxSize);                   ///< Set the maximum number of entries to keep.
    void setMaxAge(std::chrono::milliseconds maxAge);  ///< Set the maximum age for entries.

    std::chrono::milliseconds getMaxAge() const;  ///< Return the maximum age allowed.
    size_t getMaxSize() const;                    ///< Return the maximum number of entries allowed.
    void checkEntries();                          ///< Remove old entries or entries beyond maxSize.

private:
    /// Remove old entries, _mtx must be held when calling.
    void _checkEntries();

    size_t _maxSize;                    ///< Maximum size of _entries.
    std::chrono::milliseconds _maxAge;  ///< maximum age of an individual entry.

    std::queue<Entry> _entries;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_HISTOGRAM_H
