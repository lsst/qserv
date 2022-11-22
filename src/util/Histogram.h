// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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
#include <mutex>
#include <vector>

// Third party headers
#include <nlohmann/json.hpp>

namespace lsst::qserv::util {

/// This class is used to help track a value over time.
/// The `getJson()` function returns a json object that has a structure similar to this
///	{"HistogramId":"RunningTaskTimes",
///	 "avg":0.002177499999999979,
///	 "buckets":
///	   [{"count":2,"index":0,"maxVal":0.1},
///		{"count":0,"index":1,"maxVal":1.0},
///		{"count":0,"index":2,"maxVal":10.0},
///		{"count":0,"index":3,"maxVal":100.0},
///		{"count":0,"index":4,"maxVal":200.0},
///		{"count":0,"index":5,"maxVal":1.7976931348623157e+308}
///	   ],
///	 "size":2,
///	 "total":0.004354999999999958
///	}
/// `HistogramId` identifies what the Histogram instance is tracking.
/// `avg` is the average value of all entries.
/// `buckets`, each bucket contains the `count` of entries with values <= `maxVal` and > the maxVal of the
/// previous bucket.
///    Where the `index` indicates the buckets position in the series.
/// `size` is the number of entries.
/// `total` is the sum of all entries.
/// The `Histogram` keeps a list of all entries. When there are too many entries, the oldest ones are removed
/// until there are at most `_maxSize` entries. Entries older than `_maxAge` are also removed.
class Histogram {
public:
    using Ptr = std::shared_ptr<Histogram>;
    using CLOCK = std::chrono::system_clock;
    using TIMEPOINT = std::chrono::time_point<CLOCK>;

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
    Histogram(std::string const& label, std::vector<double> const& bucketVals,
              std::chrono::milliseconds maxAge, size_t maxSize = 1000);
    Histogram() = delete;
    Histogram(Histogram const&) = delete;
    Histogram& operator=(Histogram const&) = delete;

    /// Add a time and value to the histogram.
    std::string addEntry(TIMEPOINT stamp, double val, std::string const& note = "");

    /// Add a time entry using CLOCK::now() as the time stamp.
    std::string addEntry(double val, std::string const& note = "");

    void setMaxSize(size_t maxSize);                   ///< Set the maximum number of entries to keep.
    void setMaxAge(std::chrono::milliseconds maxAge);  ///< Set the maximum age for entries.

    /// Return the count for bucket `index`, where index=0 is the bucket for the smallest values.
    int getBucketCount(size_t index) const;

    /// Return the _maxVal for the bucket at `index`.
    double getBucketMaxVal(size_t index) const;

    double getAvg() const;                        ///< Return the average value of all current entries.
    double getTotal() const;                      ///< Return the total value of all entries.
    double getSize() const;                       ///< Return the number of entries.
    std::chrono::milliseconds getMaxAge() const;  ///< Return the maximum age allowed.
    size_t getMaxSize() const;                    ///< Return the maximum number of entries allowed.
    void checkEntries();                          ///< Remove old entries or entries beyond maxSize.
    std::string getString(std::string const& note = "");  ///< @return a log worthy version of the histogram.

    nlohmann::json getJson() const;  ///< Return a json version of this object.
    std::string getJsonStr() const;  ///< Return the json string for this instance.

private:
    /// _mtx must be locked, return the average value of all current entries.
    double _getAvg() const;

    /// _mtx must be locked, returns a log worthy string.
    std::string _getString(std::string const& note);

    /// _mtx must be locked, Change the counts of the appropriate bucket by `incr`.
    /// Normal values of `incr` should be -1 or 1.
    void _changeCountsBy(double val, int incr);

    /// Remove old entries, _mtx must be held when calling.
    void _checkEntries();

    std::string _label;
    mutable std::mutex _mtx;
    std::vector<Bucket> _buckets;
    int64_t _overMaxCount = 0;
    double _total = 0.0;
    int64_t _totalCount = 0;

    size_t _maxSize;                    ///< Maximum size of _entries.
    std::chrono::milliseconds _maxAge;  ///< maximum age of an individual entry.

    std::queue<Entry> _entries;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_HISTOGRAM_H
