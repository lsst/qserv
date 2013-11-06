/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief Map-reduce processing framework for command line applications.

#ifndef LSST_QSERV_ADMIN_DUPR_MAPREDUCE_H
#define LSST_QSERV_ADMIN_DUPR_MAPREDUCE_H

#include <sys/types.h>
#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/make_shared.hpp"
#include "boost/program_options.hpp"
#include "boost/ref.hpp"
#include "boost/scoped_array.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/thread.hpp"

#include "Constants.h"
#include "Csv.h"
#include "InputLines.h"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// A line of CSV formatted text at most MAX_LINE_SIZE characters long
/// and a key of copy-constructible and less-than comparable type K.
/// K must additionally define a hash function with signature:
///
///     uint32_t hash() const;
///
/// The size of this structure is critical, as there can be tens of millions
/// of records in memory while indexing or partitioning. If the compiler
/// bloats the instantiation with padding to satisfy alignment requirements,
/// a specialization which flattens and re-orders the members of the key
/// type into the record structure itself may be able to save space.
template <typename K>
struct Record {
    K key;
    uint32_t size;
    char * data;

    Record() : key(), size(0), data(0) { }
    explicit Record(K const & k) : key(k), size(0), data(0) { }

    /// Return a hash of the record key.
    uint32_t hash() const { return key.hash(); }

    bool operator<(Record const & r) const { return key < r.key; }
};


/// An append-only record silo.
template <typename K>
class Silo {
public:
    typedef dupr::Record<K> Record;

    // The allocation granularity. It is many times larger than the maximum
    // line size, making allocation infrequent and minimizing waste (one
    // version of `add` needs  `MAX_LINE_SIZE` free space, no matter what the
    // length of the line being added actually is). 32 bytes are subtracted so
    // that when implementation specific `malloc` bookkeeping data is accounted
    // for, the actual memory allocation size is likely to be nearly equal to a
    // multiple of the system page size.
    static size_t const ALLOC_SIZE = 8*MiB - 32;

    Silo() : _records(), _bytesUsed(0), _head(0), _cur(0), _end(0) { }
    ~Silo();

    bool empty() const { return _records.empty(); }
    size_t size() const { return _records.size();  }
    size_t getBytesUsed() const { return _bytesUsed; }

    /// Order silos by memory usage, from largest to smallest.
    bool operator<(Silo const & silo) const {
        return silo._bytesUsed < _bytesUsed;
    }

    std::vector<Record> const & getRecords() const {
        return _records;
    }

    void reserve(size_t cap) {
        _records.reserve(cap);
    }

    /// Clear the silo without deallocating memory.
    void clear();

    /// Sort the records in the silo.
    void sort() {
        std::sort(_records.begin(), _records.end());
    }

    /// Add a record to the silo, using `Editor::writeRecord()` to produce
    /// the record text. Passing in the editor allows records to be written
    /// directly to silo memory, avoiding a copy.
    void add(K const & key, csv::Editor const & editor);
    /// Add a record to the silo.
    void add(K const & key, char const * data, uint32_t size);

private:
    // Disable copy construction and assignment.
    Silo(Silo const &);
    Silo & operator=(Silo const &);

    void _grow();

    char _pad0[CACHE_LINE_SIZE];

    std::vector<Record> _records;
    size_t _bytesUsed;
    char * _head; // Head of linked allocation list.
    char * _cur;
    char * _end;  // End of current allocation.

    char _pad1[CACHE_LINE_SIZE];
};

template <typename K> Silo<K>::~Silo() {
    // Traverse linked-list, freeing each allocation. Forward
    // pointers are located at the beginning of each allocation.
    char * head = _head;
    while (head) {
        char * next = *reinterpret_cast<char **>(head);
        std::free(head);
        head = next;
    }
    _head = 0;
    _cur = 0;
    _end = 0;
}

template <typename K> void Silo<K>::clear() {
    _records.clear();
    _bytesUsed = 0;
    if (_head) {
        // Set data insertion point to the beginning of the
        // first allocation.
        _cur = _head + sizeof(char *);
        _end = _head + ALLOC_SIZE;
    }
}

template <typename K>
void Silo<K>::add(K const & key, csv::Editor const & editor) {
    char * buf = _cur;
    char * end = _end;
    if (end - buf < MAX_LINE_SIZE) {
        // The size of the line being written isn't known in advance, so
        // the silo must always present MAX_LINE_SIZE or more contiguous
        // bytes to editor. Memory waste is <1%.
        _grow();
        buf = _cur;
    }
    Record r(key);
    end = editor.writeRecord(buf);
    uint32_t sz = static_cast<uint32_t>(end - buf);
    r.size = sz;
    r.data = buf;
    _records.push_back(r);
    _bytesUsed += sz + sizeof(Record);
    _cur = end;
}

template <typename K>
void Silo<K>::add(K const & key, char const * data, uint32_t size) {
    if (size > MAX_LINE_SIZE) {
         throw std::runtime_error("Record too long.");
    }
    char * buf = _cur;
    char * end = _end;
    if (static_cast<uint32_t>(end - buf) < size) {
        _grow();
        buf = _cur;
    }
    Record r(key);
    std::memcpy(buf, data, size);
    end = buf + size;
    r.size = size;
    r.data = buf;
    _records.push_back(r);
    _bytesUsed += size + sizeof(Record);
    _cur = end;
}

template <typename K> void Silo<K>::_grow() {
    // [_cur, _end) has no room for data, so either advance to the next
    // allocation in the linked-list, or append a new allocation at the tail.
    char * tail = 0;
    char * next = 0;
    if (_end) {
        tail = _end - ALLOC_SIZE;
        next = *reinterpret_cast<char **>(tail);
    }
    if (!next) {
        next = static_cast<char *>(std::malloc(ALLOC_SIZE));
        if (!next) {
            throw std::bad_alloc();
        }
        if (tail) {
            *reinterpret_cast<char **>(tail) = next;
        }
        *reinterpret_cast<char **>(next) = 0;
        if (!_head) {
            _head = next;
        }
    }
    _cur = next + sizeof(char *);
    _end = next + ALLOC_SIZE;
}


/// Base class for map-reduce workers. This class sets up some `typedef`s
/// that workers are required to provide, and is otherwise nothing more than
/// a documentation point for the expected worker API, described below.
///
///     void map(char const * begin,
///              char const * end,
///              Silo & silo);
///
/// The `map` function is passed one or more lines of input text stored in
/// `[begin, end)` along with a silo. It is expected to transform input records
/// to output records and record keys, and to store them in the silo.
///
///     void reduce(RecordIter begin, RecordIter end);
///
/// The `reduce` function is passed ranges of records with identical keys.
/// Multiple consecutive calls may supply records with the same key.
///
/// Calls to `map` and `reduce` are performed in phases - essentially, `map`
/// is called over a pool of workers until either there is no input left or
/// there is no more memory for `map` results. At this point, the output
/// generated by `map` is consumed by having the pool of workers `reduce`
/// it, and the process repeats until the input is exhausted.
///
/// A worker implementation can assume that it is being used by a single
/// thread at a time, that no threads will be mapping while others are
/// reducing, and that no other workers will see the data its `map` and
/// `reduce` calls receive. In addition, if a worker sees a record with key
/// K, then it is guaranteed to see all records with that key (possibly over
/// multiple phases). The end of each reduce phase is signalled by calling:
///
///     void finish();
///
/// for every worker. If a worker has retained any state from `reduce` (for
/// example, if it buffered data that must eventually be written to disk),
/// then it must reset that state (e.g. flush buffers to disk) when `finish`
/// is called.
///
/// After all input has been read, mapped and reduced, each worker
/// is asked for a result via:
///
///     boost::shared_ptr<Result> const result();
///
/// The `Result` type must provide the following method:
///
///     void merge(Result const & result);
///
/// which is used to aggregate worker results into an overall job result.
/// Generally speaking, the result should correspond to some sort of
/// summary of the data seen by a particular worker. To indicate that there
/// is no result, `result` should return a NULL pointer. If the worker will
/// never have a result, `void` should be used as the result type - in this
/// case the `result()` function need not be defined at all.
///
/// A worker implementation need not be copy-constructible or assignable. It
/// must however provide a constructor taking a
/// `boost::program_options::variables_map const &`, as well as:
///
///     static void defineOptions(boost::program_options::options_description & opts)
///
/// `defineOptions` is expected to define the configuration parameters
/// that the constructor needs to build an instance from a `variables_map`.
template <typename KeyT, typename ResultT>
struct WorkerBase {
    typedef KeyT Key;
    typedef ResultT Result;
    typedef dupr::Silo<KeyT> Silo;
    typedef typename std::vector<Record<KeyT> >::const_iterator RecordIter;
};


namespace detail {

    /// Comparator for shared pointers to `Silo`s.
    template <typename K>
    struct SiloPtrCmp {
        bool operator()(boost::shared_ptr<Silo<K> > const & s,
                        boost::shared_ptr<Silo<K> > const & t) const {
            return *s < *t;
        }
    };


    /// A pair of iterators delineating a sorted range of immutable records.
    template <typename K>
    struct SortedRecordRange {
        typedef typename std::vector<Record<K> >::const_iterator RecordIter;

        RecordIter cur;
        RecordIter end;

        bool empty() const { return cur == end; }

        /// Advance until a record greater than the current one is found
        /// or no records remain.
        void advance() { cur = std::upper_bound(cur, end, *cur); }

        /// Order sorted ranges by their minimum records,
        /// from largest to smallest.
        bool operator<(SortedRecordRange const & r) const {
            return *r.cur < *cur;
        }
    };


    /// CRTP base-class containing the meat of the map-reduce implementation.
    /// Storage and aggregation of worker results is delegated to the derived
    /// class, allowing it to present a result-type dependent API.
    template <typename DerivedT, typename WorkerT>
    class JobBase {
    public:
        typedef WorkerT Worker;

        JobBase(boost::program_options::variables_map const & vm);
        ~JobBase();

        void run(InputLines input);
        void operator()();

        static void defineOptions(
            boost::program_options::options_description & opts);

    private:
        JobBase(JobBase const &);
        JobBase & operator=(JobBase const &);

        void _work();
        void _cleanup();
        void _fail(std::exception const & ex);

        typedef typename Worker::Key Key;
        typedef detail::SortedRecordRange<Key> SortedRecordRange;
        typedef typename SortedRecordRange::RecordIter RecordIter;
        typedef dupr::Silo<Key> Silo;
        typedef boost::shared_ptr<Silo> SiloPtr;
        typedef detail::SiloPtrCmp<Key> SiloPtrCmp;
        typedef typename std::vector<SiloPtr>::const_iterator SiloPtrIter;

        boost::program_options::variables_map const * _vm;

        InputLines _input;
        size_t _threshold;
        uint32_t _numWorkers;

        char _pad0[CACHE_LINE_SIZE];

        boost::mutex _mutex;
        bool _inputExhausted;
        uint32_t _numMappers;
        uint32_t _numReducers;
        std::vector<SiloPtr> _silos;
        std::vector<SiloPtr> _sorted;
        boost::condition_variable _mapCond;
        boost::condition_variable _reduceCond;
        bool _failed;
        std::string _errorMessage;

        char _pad1[CACHE_LINE_SIZE];

        // DerivedT is responsible for storing worker results. Note
        // that _mutex is locked when this is called.
        void _storeResultImpl(Worker & worker) {
            static_cast<DerivedT *>(this)->_storeResult(worker);
        }
    };

    template <typename DerivedT, typename WorkerT>
    JobBase<DerivedT, WorkerT>::JobBase(
        boost::program_options::variables_map const & vm
    ) : _vm(&vm),
        _threshold(0),
        _numWorkers(vm["mr.num-workers"].as<uint32_t>()),
        _inputExhausted(false),
        _numMappers(0),
        _numReducers(0),
        _failed(false)
    {
        if (_numWorkers < 1) {
            throw std::runtime_error("The number of worker threads given by "
                                     "--mr.num-workers must be at least 1");
        }
        size_t poolSize = vm["mr.pool-size"].as<size_t>();
        _threshold = (poolSize*MiB) / _numWorkers;
    }

    template <typename DerivedT, typename WorkerT>
    JobBase<DerivedT, WorkerT>::~JobBase() { }

    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::_cleanup() {
        _input = InputLines();
        _inputExhausted = false;
        _numMappers = 0;
        _numReducers = 0;
        _silos.clear();
        _sorted.clear();
        _failed = false;
        _errorMessage.clear();
    }

    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::_fail(std::exception const & ex) {
        boost::unique_lock<boost::mutex> lock(_mutex);
        if (!_failed) {
            // Mark job as failed, and set error message.
            _failed = true;
            _errorMessage = ex.what();
            lock.unlock();
            // Wake up any waiting threads.
            _mapCond.notify_all();
            _reduceCond.notify_all();
        }
    }

    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::run(InputLines input) {
        boost::scoped_array<boost::thread> threads(
            new boost::thread[_numWorkers - 1]);
        std::vector<SiloPtr> silos;
        silos.reserve(_numWorkers);
        for (uint32_t i = 0; i < _numWorkers; ++i) {
            silos.push_back(boost::make_shared<Silo>());
        }
        _silos.swap(silos);
        _input = input;
        // Launch threads.
        uint32_t i = 0;
        try {
            for (; i < _numWorkers - 1; ++i) {
                threads[i] = boost::thread(boost::ref(*this));
            }
        } catch (std::exception const & ex) {
            _fail(ex);
        }
        // The caller participates in job execution, avoiding thread
        // creation/join overhead in the single-threaded case.
        (*this)();
        // Wait for all launched threads to complete.
        for (uint32_t j = 0; j < i; ++j) {
            threads[j].join();
        }
        // Cleanup internal state. If any thread failed, raise an exception.
        bool failed = _failed;
        std::string msg = _errorMessage;
        _cleanup();
        if (failed) {
            throw std::runtime_error(msg);
        }
    }

    // Thread entry-point.
    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::operator()() {
        try {
            _work();
        } catch (std::exception const & ex) {
            _fail(ex);
        }
    }

    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::defineOptions(
        boost::program_options::options_description & opts)
    {
        namespace po = boost::program_options;
        po::options_description mr("\\_________________ Map-Reduce", 80);
        mr.add_options()
            ("mr.block-size", po::value<size_t>()->default_value(4),
             "The IO block size in MiB. Must be between 1 and 1024.")
            ("mr.num-workers", po::value<uint32_t>()->default_value(1),
             "The number of worker threads to use - must be at least 1.")
            ("mr.pool-size", po::value<size_t>()->default_value(1024),
             "Map-reduce memory pool size in MiB. This determines how much "
             "data will be accumulated in memory prior to data reduction / "
             "output. This is a soft limit, and so should probably not be "
             "set to more than 75% of available system memory.");
        opts.add(mr);
        WorkerT::defineOptions(opts);
    }

    // This implementation could be improved by decoupling disk reads from
    // mapping with a water-marked input block queue. This would allow the
    // number of reading threads to be kept at whatever number maximizes read
    // bandwidth, while allowing the number of processing threads to be scaled
    // such that blocks are processed at the same rate as they are read.
    template <typename DerivedT, typename WorkerT>
    void JobBase<DerivedT, WorkerT>::_work() {
        // Pre-allocate disk read buffer.
        boost::shared_ptr<char> buffer(
            static_cast<char *>(std::malloc(_input.getMinimumBufferCapacity())),
            std::free);
        if (!buffer) {
            throw std::bad_alloc();
        }
        // Pre-allocate space for sorted record ranges.
        std::vector<SortedRecordRange> ranges;
        ranges.reserve(_numWorkers);
        // BEWARE: a lock on _mutex is held while constructing Worker.
        // This is because _vm contains strings and the current GCC libstc++
        // basic_string doesn't seem to allow multiple threads to safely read
        // a shared const string concurrently without locking. Double frees
        // coming from ~basic_string (and other other heap corruptions) have
        // been observed when the Worker constructor is unprotected. See:
        //
        // http://gcc.gnu.org/bugzilla/show_bug.cgi?id=21334
        //
        // for details on what I think may be the root cause.
        boost::unique_lock<boost::mutex> lock(_mutex);
        Worker worker(*_vm);
        // Get a rank in [0, _numWorkers) for this thread.
        uint32_t const rank = _numMappers;
        ++_numMappers;
        // Enter the scheduling loop.
        while (true) {
            // -------------
            //   Map Phase
            // -------------

            while (!_silos.empty()) {
                if (_failed) { return; }
                // Grab the emptiest silo.
                std::pop_heap(_silos.begin(), _silos.end(), SiloPtrCmp());
                SiloPtr silo = _silos.back();
                _silos.pop_back();
                lock.unlock();
                std::pair<char *, char *> data = _input.read(buffer.get());
                if (data.first == 0 && data.second == 0) {
                    // No input left.
                    silo->sort();
                    lock.lock();
                    _inputExhausted = true;
                    _sorted.push_back(silo);
                    continue;
                }
                worker.map(data.first, data.second, *silo);
                if (silo->getBytesUsed() > _threshold) {
                    // silo memory usage has exceeded the threshold.
                    silo->sort();
                    lock.lock();
                    _sorted.push_back(silo);
                    continue;
                }
                lock.lock();
                _silos.push_back(silo);
                std::push_heap(_silos.begin(), _silos.end(), SiloPtrCmp());
            }
            // Wait until all mappers have finished.
            if (_failed) { return; }
            ++_numReducers;
            if (_numReducers == _numWorkers) {
                assert(_sorted.size() == _numWorkers);
                _numMappers = 0;
            } else {
                do {
                    _reduceCond.wait(lock);
                    if (_failed) { return; }
                } while (_numReducers != _numWorkers);
            }
            _reduceCond.notify_one();
            lock.unlock();

            // ----------------
            //   Reduce Phase
            // ----------------

            // Build a list of non-empty silo record ranges.
            ranges.clear();
            for (SiloPtrIter i = _sorted.begin(), e = _sorted.end();
                 i != e; ++i) {
                if ((*i)->empty()) {
                    continue;
                }
                SortedRecordRange r;
                r.cur = (*i)->getRecords().begin();
                r.end = (*i)->getRecords().end();
                ranges.push_back(r);
            }
            // Build a heap of record ranges and use merge sort to visit records
            // in order. Skip all records that do not hash to this worker.
            std::make_heap(ranges.begin(), ranges.end());
            while (!ranges.empty()) {
                std::pop_heap(ranges.begin(), ranges.end());
                SortedRecordRange * r = &ranges.back();
                RecordIter i = r->cur;
                r->advance();
                if (i->hash() % _numWorkers == rank) {
                    worker.reduce(i, r->cur);
                }
                if (r->empty()) {
                    ranges.pop_back();
                } else {
                    std::push_heap(ranges.begin(), ranges.end());
                }
            }
            worker.finish();

            lock.lock();
            if (_failed) { return; }
            // If no further input is available, store work results and exit.
            if (_inputExhausted) {
                _storeResultImpl(worker);
                break;
            }
            // Otherwise, wait until all reducers have finished, then
            // start another map phase.
            ++_numMappers;
            if (_numMappers == _numWorkers) {
                for (SiloPtrIter i = _sorted.begin(), e = _sorted.end(); i != e; ++i) {
                    (*i)->clear();
                }
                std::swap(_silos, _sorted);
                _numReducers = 0;
            } else {
                do {
                    _mapCond.wait(lock);
                    if (_failed) { return; }
                } while (_numMappers != _numWorkers);
            }
            _mapCond.notify_one();
        }
    }

    /// Job implementation for workers with results.
    template <typename WorkerT, typename ResultT>
    class JobImpl : private JobBase<JobImpl<WorkerT, ResultT>, WorkerT> {
        typedef JobBase<JobImpl<WorkerT, ResultT>, WorkerT> Base;

        void _storeResult(WorkerT & w) {
            boost::shared_ptr<ResultT> r = w.result();
            if (!_result) {
                _result = r;
            } else if (r) {
                _result->merge(*r);
            }
        }

        boost::shared_ptr<ResultT> _result;

        // Allow JobBase to call _storeResult.
        friend class JobBase<JobImpl<WorkerT, ResultT>, WorkerT>;

    public:
        explicit JobImpl(boost::program_options::variables_map const & vm) :
            Base(vm) { }

        boost::shared_ptr<ResultT> const run(InputLines input) {
            try {
                Base::run(input);
            } catch (...) {
                _result.reset();
                throw;
            }
            boost::shared_ptr<ResultT> r;
            r.swap(_result);
            return r;
        }

        using Base::defineOptions;
    };

    // Job implementation for workers without results.
    template <typename WorkerT>
    class JobImpl<WorkerT, void> : private JobBase<JobImpl<WorkerT, void>, WorkerT> {
        typedef JobBase<JobImpl<WorkerT, void>, WorkerT> Base;

        void _storeResult(WorkerT & w) { }

        // Allow JobBase to call _storeResult.
        friend class JobBase<JobImpl<WorkerT, void>, WorkerT>;

    public:
        explicit JobImpl(boost::program_options::variables_map const & vm) :
            Base(vm) { }

        void run(InputLines input) { Base::run(input); }

        using Base::defineOptions;
    };

} // namespace detail


/// This class runs a map-reduce job using a set of workers. If the worker
/// type `WorkerT` has no results (the `WorkerT::Result` typedef is `void`),
/// then the job API is:
///
///     void run(InputLines input);
///
/// Otherwise, it is:
///
///     boost::shared_ptr<typename WorkerT::Result> const run(InputLines input);
///
/// Multiple calls to `run` with different inputs are perfectly legal, and `run`
/// provides the strong exception safety guarantee, at least as far as in-memory
/// program state is concerned. If a worker class performs any writes, external
/// data integrity guarantees are the responsibility of the worker class;
/// generally speaking, none are provided.
///
/// The design is targeted at command line applications, which
/// is why job and worker classes must be constructible from a
/// `boost::program_options::variables_map`.
template <typename WorkerT>
class Job : public detail::JobImpl<WorkerT, typename WorkerT::Result> {
public:
    explicit Job(boost::program_options::variables_map const & vm) :
        detail::JobImpl<WorkerT, typename WorkerT::Result>(vm) { }
};

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_PROCESSINGFRAMEWORK_H
