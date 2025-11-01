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

#include "partition/InputLines.h"

#include <cstdlib>
#include <stdexcept>

#include "boost/static_assert.hpp"
#include "boost/thread.hpp"
#include "boost/algorithm/string/predicate.hpp"

#include "partition/Constants.h"
#include "partition/FileUtils.h"

namespace fs = boost::filesystem;
namespace this_thread = boost::this_thread;

namespace lsst::partition {

namespace {

typedef std::pair<char *, char *> CharPtrPair;

struct LineFragmentStorage {
    size_t size;
    char buf[MAX_LINE_SIZE];

    LineFragmentStorage(size_t sz, char *b) : size(sz) { std::memcpy(buf, b, sz); }
};

// One side of a line split in two by a block boundary.
struct LineFragment {
    LineFragmentStorage *data;

    LineFragment() : data(0) {}
    ~LineFragment() {
        delete data;
        data = 0;
    }

    // Try to store data for one side of a line split by a block boundary.
    // The first call will succeed and return NULL, in which case the
    // caller is absolved of any responsibility for the line. The second
    // call will fail and return the data stored by the first call. In this
    // case, the caller is responsible for the line.
    LineFragmentStorage *tryStore(LineFragmentStorage *newval) {
        assert(newval != 0);
#if __GNUC__ && ((__SIZEOF_POINTER__ == 4 && __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4) || \
                 (__SIZEOF_POINTER__ == 8 && __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8))
        LineFragmentStorage *oldval = 0;
        return __sync_val_compare_and_swap(&data, oldval, newval);
#else
#warning CAS not supported on this platform - falling back to locking.
        static boost::mutex m;
        boost::lock_guard<boost::mutex> lock(m);
        LineFragmentStorage *oldval = data;
        if (oldval == 0) {
            data = newval;
        }
        return oldval;
#endif
    }
};

// An input file block.
struct Block {
    std::shared_ptr<InputFile> file;
    off_t offset;
    size_t size;
    std::shared_ptr<LineFragment> head;
    std::shared_ptr<LineFragment> tail;

    Block() : file(), offset(0), size(0), head(), tail() {}

    CharPtrPair const read(char *buf, bool skipFirstLine, ConfigParamArrow const &params);
};

// Read a file block and handle the lines crossing its boundaries.
CharPtrPair const Block::read(char *buf, bool skipFirstLine, ConfigParamArrow const &configArrow) {
    // Read into buf, leaving space for a line on either side of the block.
    char *readBeg = buf + MAX_LINE_SIZE;
    char *readEnd = readBeg + size;

    // Arrow/Parquet : retrieve the real size of the arrow CSV block
    int bufferSize = 0;
    file->read(readBeg, offset, size, bufferSize, configArrow);
    if (bufferSize > 0) {
        size = bufferSize;
        readEnd = readBeg + size;
    }

    // The responsibility for returning a line which crosses the beginning
    // or end of this block lies with the last thread to encounter the
    // line.
    char *beg = readBeg;
    if (head || skipFirstLine) {
        // Scan past the first line.
        for (; beg < readEnd && *beg != '\n' && *beg != '\r'; ++beg) {
        }
        if (beg == readEnd) {
            // The first line spans the entire block. This can only happen
            // if the line is too long or for the last block in a file.
            if (tail) {
                throw std::runtime_error("Line too long.");
            }
        } else {
            ++beg;
        }
        // Skip LF in CRLF sequence and verify line length.
        if (beg < readEnd && beg[-1] == '\r' && beg[0] == '\n') {
            ++beg;
        }
        if (beg - readBeg > MAX_LINE_SIZE) {
            throw std::runtime_error("Line too long.");
        }
        if (head) {
            // This is not the first block in the enclosing file. If the
            // initial part of the first line has already been read by the
            // reader of the previous block, return the entire line in buf.
            LineFragmentStorage *right = new LineFragmentStorage(static_cast<size_t>(beg - readBeg), readBeg);
            LineFragmentStorage *left = head->tryStore(right);
            if (left != 0) {
                beg = readBeg - left->size;
                std::memcpy(beg, left->buf, left->size);
                delete right;
            }
        }
    }
    char *end = readEnd;
    if (tail) {
        // This is not the last block in the enclosing file -
        // scan to the beginning of the last line.
        for (; end > beg && end[-1] != '\n' && end[-1] != '\r'; --end) {
        }
        if (end == beg || readEnd - end > MAX_LINE_SIZE) {
            throw std::runtime_error("Line too long.");
        }
        // If the trailing part of the last line has already been read by
        // the reader of the following block, return the entire line in buf.
        LineFragmentStorage *left = new LineFragmentStorage(static_cast<size_t>(readEnd - end), end);
        LineFragmentStorage *right = tail->tryStore(left);
        if (right != 0) {
            std::memcpy(readEnd, right->buf, right->size);
            end = readEnd + right->size;
            delete left;
        }
    }
    return CharPtrPair(beg, end);
}

// Split a file into a series of blocks.
std::vector<Block> const split(fs::path const &path, off_t blockSize) {
    std::vector<Block> blocks;
    Block b;

    b.offset = 0;
    off_t fileSize = 0;
    off_t numBlocks = 0;

    if (boost::algorithm::ends_with(path.c_str(), ".parquet") ||
        boost::algorithm::ends_with(path.c_str(), ".parq")) {
        b.file = std::make_shared<InputFileArrow>(path, blockSize);

        b.size = blockSize;
        fileSize = b.file->getBatchNumber();
        numBlocks = b.file->getBatchNumber();

        blocks.reserve(numBlocks);
        for (off_t i = 0; i < numBlocks; ++i) {
            b.offset = 1;
            blocks.push_back(b);
        }
        return blocks;
    }

    b.file = std::make_shared<InputFile>(path);

    b.size = blockSize;
    fileSize = b.file->size();
    numBlocks = fileSize / blockSize;
    if (fileSize % blockSize != 0) {
        ++numBlocks;
    }

    blocks.reserve(numBlocks);
    for (off_t i = 0; i < numBlocks; ++i, b.offset += blockSize) {
        b.size = static_cast<size_t>(std::min(fileSize - b.offset, blockSize));
        b.head = b.tail;
        if (i < numBlocks - 1) {
            b.tail = std::make_shared<LineFragment>();
        } else {
            b.tail.reset();
        }
        blocks.push_back(b);
    }
    return blocks;
}

}  // unnamed namespace

class InputLines::Impl {
public:
    Impl(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine);
    Impl(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine,
         ConfigParamArrow const &config);
    ~Impl() {}

    size_t getBlockSize() const { return _blockSize; }
    size_t getMinimumBufferCapacity() const { return _blockSize + 2 * MAX_LINE_SIZE; }
    bool empty() const {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _blockCount == 0;
    }

    CharPtrPair const read(char *buf);

private:
    BOOST_STATIC_ASSERT(MAX_LINE_SIZE < 1 * MiB);

    Impl(Impl const &);
    Impl &operator=(Impl const &);

    size_t const _blockSize;
    bool const _skipFirstLine;
    ConfigParamArrow const _configArrow;

    char _pad0[CACHE_LINE_SIZE];

    boost::mutex mutable _mutex;
    size_t _blockCount;
    std::vector<Block> _queue;
    std::vector<fs::path> _paths;

    char _pad1[CACHE_LINE_SIZE];
};

InputLines::Impl::Impl(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine)
        : _blockSize(std::min(std::max(blockSize, 1 * MiB), 1 * GiB)),
          _skipFirstLine(skipFirstLine),
          _configArrow(ConfigParamArrow()),
          _mutex(),
          _blockCount(paths.size()),
          _queue(),
          _paths(paths) {}

InputLines::Impl::Impl(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine,
                       ConfigParamArrow const &config)
        : _blockSize(std::min(std::max(blockSize, 1 * MiB), 1 * GiB)),
          _skipFirstLine(skipFirstLine),
          _configArrow(config),
          _mutex(),
          _blockCount(paths.size()),
          _queue(),
          _paths(paths) {}

CharPtrPair const InputLines::Impl::read(char *buf) {
    boost::unique_lock<boost::mutex> lock(_mutex);
    while (_blockCount > 0) {
        if (!_queue.empty()) {
            // Pop the next block off the queue and read it.
            Block b = _queue.back();
            _queue.pop_back();
            --_blockCount;
            lock.unlock();  // allow block reads to proceed in parallel
            return b.read(buf, _skipFirstLine, _configArrow);
        } else if (!_paths.empty()) {
            // The queue is empty - grab the next file and split it into blocks.
            fs::path path = _paths.back();
            _paths.pop_back();
            lock.unlock();  // allow parallel file opens and splits
            std::vector<Block> v = split(path, static_cast<off_t>(_blockSize));
            // The Impl constructor initially treats files as having a
            // single block. Consume one block, and account for any
            // additional blocks generated by the split operation.
            lock.lock();
            --_blockCount;
            if (v.empty()) {
                // The input file was empty.
                continue;
            }
            Block b = v.front();
            // Insert remaining blocks in reverse order - popping them from
            // the back of the queue will yield blocks with increasing file
            // offsets.
            _queue.insert(_queue.end(), v.rbegin(), v.rend() - 1);
            _blockCount += v.size() - 1;
            lock.unlock();  // allow block reads to proceed in parallel
            return b.read(buf, _skipFirstLine, _configArrow);
        } else {
            // The queue is empty and all input paths have been processed, but
            // the block count is non-zero. This means one or more threads are
            // in the process of splitting files into blocks. Therefore, wait
            // for left-overs to appear in the queue or for the block count to
            // reach zero.
            lock.unlock();
            this_thread::yield();
            lock.lock();
        }
    }
    // All lines have been read.
    return CharPtrPair(static_cast<char *>(0), static_cast<char *>(0));
}

// Method delegation.

InputLines::InputLines(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine)
        : _impl(std::make_shared<Impl>(paths, blockSize, skipFirstLine)) {}

InputLines::InputLines(std::vector<fs::path> const &paths, size_t blockSize, bool skipFirstLine,
                       ConfigParamArrow const &configArrow)
        : _impl(std::make_shared<Impl>(paths, blockSize, skipFirstLine, configArrow)) {}

size_t InputLines::getBlockSize() const { return _impl ? _impl->getBlockSize() : 0; }

size_t InputLines::getMinimumBufferCapacity() const { return _impl ? _impl->getMinimumBufferCapacity() : 0; }

bool InputLines::empty() const { return _impl ? _impl->empty() : true; }

CharPtrPair const InputLines::read(char *buf) {
    if (_impl) {
        return _impl->read(buf);
    }
    return CharPtrPair(static_cast<char *>(0), static_cast<char *>(0));
}

}  // namespace lsst::partition
