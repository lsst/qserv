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

#include "partition/ChunkReducer.h"

#include <cstdio>
#include <stdexcept>
#include <string>

#include "boost/make_shared.hpp"

#include "partition/ConfigStore.h"
#include "partition/Hash.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace lsst::partition {

ChunkReducer::ChunkReducer(ConfigStore const& config)
        : _index(boost::make_shared<ChunkIndex>()),
          _chunkId(-1),
          _numNodes(config.get<uint32_t>("out.num-nodes")),
          _prefix(config.get<std::string>("part.prefix").c_str()),  // defend against GCC PR21334
          _outputDir(config.get<std::string>("out.dir").c_str()),   // defend against GCC PR21334
          _chunkAppender(config.get<size_t>("mr.block-size") * MiB),
          _overlapChunkAppender(config.get<size_t>("mr.block-size") * MiB) {
    if (_numNodes == 0 || _numNodes > 99999u) {
        throw std::runtime_error(
                "The --out.num-nodes option value must be "
                "between 1 and 99999.");
    }
}

void ChunkReducer::reduce(ChunkReducer::RecordIter const begin, ChunkReducer::RecordIter const end) {
    if (begin == end) {
        return;
    }
    int32_t const chunkId = begin->key.chunkId;
    if (chunkId != _chunkId) {
        finish();
        _chunkId = chunkId;
        _makeFilePaths(chunkId);
    }
    // Store records and update statistics. Files are only created/
    // opened if there is data to write to them.
    for (RecordIter cur = begin; cur != end; ++cur) {
        _index->add(cur->key);
        if (cur->key.overlap) {
            if (!_overlapChunkAppender.isOpen()) {
                _overlapChunkAppender.open(_overlapChunkPath, false);
            }
            _overlapChunkAppender.append(cur->data, cur->size);
        } else {
            if (!_chunkAppender.isOpen()) {
                _chunkAppender.open(_chunkPath, false);
            }
            _chunkAppender.append(cur->data, cur->size);
        }
    }
}

void ChunkReducer::finish() {
    // Reset current chunk ID to an invalid ID and close all output files.
    _chunkId = -1;
    _chunkAppender.close();
    _overlapChunkAppender.close();
}

void ChunkReducer::_makeFilePaths(int32_t chunkId) {
    fs::path p = _outputDir;
    if (_numNodes > 1) {
        // Files go into a node-specific sub-directory.
        char subdir[32];
        uint32_t node = hash(static_cast<uint32_t>(chunkId)) % _numNodes;
        std::snprintf(subdir, sizeof(subdir), "node_%05lu", static_cast<unsigned long>(node));
        p = p / subdir;
        fs::create_directory(p);
    }
    char suffix[32];
    std::snprintf(suffix, sizeof(suffix), "_%ld.txt", static_cast<long>(chunkId));
    _chunkPath = p / (_prefix + suffix);
    std::snprintf(suffix, sizeof(suffix), "_%ld_overlap.txt", static_cast<long>(chunkId));
    _overlapChunkPath = p / (_prefix + suffix);
}

}  // namespace lsst::partition
