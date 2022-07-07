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
/// \brief The partitioner for match tables.
///
/// A match table M contains foreign keys into a pair of identically partitioned
/// positional tables U and V (containing e.g. objects and reference objects).
/// A match in M is assigned to a chunk C if either of the positions pointed
/// to is assigned to C. If no positions in a match are separated by more than the
/// partitioning overlap radius, then a 3-way equi-join between U, M and V can
/// be decomposed into the union of 3-way joins over the set of sub-chunks:
///
///     (
///         SELECT ...
///         FROM Uᵢ INNER JOIN Mᵨ ON (Uᵢ.pk = Mᵨ.fkᵤ)
///                 INNER JOIN Vᵢ ON (Mᵨ.fkᵥ = Vᵢ.pk)
///         WHERE ...
///     ) UNION ALL (
///         SELECT ...
///         FROM Uᵢ INNER JOIN Mᵨ ON (Uᵢ.pk = Mᵨ.fkᵤ)
///                 INNER JOIN OVᵢ ON (Mᵨ.fkᵥ = OVᵢ.pk)
///         WHERE ...
///     )
///
/// Here, Uᵢ and Vᵢ contain the rows of U and V in sub-chunk i of chunk p,
/// Mᵨ contains the rows of M in chunk p, and OVᵢ is the subset of V \ Vᵢ
/// within the overlap radius of Vᵢ.

#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/make_shared.hpp"
#include "boost/program_options.hpp"
#include "boost/shared_ptr.hpp"

#include "partition/Chunker.h"
#include "partition/ChunkIndex.h"
#include "partition/CmdLineUtils.h"
#include "partition/ConfigStore.h"
#include "partition/Csv.h"
#include "partition/FileUtils.h"
#include "partition/MapReduce.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace lsst::partition {

/// Map-reduce worker class for partitioning spatial match pairs.
///
/// The `map` function computes the non-overlap location of both
/// positions in each match record, and stores the match in both
/// locations.
///
/// The `reduce` function saves output records to files, each containing
/// data for a single chunk ID. Each chunk ID is assigned to a down-stream
/// node by hashing, and the corresponding output files are created in a
/// node specific sub-directory of the output directory.
///
/// A worker's result is a ChunkIndex object that contains the total
/// record count for each chunk and sub-chunk seen by that worker.
class Worker : public WorkerBase<ChunkLocation, ChunkIndex> {
public:
    Worker(ConfigStore const& config);

    void map(char const* const begin, char const* const end, Silo& silo);
    void reduce(RecordIter const begin, RecordIter const end);
    void finish();

    boost::shared_ptr<ChunkIndex> const result() { return _index; }

    static void defineOptions(po::options_description& opts);

private:
    void _openFile(int32_t chunkId);

    csv::Editor _editor;
    std::pair<int, int> _pos1;
    std::pair<int, int> _pos2;
    int _chunkIdField;
    int _subChunkIdField;
    int _flagsField;
    Chunker _chunker;
    boost::shared_ptr<ChunkIndex> _index;
    int32_t _chunkId;
    uint32_t _numNodes;
    fs::path _outputDir;
    std::string _prefix;
    BufferedAppender _chunk;
};

Worker::Worker(ConfigStore const& config)
        : _editor(config),
          _pos1(-1, -1),
          _pos2(-1, -1),
          _chunkIdField(-1),
          _subChunkIdField(-1),
          _flagsField(-1),
          _chunker(config),
          _index(boost::make_shared<ChunkIndex>()),
          _chunkId(-1),
          _numNodes(config.get<uint32_t>("out.num-nodes")),
          _outputDir(config.get<std::string>("out.dir").c_str()),   // defend against GCC PR21334
          _prefix(config.get<std::string>("part.prefix").c_str()),  // defend against GCC PR21334
          _chunk(config.get<size_t>("mr.block-size") * MiB) {
    if (_numNodes == 0 || _numNodes > 99999u) {
        throw std::runtime_error(
                "The --out.num-nodes option value must be "
                "between 1 and 99999.");
    }
    // Map field names of interest to field indexes.
    if (!config.has("part.pos1") || !config.has("part.pos2")) {
        throw std::runtime_error(
                "The --part.pos1 and/or --part.pos2 "
                "option was not specified.");
    }
    FieldNameResolver fields(_editor);
    std::string s = config.get<std::string>("part.pos1");
    std::pair<std::string, std::string> p = parseFieldNamePair("part.pos1", s);
    _pos1.first = fields.resolve("part.pos1", s, p.first);
    _pos1.second = fields.resolve("part.pos1", s, p.second);
    s = config.get<std::string>("part.pos2");
    p = parseFieldNamePair("part.pos2", s);
    _pos2.first = fields.resolve("part.pos2", s, p.first);
    _pos2.second = fields.resolve("part.pos2", s, p.second);
    if (config.has("part.chunk")) {
        s = config.get<std::string>("part.chunk");
        _chunkIdField = fields.resolve("part.chunk", s);
    }
    s = config.get<std::string>("part.sub-chunk");
    _subChunkIdField = fields.resolve("part.sub-chunk", s);
    s = config.get<std::string>("part.flags");
    _flagsField = fields.resolve("part.flags", s);
}

void Worker::map(char const* const begin, char const* const end, Worker::Silo& silo) {
    std::pair<double, double> sc1, sc2;
    ChunkLocation loc1, loc2;
    char const* cur = begin;
    while (cur < end) {
        cur = _editor.readRecord(cur, end);
        bool null1 = _editor.isNull(_pos1.first) || _editor.isNull(_pos1.second);
        bool null2 = _editor.isNull(_pos2.first) || _editor.isNull(_pos2.second);
        if (null1 && null2) {
            throw std::runtime_error(
                    "Both partitioning positions in a match "
                    "record contain NULLs.");
        }
        if (!null1) {
            sc1.first = _editor.get<double>(_pos1.first);
            sc1.second = _editor.get<double>(_pos1.second);
            loc1 = _chunker.locate(sc1);
        }
        if (!null2) {
            sc2.first = _editor.get<double>(_pos2.first);
            sc2.second = _editor.get<double>(_pos2.second);
            loc2 = _chunker.locate(sc2);
        }
        if (!null1) {
            _editor.set(_chunkIdField, loc1.chunkId);
            _editor.set(_subChunkIdField, loc1.subChunkId);
            if (!null2) {
                // Both positions are valid.
                if (angSep(cartesian(sc1), cartesian(sc2)) * DEG_PER_RAD >
                    _chunker.getOverlap() - EPSILON_DEG) {
                    throw std::runtime_error(
                            "Partitioning positions in match "
                            "record are separated by more "
                            "than the overlap radius.");
                }
                if (loc1.chunkId == loc2.chunkId) {
                    // Both positions are in the same chunk.
                    _editor.set(_flagsField, '3');
                    silo.add(loc1, _editor);
                    continue;
                }
            }
            _editor.set(_flagsField, '1');
            silo.add(loc1, _editor);
        }
        if (!null2) {
            _editor.set(_chunkIdField, loc2.chunkId);
            _editor.set(_subChunkIdField, loc2.subChunkId);
            _editor.set(_flagsField, '2');
            silo.add(loc2, _editor);
        }
    }
}

void Worker::reduce(Worker::RecordIter const begin, Worker::RecordIter const end) {
    if (begin == end) {
        return;
    }
    int32_t const chunkId = begin->key.chunkId;
    if (chunkId != _chunkId) {
        _chunkId = chunkId;
        _openFile(chunkId);
    }
    for (RecordIter cur = begin; cur != end; ++cur) {
        _index->add(cur->key);
        _chunk.append(cur->data, cur->size);
    }
}

void Worker::finish() {
    _chunkId = -1;
    _chunk.close();
}

void Worker::defineOptions(po::options_description& opts) {
    po::options_description part("\\_______________ Partitioning", 80);
    part.add_options()("part.prefix", po::value<std::string>()->default_value("chunk"),
                       "Chunk file name prefix.");
    part.add_options()("part.chunk", po::value<std::string>(),
                       "Optional chunk ID output field name. This field name is appended "
                       "to the output field name list if it isn't already included.");
    part.add_options()("part.sub-chunk", po::value<std::string>()->default_value("subChunkId"),
                       "Sub-chunk ID output field name. This field field name is appended "
                       "to the output field name list if it isn't already included.");
    part.add_options()("part.pos1", po::value<std::string>(),
                       "The partitioning longitude and latitude angle field names of the "
                       "first matched entity, separated by a comma.");
    part.add_options()("part.pos2", po::value<std::string>(),
                       "The partitioning longitude and latitude angle field names of the "
                       "second matched entity, separated by a comma.");
    part.add_options()("part.flags", po::value<std::string>()->default_value("partitioningFlags"),
                       "The partitioning flags output field name. Bit 0, the LSB of the "
                       "field value, is set if the partition of the first entity in the "
                       "match is equal to the partition of the match pair. Likewise, bit "
                       "1 is set if the partition of the second entity is equal to the "
                       "partition of the match pair. This field name is appended to the "
                       "output field name list if it isn't already included.");
    Chunker::defineOptions(part);
    opts.add(part);
    defineOutputOptions(opts);
    csv::Editor::defineOptions(opts);
    defineInputOptions(opts);
}

void Worker::_openFile(int32_t chunkId) {
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
    _chunk.open(p / (_prefix + suffix), false);
}

typedef Job<Worker> PartitionMatchesJob;

}  // namespace lsst::partition

static char const* help =
        "The match partitioner partitions one or more input CSV files in\n"
        "preparation for loading by database worker nodes. This involves assigning\n"
        "both positions in a match pair to a location in a 2-level subdivision\n"
        "scheme, where a location consists of a chunk and sub-chunk ID, and\n"
        "outputting the match pair once for each distinct location. Match pairs\n"
        "are bucket-sorted by chunk ID, resulting in chunk files that can then\n"
        "be distributed to worker nodes for loading.\n"
        "\n"
        "A partitioned data-set can be built-up incrementally by running the\n"
        "partitioner with disjoint input file sets and the same output directory.\n"
        "Beware - the output CSV format, partitioning parameters, and worker\n"
        "node count MUST be identical between runs. Additionally, only one\n"
        "partitioner process should write to a given output directory at a\n"
        "time. If any of these conditions are not met, then the resulting\n"
        "chunk files will be corrupt and/or useless.\n";

int main(int argc, char const* const* argv) {
    namespace part = lsst::partition;
    try {
        po::options_description options;
        part::PartitionMatchesJob::defineOptions(options);
        part::ConfigStore config = part::parseCommandLine(options, argc, argv, help);
        part::ensureOutputFieldExists(config, "part.chunk");
        part::ensureOutputFieldExists(config, "part.sub-chunk");
        part::ensureOutputFieldExists(config, "part.flags");
        part::makeOutputDirectory(config, true);
        part::PartitionMatchesJob job(config);
        boost::shared_ptr<part::ChunkIndex> index = job.run(part::makeInputLines(config));
        if (!index->empty()) {
            fs::path d(config.get<std::string>("out.dir"));
            fs::path f = config.get<std::string>("part.prefix") + "_index.bin";
            index->write(d / f, false);
        }
        if (config.flag("verbose")) {
            index->write(std::cout, 0);
            std::cout << std::endl;
        } else {
            std::cout << *index << std::endl;
        }
    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// FIXME(smm): The partitioner should store essential parameters so that
//             it can detect whether the same ones are used by incremental
//             additions to a partitioned data-set.
