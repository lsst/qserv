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
/// \brief The Qserv partitioner for match tables.
///
/// A match table M contains foreign keys into a pair of identically partitioned
/// positional tables U and V (containing e.g. objects and reference objects).
/// A match in M is assigned to a partition P if either of the positions pointed
/// to is assigned to P. If no positions in a match are separated by more than the
/// partitioning overlap radius, then a 3-way equi-join between U, M and V can
/// be decomposed into the union of 3-way joins over the set of partitions P:
///
///     (
///         SELECT ...
///         FROM Uᵨ INNER JOIN Mᵨ ON (Uᵨ.pk = Mᵨ.fkᵤ)
///                 INNER JOIN Vᵨ ON (Mᵨ.fkᵥ = Vᵨ.pk)
///         WHERE ...
///     ) UNION ALL (
///         SELECT ...
///         FROM Uᵨ INNER JOIN Mᵨ ON (Uᵨ.pk = Mᵨ.fkᵤ)
///                 INNER JOIN OVᵨ ON (Mᵨ.fkᵥ = OVᵨ.pk)
///         WHERE ...
///     )
///
/// Here, Uᵨ, Mᵨ and Vᵨ are the contents of U, M and V for partition p, and
/// OVᵨ is the subset of V \ Vᵨ within the overlap radius of Vᵨ.

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

#include "Chunker.h"
#include "ChunkIndex.h"
#include "CmdLineUtils.h"
#include "Csv.h"
#include "FileUtils.h"
#include "MapReduce.h"

using std::cerr;
using std::cout;
using std::endl;
using std::exception;
using std::find;
using std::pair;
using std::runtime_error;
using std::snprintf;
using std::string;
using std::vector;

using boost::make_shared;
using boost::shared_ptr;

namespace fs = boost::filesystem;
namespace po = boost::program_options;


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

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
    Worker(po::variables_map const & vm);

    void map(char const * const begin, char const * const end, Silo & silo);
    void reduce(RecordIter const begin, RecordIter const end);
    void finish();

    shared_ptr<ChunkIndex> const result() { return _index; }

    static void defineOptions(po::options_description & opts);

private:
    void _openFile(int32_t chunkId);

    csv::Editor _editor;
    pair<int,int> _pos1;
    pair<int,int> _pos2;
    int _chunkIdField;
    int _subChunkIdField;
    int _flagsField;
    Chunker _chunker;
    shared_ptr<ChunkIndex> _index;
    int32_t _chunkId;
    uint32_t _numNodes;
    fs::path _outputDir;
    string _prefix;
    BufferedAppender _chunk;
};

Worker::Worker(po::variables_map const & vm) :
    _editor(vm),
    _pos1(-1, -1),
    _pos2(-1, -1),
    _chunkIdField(-1),
    _subChunkIdField(-1),
    _flagsField(-1),
    _chunker(vm),
    _index(make_shared<ChunkIndex>()),
    _chunkId(-1),
    _numNodes(vm["out.num-nodes"].as<uint32_t>()),
    _outputDir(vm["out.dir"].as<string>().c_str()),  // defend against GCC PR21334
    _prefix(vm["part.prefix"].as<string>().c_str()), // defend against GCC PR21334
    _chunk(vm["mr.block-size"].as<size_t>()*MiB)
{
    if (_numNodes == 0 || _numNodes > 99999u) {
        throw runtime_error("The --out.num-nodes option value must be "
                            "between 1 and 99999.");
    }
    // Map field names of interest to field indexes.
    if (vm.count("part.pos1") == 0 || vm.count("part.pos2") == 0) {
        throw runtime_error("The --part.pos1 and/or --part.pos2 "
                            "option was not specified.");
    }
    FieldNameResolver fields(_editor);
    string s = vm["part.pos1"].as<string>();
    pair<string,string> p = parseFieldNamePair("part.pos1", s);
    _pos1.first = fields.resolve("part.pos1", s, p.first);
    _pos1.second = fields.resolve("part.pos1", s, p.second);
    s = vm["part.pos2"].as<string>();
    p = parseFieldNamePair("part.pos2", s);
    _pos2.first = fields.resolve("part.pos2", s, p.first);
    _pos2.second = fields.resolve("part.pos2", s, p.second);
    if (vm.count("part.chunk") != 0) {
        s = vm["part.chunk"].as<string>();
        _chunkIdField = fields.resolve("part.chunk", s);
    }
    s = vm["part.sub-chunk"].as<string>();
    _subChunkIdField = fields.resolve("part.sub-chunk", s);
    s = vm["part.flags"].as<string>();
    _flagsField = fields.resolve("part.flags", s);
}

void Worker::map(char const * const begin,
                 char const * const end,
                 Worker::Silo & silo)
{
    pair<double, double> sc1, sc2;
    ChunkLocation loc1, loc2;
    char const * cur = begin;
    while (cur < end) {
        cur = _editor.readRecord(cur, end);
        bool null1 = _editor.isNull(_pos1.first) || _editor.isNull(_pos1.second);
        bool null2 = _editor.isNull(_pos2.first) || _editor.isNull(_pos2.second);
        if (null1 && null2) {
            throw runtime_error("Both partitioning positions in a match "
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
                    throw runtime_error("Partitioning positions in match record "
                                        "are separated by more than the overlap "
                                        "radius.");
                }
                if (loc1.chunkId == loc2.chunkId &&
                    loc1.subChunkId == loc2.subChunkId) {
                    // Both positions are in the same partitioning location.
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

void Worker::reduce(Worker::RecordIter const begin,
                    Worker::RecordIter const end) {
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

void Worker::defineOptions(po::options_description & opts) {
    po::options_description part("\\_______________ Partitioning", 80);
    part.add_options()
        ("part.prefix", po::value<string>()->default_value("chunk"),
         "Chunk file name prefix.")
        ("part.chunk", po::value<string>(),
         "Optional chunk ID output field name. This field name is appended "
         "to the output field name list if it isn't already included.")
        ("part.sub-chunk", po::value<string>()->default_value("subChunkId"),
         "Sub-chunk ID output field name. This field field name is appended "
         "to the output field name list if it isn't already included.")
        ("part.pos1", po::value<string>(),
         "The partitioning longitude and latitude angle field names of the "
         "first matched entity, separated by a comma.")
        ("part.pos2", po::value<string>(),
         "The partitioning longitude and latitude angle field names of the "
         "second matched entity, separated by a comma.")
        ("part.flags", po::value<string>()->default_value("partitioningFlags"),
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
        snprintf(subdir, sizeof(subdir), "node_%05lu",
                 static_cast<unsigned long>(node));
        p = p / subdir;
        fs::create_directory(p);
    }
    char suffix[32];
    snprintf(suffix, sizeof(suffix), "_%ld.txt", static_cast<long>(chunkId));
    _chunk.open(p / (_prefix + suffix), false);
}


typedef Job<Worker> PartitionMatchesJob;

}}}} // namespace lsst::qserv::admin::dupr


static char const * help =
    "The Qserv match partitioner partitions one or more input CSV files in\n"
    "preparation for loading by Qserv worker nodes. This involves assigning\n"
    "both positions in a match pair to a location in a 2-level subdivision\n"
    "scheme, where a location consists of a chunk and sub-chunk ID, and\n"
    "outputting the match pair once for each distinct location. Match pairs\n"
    "are bucket-sorted by chunk ID, resulting in chunk files that can then\n"
    "be distributed to Qserv worker nodes for loading.\n"
    "\n"
    "A partitioned data-set can be built-up incrementally by running the\n"
    "partitioner with disjoint input file sets and the same output directory.\n"
    "Beware - the output CSV format, partitioning parameters, and worker\n"
    "node count MUST be identical between runs. Additionally, only one\n"
    "partitioner process should write to a given output directory at a\n"
    "time. If any of these conditions are not met, then the resulting\n"
    "chunk files will be corrupt and/or useless.\n";

int main(int argc, char const * const * argv) {
    namespace dupr = lsst::qserv::admin::dupr;
    try {
        po::options_description options;
        dupr::PartitionMatchesJob::defineOptions(options);
        po::variables_map vm;
        dupr::parseCommandLine(vm, options, argc, argv, help);
        dupr::ensureOutputFieldExists(vm, "part.chunk");
        dupr::ensureOutputFieldExists(vm, "part.sub-chunk");
        dupr::ensureOutputFieldExists(vm, "part.flags");
        dupr::makeOutputDirectory(vm, true);
        dupr::PartitionMatchesJob job(vm);
        shared_ptr<dupr::ChunkIndex> index = job.run(dupr::makeInputLines(vm));
        if (!index->empty()) {
            fs::path d(vm["out.dir"].as<string>());
            fs::path f = vm["part.prefix"].as<string>() + "_index.bin";
            index->write(d / f, false);
        }
        if (vm.count("verbose") != 0) {
            index->write(cout, 0);
            cout << endl;
        } else {
            cout << *index << endl;
        }
    } catch (exception const & ex) {
        cerr << ex.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// FIXME(smm): The partitioner should store essential parameters so that
//             it can detect whether the same ones are used by incremental
//             additions to a partitioned data-set.

