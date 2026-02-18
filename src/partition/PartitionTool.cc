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

// This module's header
#include "partition/PartitionTool.h"

// System headers
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Third-party headers
#include "boost/filesystem.hpp"
#include "boost/program_options.hpp"

// Qserv headers
#include "partition/Chunker.h"
#include "partition/ChunkIndex.h"
#include "partition/ChunkReducer.h"
#include "partition/CmdLineUtils.h"
#include "partition/ConfigStore.h"
#include "partition/Csv.h"
#include "partition/ObjectIndex.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace lsst::partition {

class PartitionTool::Worker : public ChunkReducer {
public:
    Worker(ConfigStore const& config);

    /// Compute all partitioning locations of each input
    /// record and store an output record per-location.
    void map(char const* const begin, char const* const end, Silo& silo);

    static void defineOptions(po::options_description& opts);

private:
    csv::Editor _editor;
    std::pair<int, int> _pos;
    int _idField;
    int _chunkIdField;
    int _subChunkIdField;
    std::string _idFieldName;
    std::string _chunkIdFieldName;
    std::string _subChunkIdFieldName;
    Chunker _chunker;
    std::vector<ChunkLocation> _locations;
    bool _disableChunks;
    /// The cached pointer to the "director" index for object ID partitioning.
    ObjectIndex* _objectIndex = nullptr;
};

PartitionTool::Worker::Worker(ConfigStore const& config)
        : ChunkReducer(config),
          _editor(config),
          _pos(-1, -1),
          _idField(-1),
          _chunkIdField(-1),
          _subChunkIdField(-1),
          _chunker(config),
          _disableChunks(config.flag("part.disable-chunks")),
          _objectIndex(config.objectIndex1().get()) {
    if (!config.has("part.pos") && !config.has("part.id")) {
        throw std::runtime_error("Neither --part.pos not --part.id option were specified.");
    }
    FieldNameResolver fields(_editor);
    if (config.has("part.pos")) {
        std::string const s = config.get<std::string>("part.pos");
        std::pair<std::string, std::string> const p = parseFieldNamePair("part.pos", s);
        _pos.first = fields.resolve("part.pos", s, p.first);
        _pos.second = fields.resolve("part.pos", s, p.second);
    }
    if (config.has("part.id")) {
        _idFieldName = config.get<std::string>("part.id");
        _idField = fields.resolve("part.id", _idFieldName);
    }
    _chunkIdFieldName = config.get<std::string>("part.chunk");
    _chunkIdField = fields.resolve("part.chunk", _chunkIdFieldName);
    _subChunkIdFieldName = config.get<std::string>("part.sub-chunk");
    _subChunkIdField = fields.resolve("part.sub-chunk", _subChunkIdFieldName);
    // Create or open the "secondary" index (if required)
    if (_pos.first == -1) {
        // The objectID partitioning requires the input "secondary" index to exist
        std::string const url = config.get<std::string>("part.id-url");
        if (url.empty()) {
            throw std::runtime_error("Secondary index URL --part.id-url was not specified.");
        }
        _objectIndex->open(url, _editor.getInputDialect());
    } else {
        // The RA/DEC partitioning will create and populate the "secondary" index if requested
        if (_idField != -1) {
            fs::path const outDir = config.get<std::string>("out.dir");
            fs::path const indexPath =
                    outDir / (config.get<std::string>("part.prefix") + "_object_index.txt");
            _objectIndex->create(indexPath.string(), _editor, _idFieldName, _chunkIdFieldName,
                                 _subChunkIdFieldName);
        }
    }
}

void PartitionTool::Worker::map(char const* const begin, char const* const end,
                                PartitionTool::Worker::Silo& silo) {
    typedef std::vector<ChunkLocation>::const_iterator LocIter;
    std::pair<double, double> sc;
    char const* cur = begin;
    while (cur < end) {
        cur = _editor.readRecord(cur, end);
        if (_pos.first != -1) {
            // RA/DEC partitioning for the director or child tables. Allowing overlaps and
            // the "secondary" index generation (if requested).
            sc.first = _editor.get<double>(_pos.first);
            sc.second = _editor.get<double>(_pos.second);
            // Locate partitioning position and output a record for each location.
            _locations.clear();
            _chunker.locate(sc, -1, _locations);
            assert(!_locations.empty());
            for (LocIter i = _locations.begin(), e = _locations.end(); i != e; ++i) {
                _editor.set(_chunkIdField, i->chunkId);
                _editor.set(_subChunkIdField, i->subChunkId);
                if (!_disableChunks) silo.add(*i, _editor);
                // Populate the "secondary" index only for the non-overlap rows.
                if (_idField != -1 && !i->overlap) {
                    _objectIndex->write(_editor.get(_idField, true), *i);
                }
            }
        } else if (_idField != -1) {
            // The objectId partitioning mode of a child table based on an existing
            // "secondary" index for the FK to the corresponding "director" table.
            auto const chunkSubChunk = _objectIndex->read(_editor.get(_idField, true));
            int32_t const chunkId = chunkSubChunk.first;
            int32_t const subChunkId = chunkSubChunk.second;
            ChunkLocation location(chunkId, subChunkId, false);
            _editor.set(_chunkIdField, chunkId);
            _editor.set(_subChunkIdField, subChunkId);
            if (!_disableChunks) silo.add(location, _editor);
        } else {
            throw std::logic_error("Neither --part.pos not --part.id option were specified.");
        }
    }
}

void PartitionTool::Worker::defineOptions(po::options_description& opts) {
    po::options_description part("\\_______________ Partitioning", 80);
    part.add_options()("part.prefix", po::value<std::string>()->default_value("chunk"),
                       "Chunk file name prefix.");
    part.add_options()("part.chunk", po::value<std::string>(),
                       "Optional chunk ID output field name. This field name is appended "
                       "to the output field name list if it isn't already included.");
    part.add_options()("part.sub-chunk", po::value<std::string>()->default_value("subChunkId"),
                       "Sub-chunk ID output field name. This field name is appended "
                       "to the output field name list if it isn't already included.");
    part.add_options()("part.id", po::value<std::string>(),
                       "The name of a field which has an object identifier. If it's provided then"
                       "then the secondary index will be open or created.");
    part.add_options()("part.pos", po::value<std::string>(),
                       "The partitioning longitude and latitude angle field names, "
                       "separated by a comma.");
    part.add_options()("part.id-url", po::value<std::string>(),
                       "Universal resource locator for an existing secondary index.");
    part.add_options()("part.disable-chunks", po::bool_switch()->default_value(false),
                       "This flag if present would disable making chunk files in the output folder. "
                       "It's meant to run the tool in the 'dry run' mode, validating input files, "
                       "generating the objectId-to-chunk/sub-chunk index map.");
    Chunker::defineOptions(part);
    opts.add(part);
    defineOutputOptions(opts);
    csv::Editor::defineOptions(opts);
    defineInputOptions(opts);
}

PartitionTool::PartitionTool(nlohmann::json const& params, int argc, char const* const* argv) {
    char const* help =
            "The spherical partitioner partitions one or more input CSV files in\n"
            "preparation for loading into database worker nodes. This boils down to\n"
            "assigning each input position to locations in a 2-level subdivision\n"
            "scheme, where a location consists of a chunk and sub-chunk ID, and\n"
            "then bucket-sorting input records into output files by chunk ID.\n"
            "Chunk files can then be distributed to worker nodes for loading.\n"
            "\n"
            "A partitioned data-set can be built-up incrementally by running the\n"
            "partitioner with disjoint input file sets and the same output directory.\n"
            "Beware - the output CSV format, partitioning parameters, and worker\n"
            "node count MUST be identical between runs. Additionally, only one\n"
            "partitioner process should write to a given output directory at a\n"
            "time. If any of these conditions are not met, then the resulting\n"
            "chunk files will be corrupt and/or useless.\n";

    if (params.is_null() && (argc == 0 || argv == nullptr)) {
        throw std::invalid_argument("Either params or command-line arguments must be provided.");
    }
    if (argv != nullptr && argc > 0) {
        po::options_description options;
        Job<PartitionTool::Worker>::defineOptions(options);
        config = std::make_shared<ConfigStore>(parseCommandLine(options, argc, argv, help));
    }
    if (!params.is_null()) {
        if (config != nullptr) {
            config->add(params);
        } else {
            config = std::make_shared<ConfigStore>(params);
        }
    }
    ensureOutputFieldExists(*config, "part.chunk");
    ensureOutputFieldExists(*config, "part.sub-chunk");
    makeOutputDirectory(*config, true);
    Job<PartitionTool::Worker> job(*config);
    chunkIndex = job.run(makeInputLines(*config));
    if (!chunkIndex->empty()) {
        fs::path d(config->get<std::string>("out.dir"));
        fs::path f = config->get<std::string>("part.prefix") + "_index.bin";
        chunkIndex->write(d / f, false);
    }
}

}  // namespace lsst::partition
