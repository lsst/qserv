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

#include "partition/CmdLineUtils.h"

#include <cstdlib>
#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include "boost/algorithm/string/predicate.hpp"

#include "partition/ConfigStore.h"
#include "partition/Constants.h"
#include "partition/FileUtils.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace lsst::partition {

FieldNameResolver::~FieldNameResolver() { _editor = 0; }

int FieldNameResolver::resolve(std::string const& option, std::string const& value,
                               std::string const& fieldName, bool unique) {
    int i = _editor->getFieldIndex(fieldName);
    if (i < 0) {
        throw std::runtime_error("--" + option + "=\"" + value + "\" specifies an unrecognized field.");
    }
    if (!_fields.insert(i).second && unique) {
        throw std::runtime_error("--" + option + "=\"" + value + "\" specifies a duplicate field.");
    }
    return i;
}

ConfigStore parseCommandLine(po::options_description const& options, int argc, char const* const* argv,
                             char const* help) {
    ConfigStore config;
    // Define common options.
    po::options_description common("\\_____________________ Common", 80);
    common.add_options()("help,h", po::bool_switch()->default_value(false), "Demystify program usage.");
    common.add_options()("verbose,v", po::bool_switch()->default_value(false), "Chatty output.");
    common.add_options()("config-file,c", po::value<std::vector<std::string>>(),
                         "The name of a configuration file containing program option values "
                         "in a JSON-like format. May be specified any number of times. If an "
                         "option is specified more than once, the first specification "
                         "usually takes precedence. Command line options have the highest "
                         "precedence, followed by configuration files, which are parsed in "
                         "the order specified on the command-line and should therefore be "
                         "listed in most to least specific order. Note that the config-file "
                         "option itself is not recognized inside of a configuration file.");
    po::options_description all;
    all.add(common).add(options);
    // Parse command line. Older boost versions (1.41) require the const_cast.
    po::variables_map vm;
    po::store(po::parse_command_line(argc, const_cast<char**>(argv), all), vm);
    po::notify(vm);
    if ((vm.count("help") != 0) && vm["help"].as<bool>()) {
        std::cout << argv[0] << " [options]\n\n" << help << "\n" << all << std::endl;
        std::exit(EXIT_SUCCESS);
    }
    // Parse configuration files, if any.
    if (vm.count("config-file") != 0) {
        for (auto&& f : vm["config-file"].as<std::vector<std::string>>()) {
            config.parse(f);
        }
    }
    // Add command-line parameters to the configuration as well
    config.add(vm);
    return config;
}

namespace {
std::string const trim(std::string const& s) {
    static char const* const WS = "\t\n\r ";
    size_t i = s.find_first_not_of(WS);
    if (i == std::string::npos) {
        return std::string();
    }
    return s.substr(i, s.find_last_not_of(WS) - i + 1);
}
}  // namespace

std::pair<std::string, std::string> const parseFieldNamePair(std::string const& opt, std::string const& val) {
    std::pair<std::string, std::string> p;
    size_t i = val.find_first_of(',');
    if (i == std::string::npos) {
        throw std::runtime_error("--" + opt + "=" + val + " is not a comma separated field name pair.");
    }
    if (val.find_first_of(',', i + 1) != std::string::npos) {
        throw std::runtime_error("--" + opt + "=" + val + " is not a comma separated field name pair.");
    }
    p.first = trim(val.substr(0, i));
    p.second = trim(val.substr(i + 1));
    if (p.first.empty() || p.second.empty()) {
        throw std::runtime_error("--" + opt + "=" + val + " is not a comma separated field name pair.");
    }
    return p;
}

void defineInputOptions(po::options_description& opts) {
    po::options_description input("\\______________________ Input", 80);
    input.add_options()("in.path,i", po::value<std::vector<std::string>>(),
                        "An input file or directory name. If the name identifies a "
                        "directory, then all the files and symbolic links to files in "
                        "the directory are treated as inputs. This option must be "
                        "specified at least once.");
    opts.add(input);
}

InputLines const makeInputLines(ConfigStore const& config) {
    size_t const blockSize = config.get<size_t>("mr.block-size");
    if (blockSize < 1 || blockSize > 1024) {
        throw std::runtime_error(
                "The IO block size given by --mr.block-size "
                "must be between 1 and 1024 MiB.");
    }
    if (!config.has("in.path")) {
        throw std::runtime_error(
                "At least one input file must be provided "
                "using --in.path.");
    }
    std::vector<fs::path> paths;
    bool bIsParquetFile = false;
    for (auto&& s : config.get<std::vector<std::string>>("in.path")) {
        fs::path p(s);
        fs::file_status stat = fs::status(p);
        if (stat.type() == fs::regular_file && fs::file_size(p) > 0) {
            paths.push_back(p);
        } else if (stat.type() == fs::directory_file) {
            for (fs::directory_iterator d(p), de; d != de; ++d) {
                if (d->status().type() == fs::regular_file && fs::file_size(p) > 0) {
                    paths.push_back(d->path());
                }
            }
        }
        bIsParquetFile = (boost::algorithm::ends_with(s.c_str(), ".parquet") ||
                          boost::algorithm::ends_with(s.c_str(), ".parq"));
    }
    if (paths.empty()) {
        throw std::runtime_error(
                "No non-empty input files found among the "
                "files and directories specified via --in.path.");
    }

    // Arrow : collect parameter name list to be read from parquet file
    std::vector<std::string> names;
    if (config.has("out.csv.field")) names = config.get<std::vector<std::string>>("in.csv.field");
    // Direct parquet file reading is not possible using MT - March 2023
    if (bIsParquetFile && config.has("mr.num-workers") && config.get<int>("mr.num-workers") > 1)
        throw std::runtime_error(
                "Parquet file partition cannot be done in MT - mr.num-workers parameter must be set to 1 in "
                "parition.json file ");

    return InputLines(paths, blockSize * MiB, false, names);
}

void defineOutputOptions(po::options_description& opts) {
    po::options_description output("\\_____________________ Output", 80);
    output.add_options()("out.dir", po::value<std::string>(), "The directory to write output files to.");
    output.add_options()("out.num-nodes", po::value<uint32_t>()->default_value(1u),
                         "The number of down-stream nodes that will be using the output "
                         "files. If this is more than 1, then output files are assigned to "
                         "nodes by hashing and are placed into a sub-directory of out.dir "
                         "named node_XXXXX, where XXXXX is a logical node ID between 0 and "
                         "out.num-nodes - 1.");
    opts.add(output);
}

void makeOutputDirectory(ConfigStore& config, bool mayExist) {
    fs::path outDir;
    if (config.has("out.dir")) {
        outDir = config.get<std::string>("out.dir");
    }
    if (outDir.empty()) {
        std::cerr << "No output directory specified (use --out.dir)." << std::endl;
        std::exit(EXIT_FAILURE);
    }
    outDir = fs::system_complete(outDir);
    if (outDir.filename() == ".") {
        // fs::create_directories returns false for "does_not_exist/", even
        // when "does_not_exist" must be created. This is because the
        // trailing slash causes the last path component to be ".", which
        // exists once it is iterated to.
        outDir.remove_filename();
    }
    config.set("out.dir", outDir.string());
    if (fs::create_directories(outDir) == false && !mayExist) {
        std::cerr << "The output directory --out.dir=" << outDir.string()
                  << " already exists - please choose another." << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void ensureOutputFieldExists(ConfigStore& config, std::string const& opt) {
    if (!config.has(opt)) {
        return;
    }
    std::vector<std::string> names;
    if (!config.has("out.csv.field")) {
        if (!config.has("in.csv.field")) {
            std::cerr << "Input CSV field names not specified." << std::endl;
            std::exit(EXIT_FAILURE);
        }
        names = config.get<std::vector<std::string>>("in.csv.field");
    } else {
        names = config.get<std::vector<std::string>>("out.csv.field");
    }
    std::string const name = config.get<std::string>(opt);
    if (std::find(names.begin(), names.end(), name) == names.end()) {
        names.push_back(name);
    }
    config.set("out.csv.field", names);
}

std::vector<int32_t> const chunksToDuplicate(Chunker const& chunker, ConfigStore const& config) {
    if (config.has("chunk-id")) {
        return config.get<std::vector<int32_t>>("chunk-id");
    }
    SphericalBox region(config.get<double>("lon-min"), config.get<double>("lon-max"),
                        config.get<double>("lat-min"), config.get<double>("lat-max"));
    uint32_t node = 0;
    uint32_t numNodes = 1;
    if (config.has("out.node")) {
        node = config.get<uint32_t>("out.node");
        numNodes = config.get<uint32_t>("out.num-nodes");
        if (node >= numNodes) {
            std::runtime_error(
                    "The --out.node option value "
                    "must be less than --out.num-nodes.");
        }
    }
    return chunker.getChunksIn(region, node, numNodes);
}

}  // namespace lsst::partition
