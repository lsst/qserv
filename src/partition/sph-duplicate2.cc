/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
/// \brief Duplicate Object, Source and ForcedSource entries in an existing
///        partition.

#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "boost/program_options.hpp"

#include "lsst/sphgeom/LonLat.h"
#include "lsst/sphgeom/HtmPixelization.h"
#include "lsst/sphgeom/UnitVector3d.h"

#include "partition/Chunker.h"
#include "partition/Geometry.h"

namespace po = boost::program_options;
namespace sphgeom = lsst::sphgeom;
namespace part = lsst::partition;

namespace {

// ===================
// Command line parser
// ===================

class CmdLineOptions {
private:
    template <typename T>
    T _getMandatoryOption(std::string const &name, po::variables_map const &vm) {
        if (!vm.count(name)) throw new std::invalid_argument("missing command line option: " + name);

        return vm[name].as<T>();
    }

public:
    /// Trivial constructor
    CmdLineOptions() {}

    /// Destructor
    ~CmdLineOptions() {}

    /// Parse command line options and populate data members with results.
    /**
     * @return 'false' if the appplication was run in the 'hel' mode.
     */
    bool parse(int argc, char const *const *argv) {
        po::options_description desc(
                "\n"
                "DESCRIPTION\n"
                "\n"
                "  The tool will duplicate a partition by shifting Objects, Sources\n"
                "  and ForcedSources to the right along the RA dimension by the specified\n"
                "  delta.\n"
                "\n"
                "GENERAL USAGE:\n"
                "\n"
                "  [OPTIONS] [<chunk>]\n"
                "\n"
                "OPTIONS AND PARAMETERS");

        // General options
        desc.add_options()("help,h", "Print this help")("debug,d", "Print debug info");
        desc.add_options()("verbose,v", "Produce verbose output.");
        // Spatial configuration of the input
        desc.add_options()("chunk,c", po::value<uint32_t>(),
                           "Chunk identifier. The identifier may also be passed into the application "
                           "as a positional parameter.");
        desc.add_options()("part.num-stripes,s", po::value<int>()->default_value(85),
                           "The number of stripes.");
        desc.add_options()("part.num-sub-stripes,b", po::value<int>()->default_value(12),
                           "The number of sub-stripes to divide each stripe into.");
        desc.add_options()("part.overlap,p", po::value<double>()->default_value(0.01),
                           "Chunk/sub-chunk overlap radius (deg).");
        // Table schema definitions (needed to parse the input TSV files)
        desc.add_options()("coldef.object,O", po::value<std::string>(),
                           "Input file with the names of all columns of the Object table.");
        desc.add_options()("coldef.source,S", po::value<std::string>(),
                           "Input file with the names of all columns of the Source table.");
        desc.add_options()("coldef.forcedsource,F", po::value<std::string>(),
                           "Input file with the names of all columns of the ForcedSource table.");
        // Data folders
        desc.add_options()("indir,i", po::value<std::string>(), "Input folder with TSV files");
        desc.add_options()("outdir,o", po::value<std::string>(), "Output folder for modified TSV files.");
        // Parameters affecting the transformation process for the RA/DECL
        // and primary keys.
        desc.add_options()("duplicate.ra-shift,t", po::value<double>(),
                           "Shift to the right in the RA dimension (degrees)");
        desc.add_options()("duplicate.htm-subdivision-level,l", po::value<int>()->default_value(0),
                           "The number of HTM subdivision level to disambiguate Object IDs "
                           "(in the range of 9 to 13.\n"
                           "NOTE: this parameter and 'duplicate.htm-maps' are mutually exclusive");
        desc.add_options()("duplicate.htm-maps,m", po::value<std::string>(),
                           "The input folder with maps for object and source buckets "
                           "(max sub-IDs per htm8 bucket)\n");
        desc.add_options()("duplicate.store-input,D",
                           "Store input rows in the output streams as well (if 'true')");
        desc.add_options()("duplicate.force-new-keys,N",
                           "Force the new 0-based sequence of the Object IDs for both "
                           "duplicate "
                           "and input objects when option 'duplicate.store-input' is "
                           "used.\n"
                           "NOTE: this parameter and 'duplicate.htm-maps' are mutually "
                           "exclusive");
        desc.add_options()("duplicate.do-not-store,n",
                           "The 'dry run' mode - do not write output files (if "
                           "'true')");
        // Options meant to reduce the amount of generated data. May be useful
        // for debugging/verification purposes.
        desc.add_options()("max-object-rows", po::value<size_t>()->default_value(0),
                           "Read at most the specified number of input Object rows (if not 0)");
        desc.add_options()("max-source-rows", po::value<size_t>()->default_value(0),
                           "Read at most the specified number of input Source rows (if not 0)");
        desc.add_options()("max-forcedsource-rows", po::value<size_t>()->default_value(0),
                           "Read at most the specified number of input ForcedSource rows (if not 0)");
        desc.add_options()("where-object-id", po::value<uint64_t>()->default_value(0),
                           "Read all, process only  subset of rows related to that Object ID "
                           "(if not 0)");

        po::positional_options_description chunk_descr;
        chunk_descr.add("chunk", -1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(chunk_descr).run(), vm);
        po::notify(vm);

        debug = vm.count("debug") > 0;
        verbose = vm.count("verbose") > 0;

        if (vm.count("help") || !vm.count("chunk")) {
            std::cout << desc << "\n";
            return false;
        }

        chunkId = _getMandatoryOption<uint32_t>("chunk", vm);
        numStripes = _getMandatoryOption<int>("part.num-stripes", vm);
        numSubStripesPerStripe = _getMandatoryOption<int>("part.num-sub-stripes", vm);
        overlap = _getMandatoryOption<double>("part.overlap", vm);

        coldefObjectName = _getMandatoryOption<std::string>("coldef.object", vm);
        coldefSourceName = _getMandatoryOption<std::string>("coldef.source", vm);
        coldefForcedSourceName = _getMandatoryOption<std::string>("coldef.forcedsource", vm);

        indir = _getMandatoryOption<std::string>("indir", vm);
        outdir = _getMandatoryOption<std::string>("outdir", vm);

        raShift = _getMandatoryOption<double>("duplicate.ra-shift", vm);
        htmSubdivisionLevel = _getMandatoryOption<int>("duplicate.htm-subdivision-level", vm);
        if (htmSubdivisionLevel) {
            if (!(htmSubdivisionLevel >= 9) && (htmSubdivisionLevel <= part::HTM_MAX_LEVEL))
                throw new std::range_error("invalid HTM subdivision level");
            if (vm.count("duplicate.htm-maps") > 0)
                throw new std::invalid_argument(
                        "option 'duplicate.htm-maps' can't be used together with "
                        "'duplicate.htm-subdivision-level'");
        } else {
            htmMaps = _getMandatoryOption<std::string>("duplicate.htm-maps", vm);
            if (vm.count("duplicate.force-new-keys") > 0)
                throw new std::invalid_argument(
                        "option 'duplicate.htm-maps' can't be used together with 'duplicate.force-new-keys'");
        }
        storeInput = vm.count("duplicate.store-input") > 0;
        forceNewKeys = vm.count("duplicate.force-new-keys") > 0;
        dryRun = vm.count("duplicate.do-not-store") > 0;

        maxObjectRows = _getMandatoryOption<size_t>("max-object-rows", vm);
        maxSourceRows = _getMandatoryOption<size_t>("max-source-rows", vm);
        maxForcedSourceRows = _getMandatoryOption<size_t>("max-forcedsource-rows", vm);
        whereObjectId = _getMandatoryOption<uint64_t>("where-object-id", vm);

        return true;
    }

private:
    /// Copy constructor (not allowed)
    CmdLineOptions(CmdLineOptions const &);

    /// Assignment operator (ot allowed)
    CmdLineOptions &operator=(CmdLineOptions const &);

public:
    bool verbose;
    bool debug;

    uint32_t chunkId;
    int numStripes;
    int numSubStripesPerStripe;
    double overlap;

    std::string coldefObjectName;
    std::string coldefSourceName;
    std::string coldefForcedSourceName;

    std::string indir;
    std::string outdir;

    double raShift;
    int htmSubdivisionLevel;
    std::string htmMaps;
    bool storeInput;
    bool forceNewKeys;
    bool dryRun;

    size_t maxObjectRows;
    size_t maxSourceRows;
    size_t maxForcedSourceRows;
    uint64_t whereObjectId;
};

/// The parser instance
CmdLineOptions opt;

/// HtmId generator for level 20
sphgeom::HtmPixelization htmIdGen20(20);

/// Packaged sperical coordinate
struct RaDecl {
    double ra;
    double decl;
};

/// Transform RA/DECL
/**
 * Shift and wrap (if needed) over the maximum ed in each dimension.
 *
 * @return the translated coordinates
 */
RaDecl transformRaDecl(double ra, double decl, part::SphericalBox const &box) {
    RaDecl coord{ra, decl};

    coord.ra += opt.raShift;
    double const raMax4wrap = box.getLonMax() + (box.wraps() ? 360. : 0.);
    if (coord.ra >= raMax4wrap) coord.ra = box.getLonMin() + (coord.ra - raMax4wrap);

    return coord;
}

/// The generator class for issuing series of unique 64-bit identifiers
class PrimaryKeyGenerator {
private:
    /// The HTM ID map
    typedef std::map<uint32_t, uint32_t> HtmIdMap;

public:
    /// Construct the generator for the specified table
    PrimaryKeyGenerator(CmdLineOptions const &opt, std::string const &table) : _opt(opt), _table(table) {}

    ~PrimaryKeyGenerator() {}

    /// Load the keys (if required by the commad line configuration) from a file
    void load() {
        std::string const filename = _opt.htmMaps + "/" + std::to_string(_opt.chunkId) + "." + _table;

        _maxId.clear();

        std::ifstream infile(filename, std::ifstream::in);
        for (std::string line; std::getline(infile, line);) {
            std::stringstream is(line);
            uint32_t htm;
            uint32_t id;
            is >> htm;
            is >> id;
            _maxId[htm] = id;
        }
    }

    /// Allocate and return the next key in a series
    uint64_t next(uint64_t const oldId, RaDecl const &coord) {
        // Compute new ID for the shifted RA/DECL using the requested
        // algorithm.

        uint64_t newId;

        if (_opt.htmSubdivisionLevel) {
            // Increase the HTM level for the hight 32-bit part of the ID

            uint32_t const newHtmId =
                    part::htmId(part::cartesian(coord.ra, coord.decl), _opt.htmSubdivisionLevel);

            newId = newHtmId;
            newId <<= 32;

            if (_opt.forceNewKeys) {
                // The new sequence approach: use the key generator

                newId |= _nextLowerId(newHtmId);

            } else {
                // The concervative approache: copy the lower 32-bit part from the input ID

                newId |= oldId % (1UL << 32);
            }

        } else {
            // Use the Htm8 buckets as the high 32-bit and use the next available
            // 32-bit lower sub-ID in a loaded sequence for thm8 bucket.

            uint32_t const newHtmId = part::htmId(part::cartesian(coord.ra, coord.decl), 8);

            newId = newHtmId;
            newId <<= 32;
            newId |= _nextLowerId(newHtmId);
        }
        return newId;
    }

private:
    /// Allocate and return the next lower (32-bit) fraction of the key
    /**
     * The lower ID is a 32 bit number which has the following structure:
     *
     * bits: 31-18: the last 14 bits of the current chunk number
     * bits: 00-17: the last 18 bits of the local series witin the specified HTM ID
     *
     * ATTENTION: The algorithm allows chunk numbers in a range of: 0 -  16k
     *            and local series identifiers in a range of:       0 .. 256k.
     *            Any further increase in the density of objcts/sources will
     *            require increasing the HTM ID level of the upper index.
     */
    uint32_t _nextLowerId(uint32_t const htmId) {
        HtmIdMap::iterator itr = _maxId.find(htmId);
        uint32_t seriesId = 0UL;
        if (itr == _maxId.end()) {
            _maxId[htmId] = seriesId;
        } else {
            seriesId = ++(itr->second);
            if (seriesId >= 0x3FFFF)
                throw new std::out_of_range("maximum allowed limit of 256k has been reached for HTM ID: " +
                                            std::to_string(htmId) +
                                            ". Increase the HTM ID level of the Primary Key generator");
        }
        return (seriesId & 0x3FFFF) | ((_opt.chunkId & 0x3FFF) << 18);
    }

    /// Default constructor (is disabled)
    PrimaryKeyGenerator();

    /// Assignment operator (is disabled)
    PrimaryKeyGenerator &operator=(PrimaryKeyGenerator const &lhs);

private:
    CmdLineOptions const &_opt;
    std::string _table;

    HtmIdMap _maxId;
};

/// Generator instance for Objects
PrimaryKeyGenerator pkGenObject(opt, "objects");

/// Generator instance for Sources
PrimaryKeyGenerator pkGenSource(opt, "sources");

/// The base class for the column definition parsers
/**
 * Each subclasses of this class has two purposes:
 * - serving as a repository of columns (in the order they're defined by the schema)
 * - providing an zer-based index for locations of important columns in a rows
 */
class ColDef {
public:
    /// Destructor
    virtual ~ColDef() {}

    /// Load column definitions from a file
    void load(std::string const &filename) {
        std::ifstream infile(filename, std::ifstream::in);
        std::string name;
        for (int colnum = 0; std::getline(infile, name); colnum++) {
            columns.push_back(name);
            if (name.size() > maxLen) maxLen = name.size();

            this->_evaluateColumn(name, colnum);
        }
        if (!_isValid()) throw new std::range_error("ColDef file " + filename + " is not complete");
    }

protected:
    /// Default constructor
    ColDef() : maxLen(0) {}

    /// Evaluate the column
    virtual void _evaluateColumn(std::string const &name, int colnum) = 0;

    /// Validator for the definitions
    /**
     * @return 'true' if all expected columns were found in a definition files.
     */
    virtual bool _isValid() const = 0;

private:
    /// Copy constructor (is disabled)
    ColDef(ColDef const &);

    /// Assignment operator (is disabled)
    ColDef &operator=(ColDef const &lhs);

public:
    std::vector<std::string> columns;
    size_t maxLen;
};

/// Column definitions for the Object table
class ColDefObject : public ColDef {
public:
    /// Default constructor
    ColDefObject()
            : ColDef(), idxDeepSourceId(-1), idxRa(-1), idxDecl(-1), idxChunkId(-1), idxSubChunkId(-1) {}

    /// Destructor
    virtual ~ColDefObject() {}

protected:
    /// Evaluate the column
    virtual void _evaluateColumn(std::string const &name, int colnum) {
        if ("deepSourceId" == name) {
            idxDeepSourceId = colnum;
        } else if ("ra" == name) {
            idxRa = colnum;
        } else if ("decl" == name) {
            idxDecl = colnum;
        } else if ("chunkId" == name) {
            idxChunkId = colnum;
        } else if ("subChunkId" == name) {
            idxSubChunkId = colnum;
        }
    }

    /// Validator for the definitions
    virtual bool _isValid() const {
        return idxDeepSourceId * idxRa * idxDecl * idxChunkId * idxSubChunkId >= 0;
    }

private:
    /// Copy constructor (is disabled)
    ColDefObject(ColDefObject const &);

    /// Assignment operator (is disabled)
    ColDefObject &operator=(ColDefObject const &lhs);

public:
    int idxDeepSourceId;
    int idxRa;
    int idxDecl;
    int idxChunkId;
    int idxSubChunkId;
};

/// Object table's column definitions instance
ColDefObject coldefObject;

/// Column definitions for the Source table
class ColDefSource : public ColDef {
public:
    /// Default constructor
    ColDefSource()
            : ColDef(),
              idxId(-1),
              idxCoordRa(-1),
              idxCoordDecl(-1),
              idxCoordHtmId20(-1),
              idxParent(-1),
              idxObjectId(-1),
              idxClusterCoordRa(-1),
              idxClusterCoordDecl(-1) {}

    /// Destructor
    virtual ~ColDefSource() {}

protected:
    /// Evaluate the column
    virtual void _evaluateColumn(std::string const &name, int colnum) {
        if ("id" == name) {
            idxId = colnum;
        } else if ("coord_ra" == name) {
            idxCoordRa = colnum;
        } else if ("coord_decl" == name) {
            idxCoordDecl = colnum;
        } else if ("coord_htmId20" == name) {
            idxCoordHtmId20 = colnum;
        } else if ("parent" == name) {
            idxParent = colnum;
        } else if ("objectId" == name) {
            idxObjectId = colnum;
        } else if ("cluster_coord_ra" == name) {
            idxClusterCoordRa = colnum;
        } else if ("cluster_coord_decl" == name) {
            idxClusterCoordDecl = colnum;
        }
    }

    /// Validator for the definitions
    virtual bool _isValid() const {
        return idxId * idxCoordRa * idxCoordDecl * idxCoordHtmId20 * idxParent * idxObjectId *
                       idxClusterCoordRa * idxClusterCoordDecl >=
               0;
    }

private:
    /// Copy constructor (is disabled)
    ColDefSource(ColDefSource const &);

    /// Assignment operator (is disabled)
    ColDefSource &operator=(ColDefSource const &lhs);

public:
    int idxId;
    int idxCoordRa;
    int idxCoordDecl;
    int idxCoordHtmId20;
    int idxParent;
    int idxObjectId;
    int idxClusterCoordRa;
    int idxClusterCoordDecl;
};

/// Source table's column definitions instance
ColDefSource coldefSource;

class ColDefForcedSource : public ColDef {
public:
    /// Default constructor
    ColDefForcedSource() : ColDef(), idxDeepSourceId(-1), idxChunkId(-1), idxSubChunkId(-1) {}

    /// Destructor
    virtual ~ColDefForcedSource() {}

protected:
    /// Evaluate the column
    virtual void _evaluateColumn(std::string const &name, int colnum) {
        if ("deepSourceId" == name) {
            idxDeepSourceId = colnum;
        } else if ("chunkId" == name) {
            idxChunkId = colnum;
        } else if ("subChunkId" == name) {
            idxSubChunkId = colnum;
        }
    }

    /// Validator for the definitions
    virtual bool _isValid() const { return idxDeepSourceId * idxChunkId * idxSubChunkId >= 0; }

private:
    /// Copy constructor (is disabled)
    ColDefForcedSource(ColDefForcedSource const &);

    /// Assignment operator (is disabled)
    ColDefForcedSource &operator=(ColDefForcedSource const &lhs);

public:
    int idxDeepSourceId;
    int idxChunkId;
    int idxSubChunkId;
};

/// ForcedSource table's column definitions instance
ColDefForcedSource coldefForcedSource;

/// Write a row into a stream
void writeRow(std::vector<std::string> const &tokens, std::ofstream &os) {
    if (opt.dryRun) return;

    for (size_t idx = 0; idx < tokens.size(); ++idx) {
        if (idx) os << "\t";
        os << tokens[idx];
    }
    os << "\n";
}

/// The transformation map sbetween the old and new primary keys of object tables
typedef std::map<uint64_t, uint64_t> ObjectIdTransformMap;

/// Transformation table instances. One map is for the original (input) objects,
/// and the other one - for the duplicate ones.
ObjectIdTransformMap objIdTransformInput, objIdTransformDuplicate;

/// Objects which were found out-of-the partition box. THese objects
/// will not be duplicated or recorded into the output streams.
std::set<uint64_t> objIdOutOfBox;

/// Duplicate the next row of the chunk's Object table
size_t duplicateObjectRow(std::string &line, part::SphericalBox const &box, std::ofstream &os) {
    // Split the input line into tokens and store them
    // in a temporrary array at positions which are supposed to match
    // the correposnding ColDef

    std::stringstream is(line);

    std::vector<std::string> tokens(coldefObject.columns.size());

    size_t colnum = 0;

    std::string token;
    while (std::getline(is, token, '\t')) {
        if (colnum == tokens.size())
            throw new std::range_error("too many tokens in a row of the input Object file");
        tokens[colnum++] = token;
    }
    if (colnum != tokens.size())
        throw new std::range_error("too few tokens in a row of the input Object file");

    // Extract values which need to be transformed

    uint64_t deepSourceId(0);
    double ra(0.);
    double decl(0.);

    int idx = 0;
    for (std::string const token : tokens) {
        if (coldefObject.idxDeepSourceId == idx) {
            deepSourceId = boost::lexical_cast<uint64_t>(token);
        } else if (coldefObject.idxRa == idx) {
            ra = boost::lexical_cast<double>(token);
        } else if (coldefObject.idxDecl == idx) {
            decl = boost::lexical_cast<double>(token);
        }
        ++idx;
    }

    // Skip this object if the Object ID filter is enabled and
    // the ID doesn't match the filter.

    if (opt.whereObjectId && (opt.whereObjectId != deepSourceId)) return 0;

    // Skip the object if it doesn't fall into the partition. Report
    // it in the map.

    if (!box.contains(ra, decl)) {
        objIdOutOfBox.insert(deepSourceId);
        return 0;
    }

    // Compute the new Object ID for the input row if requested

    uint64_t const newInputDeepSourceId =
            opt.forceNewKeys ? pkGenObject.next(deepSourceId, RaDecl{ra, decl}) : deepSourceId;

    objIdTransformInput[deepSourceId] = newInputDeepSourceId;

    // Position transformation

    RaDecl const coord = transformRaDecl(ra, decl, box);

    // Compute new Object ID for the shifted RA/DECL using an algorithm
    // requested when invoking the application.

    uint64_t const newDeepSourceId = pkGenObject.next(deepSourceId, coord);

    if (opt.debug) {
        std::cout << "\n"
                  << "        deepSourceId: " << deepSourceId << "  " << (deepSourceId >> 32) << " "
                  << (deepSourceId % (1UL << 32)) << "\n"
                  << "newInputDeepSourceId: " << newInputDeepSourceId << "  " << (newInputDeepSourceId >> 32)
                  << " " << (newInputDeepSourceId % (1UL << 32)) << "\n"
                  << "     newDeepSourceId: " << newDeepSourceId << "  " << (newDeepSourceId >> 32) << " "
                  << (newDeepSourceId % (1UL << 32)) << "\n"
                  << "                  ra: " << boost::lexical_cast<std::string>(ra) << " -> "
                  << boost::lexical_cast<std::string>(coord.ra) << "\n"
                  << "                decl: " << boost::lexical_cast<std::string>(decl) << " -> "
                  << boost::lexical_cast<std::string>(coord.decl) << "\n";
    }
    objIdTransformDuplicate[deepSourceId] = newDeepSourceId;

    // Save the input row if requested.
    // Then update the row and store the updated row as well.

    if (opt.storeInput) {
        tokens[coldefObject.idxDeepSourceId] = boost::lexical_cast<std::string>(newInputDeepSourceId);
        tokens[coldefObject.idxChunkId] = "0";
        tokens[coldefObject.idxSubChunkId] = "0";
        writeRow(tokens, os);
    }
    tokens[coldefObject.idxDeepSourceId] = boost::lexical_cast<std::string>(newDeepSourceId);
    tokens[coldefObject.idxRa] = boost::lexical_cast<std::string>(coord.ra);
    tokens[coldefObject.idxDecl] = boost::lexical_cast<std::string>(coord.decl);
    tokens[coldefObject.idxChunkId] = "0";
    tokens[coldefObject.idxSubChunkId] = "0";

    writeRow(tokens, os);

    return opt.storeInput ? 2 : 1;
}

/// Duplicate all rows of the chunk's Object table
std::pair<size_t, size_t> duplicateObject(part::SphericalBox const &box) {
    std::string const inFileName = opt.indir + "/Object_" + std::to_string(opt.chunkId) + ".txt",
                      outFileName = opt.outdir + "/Object_" + std::to_string(opt.chunkId) + ".txt";

    size_t numProcessed = 0, numRecorded = 0;

    std::ifstream infile(inFileName, std::ifstream::in);
    std::ofstream outfile(outFileName, std::ofstream::out | std::ofstream::trunc);

    objIdTransformInput.clear();
    objIdTransformDuplicate.clear();

    for (std::string line; std::getline(infile, line);) {
        numRecorded += duplicateObjectRow(line, box, outfile);
        ++numProcessed;
        if ((opt.maxObjectRows > 0) && (numProcessed >= opt.maxObjectRows)) break;
    }
    return std::make_pair(numProcessed, numRecorded);
}

/// Duplicate the next row of the chunk's Source table
size_t duplicateSourceRow(std::string &line, part::SphericalBox const &box, std::ofstream &os) {
    // Split the input line into tokens and store them
    // in a temporrary array at positions which are supposed to match
    // the correposnding ColDef

    std::stringstream is(line);

    std::vector<std::string> tokens(coldefSource.columns.size());

    size_t colnum = 0;

    std::string token;
    while (std::getline(is, token, '\t')) {
        if (colnum == tokens.size())
            throw new std::range_error("too many tokens in a row of the input Source file");
        tokens[colnum++] = token;
    }
    if (colnum != tokens.size())
        throw new std::range_error("too few tokens in a row of the input Source file");

    // Extract values which need to be transformed

    uint64_t id(0ULL);
    double coord_ra(0.);
    double coord_decl(0.);
    uint64_t coord_htmId20(0ULL);
    uint64_t objectId(0ULL);
    double cluster_coord_ra(0.);
    double cluster_coord_decl(0.);

    int idx = 0;
    for (std::string const token : tokens) {
        if (coldefSource.idxId == idx) {
            id = boost::lexical_cast<uint64_t>(token);
        } else if (coldefSource.idxCoordRa == idx) {
            coord_ra = boost::lexical_cast<double>(token);
        } else if (coldefSource.idxCoordDecl == idx) {
            coord_decl = boost::lexical_cast<double>(token);
        } else if (coldefSource.idxCoordHtmId20 == idx) {
            coord_htmId20 = boost::lexical_cast<uint64_t>(token);
        } else if (coldefSource.idxObjectId == idx) {
            objectId = boost::lexical_cast<uint64_t>(token);
        } else if (coldefSource.idxClusterCoordRa == idx) {
            cluster_coord_ra = boost::lexical_cast<double>(token);
        } else if (coldefSource.idxClusterCoordDecl == idx) {
            cluster_coord_decl = boost::lexical_cast<double>(token);
        }
        ++idx;
    }

    // Skip this source if the Object ID filter is enabled and
    // the relevant ID doesn't match the filter.

    if (opt.whereObjectId && (opt.whereObjectId != objectId)) return 0;

    // Skip this source if its object was found outside
    // the partition's box.

    if (objIdOutOfBox.count(objectId)) return 0;

    // Compute the new Source ID for the input row if requested

    uint64_t const newInputId = opt.forceNewKeys ? pkGenSource.next(id, RaDecl{coord_ra, coord_decl}) : id;

    // Recompute the HtmId (level=20) for the input source if requested

    uint64_t const newInputCoord_htmId20 =
            opt.forceNewKeys ? htmIdGen20.index(sphgeom::UnitVector3d(
                                       sphgeom::LonLat::fromDegrees(coord_ra, coord_decl)))
                             : coord_htmId20;

    // Position transformation

    RaDecl const coord = transformRaDecl(coord_ra, coord_decl, box),
                 cluster_coord = transformRaDecl(cluster_coord_ra, cluster_coord_decl, box);

    // Compute new Source ID for the shifted RA/DECL using an algorithm
    // requested when invoking the application.

    uint64_t const newId = pkGenSource.next(id, coord);

    // Compute new HtmId (level=20) for the source

    uint64_t const newCoord_htmId20 =
            htmIdGen20.index(sphgeom::UnitVector3d(sphgeom::LonLat::fromDegrees(coord.ra, coord.decl)));

    ObjectIdTransformMap::const_iterator const itr = objIdTransformDuplicate.find(objectId);
    if (itr == objIdTransformDuplicate.end())
        throw new std::out_of_range("no replacememnt found for objectId: " + std::to_string(objectId));
    uint64_t const newObjectId = itr->second;

    if (opt.debug) {
        std::cout << "\n"
                  << "                   id: " << id << "  " << (id >> 32) << " " << (id % (1UL << 32))
                  << "\n"
                  << "           newInputId: " << newInputId << "  " << (newInputId >> 32) << " "
                  << (newInputId % (1UL << 32)) << "\n"
                  << "                newId: " << newId << "  " << (newId >> 32) << " "
                  << (newId % (1UL << 32)) << "\n"
                  << "             coord_ra: " << boost::lexical_cast<std::string>(coord_ra) << " -> "
                  << boost::lexical_cast<std::string>(coord.ra) << "\n"
                  << "           coord_decl: " << boost::lexical_cast<std::string>(coord_decl) << " -> "
                  << boost::lexical_cast<std::string>(coord.decl) << "\n"
                  << "        coord_htmId20: " << coord_htmId20 << "\n"
                  << "newInputCoord_htmId20: " << newInputCoord_htmId20 << "\n"
                  << "     newCoord_htmId20: " << newCoord_htmId20 << "\n"
                  << "             objectId: " << objectId << "  " << (objectId >> 32) << " "
                  << (objectId % (1UL << 32)) << "\n"
                  << "          newObjectId: " << newObjectId << "  " << (newObjectId >> 32) << " "
                  << (newObjectId % (1UL << 32)) << "\n"
                  << "     cluster_coord_ra: " << boost::lexical_cast<std::string>(cluster_coord_ra) << " -> "
                  << boost::lexical_cast<std::string>(cluster_coord.ra) << "\n"
                  << "   cluster_coord_decl: " << boost::lexical_cast<std::string>(cluster_coord_decl)
                  << " -> " << boost::lexical_cast<std::string>(cluster_coord.decl) << "\n";
    }

    // Save the input row if requested.
    // Then update the row and store the updated row as well.

    if (opt.storeInput) {
        tokens[coldefSource.idxId] = boost::lexical_cast<std::string>(newInputId);
        tokens[coldefSource.idxCoordHtmId20] = boost::lexical_cast<std::string>(newInputCoord_htmId20);
        tokens[coldefSource.idxObjectId] = boost::lexical_cast<std::string>(objIdTransformInput[objectId]);
        writeRow(tokens, os);
    }
    tokens[coldefSource.idxId] = boost::lexical_cast<std::string>(newId);
    tokens[coldefSource.idxCoordRa] = boost::lexical_cast<std::string>(coord.ra);
    tokens[coldefSource.idxCoordDecl] = boost::lexical_cast<std::string>(coord.decl);
    tokens[coldefSource.idxCoordHtmId20] = boost::lexical_cast<std::string>(newCoord_htmId20);
    tokens[coldefSource.idxObjectId] = boost::lexical_cast<std::string>(newObjectId);
    tokens[coldefSource.idxClusterCoordRa] = boost::lexical_cast<std::string>(cluster_coord.ra);
    tokens[coldefSource.idxClusterCoordDecl] = boost::lexical_cast<std::string>(cluster_coord.decl);

    writeRow(tokens, os);

    return opt.storeInput ? 2 : 1;
}

/// Duplicate all rows of the chunk's Source table
std::pair<size_t, size_t> duplicateSource(part::SphericalBox const &box) {
    std::string const inFileName = opt.indir + "/Source_" + std::to_string(opt.chunkId) + ".txt",
                      outFileName = opt.outdir + "/Source_" + std::to_string(opt.chunkId) + ".txt";

    size_t numProcessed(0), numRecorded(0);

    std::ifstream infile(inFileName, std::ifstream::in);
    std::ofstream outfile(outFileName, std::ofstream::out | std::ofstream::trunc);

    for (std::string line; std::getline(infile, line);) {
        numRecorded += duplicateSourceRow(line, box, outfile);
        ++numProcessed;
        if ((opt.maxSourceRows > 0) && (numProcessed >= opt.maxSourceRows)) break;
    }
    return std::make_pair(numProcessed, numRecorded);
}

/// Duplicate the next row of the chunk's ForcedSource table
size_t duplicateForcedSourceRow(std::string &line, part::SphericalBox const &box, std::ofstream &os) {
    // Split the input line into tokens and store them
    // in a temporrary array at positions which are supposed to match
    // the correposnding ColDef

    std::stringstream is(line);

    std::vector<std::string> tokens(coldefForcedSource.columns.size());

    size_t colnum = 0;

    std::string token;
    while (std::getline(is, token, '\t')) {
        if (colnum == tokens.size())
            throw new std::range_error("too many tokens in a row of the input ForcedSource file");
        tokens[colnum++] = token;
    }
    if (colnum != tokens.size())
        throw new std::range_error("too few tokens in a row of the input ForcedSource file");

    // Extract values which need to be transformed

    uint64_t deepSourceId(0ULL);

    int idx = 0;
    for (std::string const token : tokens) {
        if (coldefForcedSource.idxDeepSourceId == idx) {
            deepSourceId = boost::lexical_cast<uint64_t>(token);
        }
        ++idx;
    }

    // Skip this source if the Object ID filter is enabled and
    // the relevant ID doesn't match the filter.

    if (opt.whereObjectId && (opt.whereObjectId != deepSourceId)) return 0;

    // Skip this source if its object was found outside
    // the partition's box.

    if (objIdOutOfBox.count(deepSourceId)) return 0;

    ObjectIdTransformMap::const_iterator const itr = objIdTransformDuplicate.find(deepSourceId);
    if (itr == objIdTransformDuplicate.end())
        throw new std::out_of_range("no replacememnt found for deepSourceId: " +
                                    std::to_string(deepSourceId));
    uint64_t const newDeepSourceId = itr->second;

    if (opt.debug) {
        std::cout << "\n"
                  << "   deepSourceId: " << deepSourceId << "  " << (deepSourceId >> 32) << " "
                  << (deepSourceId % (1UL << 32)) << "\n"
                  << "newDeepSourceId: " << newDeepSourceId << "  " << (newDeepSourceId >> 32) << " "
                  << (newDeepSourceId % (1UL << 32)) << "\n";
    }

    // Save the input row if requested.
    // Then update the row and store the updated row as well.

    if (opt.storeInput) {
        tokens[coldefForcedSource.idxDeepSourceId] =
                boost::lexical_cast<std::string>(objIdTransformInput[deepSourceId]);
        tokens[coldefForcedSource.idxChunkId] = "0";
        tokens[coldefForcedSource.idxSubChunkId] = "0";
        writeRow(tokens, os);
    }
    tokens[coldefForcedSource.idxDeepSourceId] = boost::lexical_cast<std::string>(newDeepSourceId);
    tokens[coldefForcedSource.idxChunkId] = "0";
    tokens[coldefForcedSource.idxSubChunkId] = "0";

    writeRow(tokens, os);

    return opt.storeInput ? 2 : 1;
}

/// Duplicate all rows of the chunk's ForcedSource table
std::pair<size_t, size_t> duplicateForcedSource(part::SphericalBox const &box) {
    std::string const inFileName = opt.indir + "/ForcedSource_" + std::to_string(opt.chunkId) + ".txt",
                      outFileName = opt.outdir + "/ForcedSource_" + std::to_string(opt.chunkId) + ".txt";

    size_t numProcessed = 0, numRecorded = 0;

    std::ifstream infile(inFileName, std::ifstream::in);
    std::ofstream outfile(outFileName, std::ofstream::out | std::ofstream::trunc);

    for (std::string line; std::getline(infile, line);) {
        numRecorded += duplicateForcedSourceRow(line, box, outfile);
        ++numProcessed;
        if ((opt.maxForcedSourceRows > 0) && (numProcessed >= opt.maxForcedSourceRows)) break;
    }
    return std::make_pair(numProcessed, numRecorded);
}

/// Process the current chunk
void duplicate() {
    if (!opt.htmSubdivisionLevel) {
        // Preload keys into the primary keys generators of both tables
        pkGenObject.load();
        pkGenSource.load();
    }

    part::Chunker chunker(opt.overlap, opt.numStripes, opt.numSubStripesPerStripe);
    part::SphericalBox const &box(chunker.getChunkBounds(opt.chunkId));

    if (opt.verbose)
        std::cout << "\n"
                  << "Processing chunk " << opt.chunkId << "\n"
                  << "\n"
                  << "    lon.min: " << box.getLonMin() << "\n"
                  << "    lon.max: " << box.getLonMax() << "\n"
                  << "    lat.min: " << box.getLatMin() << "\n"
                  << "    lat.max: " << box.getLatMax() << "\n";

    std::pair<size_t, size_t> const objectRows = duplicateObject(box);
    if (opt.verbose)
        std::cout << "\n"
                  << "    total of " << objectRows.first << " Object rows processed, " << objectRows.second
                  << " recorded, " << objIdOutOfBox.size() << " ignored\n";

    std::pair<size_t, size_t> const sourceRows = duplicateSource(box);
    if (opt.verbose)
        std::cout << "\n"
                  << "    total of " << sourceRows.first << " Source rows processed, " << sourceRows.second
                  << " recorded\n";

    std::pair<size_t, size_t> const forcedSourceRows = duplicateForcedSource(box);
    if (opt.verbose)
        std::cout << "\n"
                  << "    total of " << forcedSourceRows.first << " ForcedSource rows processed, "
                  << forcedSourceRows.second << " recorded\n";
}
}  // namespace

int main(int argc, char const *const *argv) {
    try {
        if (!::opt.parse(argc, argv)) return EXIT_FAILURE;

        // Load table schemas (a part of it) before duplicating chunks
        ::coldefObject.load(::opt.coldefObjectName);
        ::coldefSource.load(::opt.coldefSourceName);
        ::coldefForcedSource.load(::opt.coldefForcedSourceName);

        // Process the chunk(s)
        ::duplicate();

    } catch (std::exception const &ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
