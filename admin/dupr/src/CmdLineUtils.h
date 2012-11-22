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
/// \brief Command-line utility functions.

#ifndef LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H
#define LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/program_options.hpp"

#include "Chunker.h"
#include "Csv.h"
#include "InputLines.h"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// Helper class for mapping field names to indexes. The `resolve` method
/// checks that a field exists, and optionally that the field name has not
/// previously been resolved to an index. This is useful when there are many
/// command line options taking field names and passing the same name to more
/// than one of them doesn't make sense.
class FieldNameResolver {
public:
     FieldNameResolver(csv::Editor const & editor) : _editor(&editor) { }
     ~FieldNameResolver();
     /// Retrieve index of `fieldName`, where `fieldName` has been extracted
     /// from the value of the given option.
     int resolve(std::string const & option,
                 std::string const & value,
                 std::string const & fieldName,
                 bool unique=true);
     /// Retrieve index of `fieldName`, where `fieldName` is the value of the
     /// given option.
     int resolve(std::string const & option,
                 std::string const & fieldName,
                 bool unique=true) {
         return resolve(option, fieldName, fieldName, unique);
     }

private:
     csv::Editor const * _editor;
     std::set<int> _fields;
};

/// Parse the given command line according to the options given and store
/// the results in `vm`. This function defines generic options `help`, `verbose`,
/// and `config-file`. It handles `help` output and configuration file parsing
/// for the caller.
void parseCommandLine(boost::program_options::variables_map & vm,
                      boost::program_options::options_description const & opts,
                      int argc,
                      char const * const * argv,
                      char const * help);

/// Parse an option value that contains a comma separated pair of field names,
/// Leading/trailing whitespace is stripped from each name, and empty names are
/// rejected.
std::pair<std::string, std::string> const parseFieldNamePair(
     std::string const & opt, std::string const & val);

/// Define the `in` option.
void defineInputOptions(boost::program_options::options_description & opts);

/// Construct an InputLines object from input files and/or directories.
InputLines const makeInputLines(boost::program_options::variables_map & vm);

/// Define the `out.dir` and `out.num-nodes` options.
void defineOutputOptions(boost::program_options::options_description & opts);

/// Handle output directory checking/creation. Assumes `defineOutputOptions()`
/// has been used.
void makeOutputDirectory(boost::program_options::variables_map & vm,
                         bool mayExist);

/// Ensure that the field name given by the option `opt` is listed as an output
/// field (in `out.csv.field`) by appending it if necessary.
void ensureOutputFieldExists(boost::program_options::variables_map & vm,
                             std::string const & opt);

/// Compute the IDs of chunks for which data must be generated, or for which
/// the record count must be estimated.
std::vector<int32_t> const chunksToDuplicate(
    Chunker const & chunker, boost::program_options::variables_map const & vm);

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H

