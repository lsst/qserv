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

// Class header
#include "util/ResultFileName.h"

// Third party headers
#include "boost/filesystem.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/String.h"

using namespace std;
using namespace nlohmann;
namespace fs = boost::filesystem;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.ResultFileName");
}

namespace lsst::qserv::util {

string const ResultFileName::fileExt = ".csv";

ResultFileName::ResultFileName(qmeta::CzarId czarId, QueryId queryId, UberJobId ujId)
        : _fileName(to_string(czarId) + splitToken() + to_string(queryId) + splitToken() + to_string(ujId) +
                    fileExt),
          _czarId(czarId),
          _queryId(queryId),
          _ujId(ujId) {
    if (ujId < 0) {
        string const eMsg =
                "ResultFileName cannot be called with negative, which would be invalid for a filename " +
                _fileName;
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw std::invalid_argument(eMsg);
    }
}

ResultFileName::ResultFileName(fs::path const& filePath) : _fileName(filePath.filename().string()) {
    _parse();
}

ResultFileName::ResultFileName(string const& filePath) : _fileName(fs::path(filePath).filename().string()) {
    _parse();
}

json ResultFileName::toJson() const {
    return json::object({{"czar_id", _czarId}, {"query_id", _queryId}, {"uj_id", _ujId}});
}

bool ResultFileName::operator==(ResultFileName const& rhs) const { return _fileName == rhs._fileName; }

ostream& operator<<(ostream& os, ResultFileName const& parser) {
    os << parser.toJson();
    return os;
}

string ResultFileName::_context(string const& func) { return "FileChannelShared::ResultFileName::" + func; }

void ResultFileName::_parse() {
    fs::path const fileName = _fileName;
    string const fileNameExt = fileName.extension().string();
    if (fileNameExt != fileExt) {
        throw invalid_argument(_context(__func__) + " not a valid result file name: " + _fileName +
                               ", file ext: " + fileNameExt + ", expected: " + fileExt);
    }
    _taskAttributes = String::parseToVectUInt64(fileName.stem().string(), splitToken());
    if (_taskAttributes.size() != 3) {
        throw invalid_argument(_context(__func__) + " not a valid result file name: " + _fileName);
    }
    size_t attrIndex = 0;
    _validateAndStoreAttr(attrIndex++, "czarId", _czarId);
    _validateAndStoreAttr(attrIndex++, "queryId", _queryId);
    _validateAndStoreAttr(attrIndex++, "ujId", _ujId);
}

}  // namespace lsst::qserv::util
