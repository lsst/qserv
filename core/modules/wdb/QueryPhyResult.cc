/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
/// QueryPhyResult instances contain and manage result table operations
#include "wdb/QueryPhyResult.h"

// System headers
#include <fcntl.h>
#include <iterator>
#include <sys/stat.h>

// Local headers
#include "sql/SqlErrorObject.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wlog/WLogger.h"


namespace lsst {
namespace qserv {
namespace wdb {            

////////////////////////////////////////////////////////////////////////
void QueryPhyResult::addResultTable(std::string const& t) {
    _resultTables.insert(t);
}

bool QueryPhyResult::hasResultTable(std::string const& t) const {
    return _resultTables.end() != _resultTables.find(t);
}

void QueryPhyResult::reset() {
    _resultTables.clear();
    _outDb.assign(std::string());
}

std::string QueryPhyResult::getCommaResultTables()  {
    std::stringstream ss;
    std::string s;
    std::copy(_resultTables.begin(), _resultTables.end(),
              std::ostream_iterator<std::string const&>(ss, ","));
    s = ss.str();
    s.erase(s.end()-1, s.end()); // drop final comma
    return s;
}

std::string QueryPhyResult::_getSpaceResultTables() const {
    std::stringstream ss;
    std::copy(_resultTables.begin(), _resultTables.end(),
              std::ostream_iterator<std::string const&>(ss, " "));
    return ss.str();
}

bool QueryPhyResult::performMysqldump(wlog::WLogger& log,
                                      std::string const& user,
                                      std::string const& dumpFile,
                                      sql::SqlErrorObject& errObj) {
    // Dump a database to a dumpfile.

    // Make sure the path exists
    _mkdirP(dumpFile);

    std::string cmd = wconfig::getConfig().getString("mysqlDump") +
        (Pformat(
            " --compact --add-locks --create-options --skip-lock-tables"
	    " --socket=%1%"
            " -u %2%"
            " --result-file=%3% %4% %5%")
         % wconfig::getConfig().getString("mysqlSocket")
         % user
         % dumpFile % _outDb
         % _getSpaceResultTables()).str();
    log.info((Pformat("dump cmdline: %1%") % cmd).str());

    log.info((Pformat("TIMING,000000QueryDumpStart,%1%")
            % ::time(NULL)).str());
    int cmdResult = system(cmd.c_str());

    log.info((Pformat("TIMING,000000QueryDumpFinish,%1%")
            % ::time(NULL)).str());

    if (cmdResult != 0) {
        errObj.setErrNo(errno);
        return errObj.addErrMsg("Unable to dump database " + _outDb
                                + " to " + dumpFile);
    }
    return true;
}

void QueryPhyResult::_mkdirP(std::string const& filePath) {
    // Quick and dirty mkdir -p functionality.  No error checking.
    std::string::size_type pos = 0;
    struct stat statbuf;
    while ((pos = filePath.find('/', pos + 1)) != std::string::npos) {
        std::string dir(filePath, 0, pos);
        if (::stat(dir.c_str(), &statbuf) == -1) {
            if (errno == ENOENT) {
                ::mkdir(dir.c_str(), 0777);
            }
        }
    }
}

}}} // namespace lsst::qserv::wdb
