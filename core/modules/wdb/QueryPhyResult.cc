// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "wdb/QueryPhyResult.h"

// System headers
#include <cassert>
#include <fcntl.h>
#include <iterator>
#include <sys/stat.h>

// Third-party headers

// Qserv headers
#include "sql/SqlErrorObject.h"
#include "wbase/Base.h"
#include "wbase/SendChannel.h"
#include "wconfig/Config.h"
#include "util/StringHash.h"

namespace lsst {
namespace qserv {
namespace wdb {
////////////////////////////////////////////////////////////////////////
class FileCleanup : public wbase::SendChannel::ReleaseFunc {
public:
    FileCleanup(int fd_, std::string const& filename_)
        : fd(fd_), filename(filename_) {}

    virtual void operator()() {
        ::close(fd);
        ::unlink(filename.c_str());
    }

    static std::shared_ptr<FileCleanup> newInstance(int fd,
                                               std::string const& filename) {
        return std::make_shared<FileCleanup>(fd, filename);
    }

    int fd;
    std::string filename;
};

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

std::string QueryPhyResult::_computeTmpFileName() const {
    // Should become obsolete with new result handling
    std::string defPath = "/dev/shm";
    std::ostringstream os;
    // pid, time(seconds), hash of resulttables should be unique
    pid_t pid = ::getpid();
    time_t utime = ::time(0);
    std::string tables = _getSpaceResultTables();
    std::string hash = util::StringHash::getMd5Hex(tables.data(),
                                                   tables.size());
    os.str("");
    os << defPath << "/" << pid << "_" << utime << "_" << hash;
    return os.str();
}

bool QueryPhyResult::dumpToChannel(LOG_LOGGER const& log,
                                   std::string const& user,
                                   std::shared_ptr<wbase::SendChannel> sc,
                                   sql::SqlErrorObject& errObj) {
    std::string dumpFile = _computeTmpFileName();
    if(!performMysqldump(log, user, dumpFile, errObj)) {
        return false;
    }
    int fd = ::open(dumpFile.c_str(), O_RDONLY);
    if(fd == -1) {
        errObj.setErrNo(errno);
        return errObj.addErrMsg("Couldn't open result file " + dumpFile);
    }

    struct stat s;
    if(-1 == ::fstat(fd, &s)) {
        ::close(fd);
        errObj.setErrNo(errno);
        return errObj.addErrMsg("Couldn't fstat result file " + dumpFile);
    }
    wbase::SendChannel::Size fSize = s.st_size;

    assert(sc);
    sc->setReleaseFunc(FileCleanup::newInstance(fd, dumpFile));
    if(!sc->sendFile(fd, fSize)) {
        // Error sending the result.
        // Not sure we need to do anything differently, but make sure we clean
        // things up.
    }
    return true;
}

bool QueryPhyResult::performMysqldump(LOG_LOGGER const& log,
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
    LOGF(log, LOG_LVL_INFO, "dump cmdline: %1%" % cmd);

    LOGF(log, LOG_LVL_INFO, "TIMING,000000QueryDumpStart,%1%" % ::time(NULL));
    int cmdResult = system(cmd.c_str());

    LOGF(log, LOG_LVL_INFO, "TIMING,000000QueryDumpFinish,%1%" % ::time(NULL));

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
