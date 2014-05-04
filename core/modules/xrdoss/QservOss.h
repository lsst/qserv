// -*- LSST-C++ -*-
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
// QservOss is an XrdOss implementation to be used as a cmsd ofs plugin to
// provide file stat capabilities. This implementation populates a data
// structure via lookups on a mysqld instance and uses that structure to answer
// stat() calls. In doing so, the cmsd no longer performs filesystem stat()
// calls and qserv no longer requires tools to maintain an "export directory" in
// the filesystem.

#ifndef LSST_QSERV_XRDOSS_QSERVOSS_H
#define LSST_QSERV_XRDOSS_QSERVOSS_H

// System headers
#include <deque>
#include <set>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>
#include "XrdOss/XrdOss.hh"

// Forward declarations
class XrdOss;
class XrdOucEnv;
class XrdSysLogger;
namespace lsst {
namespace qserv {
namespace wlog {
    class WLogger;
}
namespace wpublish {
    class ChunkInventory;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace xrdoss {

/// An XrdOssDF implementation that merely pays lip-service to incoming
/// directory operations. QservOss objects must return XrdOssDF (or children)
/// objects as part of their interface contract.
class FakeOssDf : public XrdOssDF {
public:
    virtual int Close(long long *retsz=0) { return XrdOssOK; }
    virtual int Opendir(const char *) { return XrdOssOK; }
    virtual int Readdir(char *buff, int blen) { return XrdOssOK; }

    // Constructor and destructor
    FakeOssDf() {}
    ~FakeOssDf() {}
};

/// QservOss is an XrdOss implementation that answers stat() based on an
/// internal data structure instead of filesystem polling. The internal data
/// structure is populated by queries on an associated mysqld instance.
class QservOss : public XrdOss {
public:
    typedef std::set<std::string> StringSet; // Convert to unordered_set (C++0x)
    static QservOss* getInstance();

    /// Reset this instance to these settings.
    QservOss* reset(XrdOss *native_oss,
                    XrdSysLogger *log,
                    const char   *cfgFn,
                    const char   *cfgParams,
                    const char   *name);

    // XrdOss overrides (relevant)
    virtual int Stat(const char* path, struct stat* buff,
                     int opts=0, XrdOucEnv*e=NULL);
    virtual int StatVS(XrdOssVSInfo *sP,
                       const char *sname=0,
                       int updt=0, XrdOucEnv*e=NULL);

    virtual int Init(XrdSysLogger* log, const char* cfgFn);

    // XrdOss overrides (stubs)
    virtual XrdOssDF *newDir(const char *tident) { return new FakeOssDf(); }
    virtual XrdOssDF *newFile(const char *tident) { return new FakeOssDf(); }
    virtual int Chmod(const char *, mode_t mode, XrdOucEnv*) { return -ENOTSUP;}
    virtual int Create(const char *, const char *, mode_t,
                       XrdOucEnv &, int opts=0) { return -ENOTSUP;}
    virtual int Mkdir(const char *, mode_t mode, int, XrdOucEnv*) {
        return -ENOTSUP;}
    virtual int Remdir(const char *, int, XrdOucEnv*) { return -ENOTSUP;}
    virtual int Truncate(const char *, unsigned long long, XrdOucEnv*) {
        return -ENOTSUP;}
    virtual int Unlink(const char *, int, XrdOucEnv*) { return -ENOTSUP;}
    virtual int Rename(const char*, const char*, XrdOucEnv*, XrdOucEnv*) {
        return -ENOTSUP;}

    void refresh();
private:
    QservOss();
    void _fillQueryFileStat(struct stat &buf);
    bool _checkExist(std::string const& db, int chunk);

    // fields (non-static)
    boost::shared_ptr<StringSet> _pathSet;
    boost::shared_ptr<wpublish::ChunkInventory> _chunkInventory;
    std::string _cfgFn;
    std::string _cfgParams;
    std::string _name;
    XrdSysLogger* _xrdSysLogger;
    boost::shared_ptr<wlog::WLogger> _log;
    time_t _initTime;
};

}}} // namespace lsst::qserv::xrdoss

#endif //  LSST_QSERV_XRDOSS_QSERVOSS_H
