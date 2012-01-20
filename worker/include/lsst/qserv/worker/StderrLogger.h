/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
//  class StderrLogger -- A basic logger class that outputs to stderr
//  as a stand-in for non-xrootd environments.
#ifndef LSST_QSERV_WORKER_STDERRLOGGER_H
#define LSST_QSERV_WORKER_STDERRLOGGER_H
#include <iostream>

namespace lsst {
namespace qserv {
namespace worker {

class StderrLogger : public Logger {
public:
    typedef boost::shared_ptr<StderrLogger> Ptr;
    virtual ~StderrLogger() {}
    virtual void operator()(std::string const& s) {
        std::cerr << "LOG: " << s << std::endl;
    }
    virtual void operator()(char const* s) {
        std::cerr << "LOG: " << s << std::endl;
    }
};
}}}  // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_STDERRLOGGER_H
