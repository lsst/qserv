// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_WDB_QUERYACTION_H
#define LSST_QSERV_WDB_QUERYACTION_H
 /**
  * @file
  *
  * @brief QueryAction instances perform single-shot query execution with the
  * result reflected in the db state or returned via a SendChannel. Works with
  * new XrdSsi API.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>

// Third-party headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wbase/Task.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wdb {
class ChunkResourceMgr;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wdb {

////////////////////////////////////////////////////////////////////////
/// Bundle of values needed to construct a QueryAction
struct QueryActionArg {
public:
    QueryActionArg() {}

    QueryActionArg(LOG_LOGGER const& log_,
                   wbase::Task::Ptr task_,
                   boost::shared_ptr<ChunkResourceMgr> mgr_)
        : log(log_), task(task_), mgr(mgr_) { }
    LOG_LOGGER log; ///< Logging handle
    wbase::Task::Ptr task; ///< Actual task
    boost::shared_ptr<ChunkResourceMgr> mgr; ///< Resource reservation
};
////////////////////////////////////////////////////////////////////////
/// A worker-side query action. Depending on contents of the task, writes
/// results to a table or to a supplied SendChannel.
class QueryAction {
public:
    QueryAction(QueryActionArg& a);
    ~QueryAction();
    bool operator()(); // Execute action

    void poison(); // Cancel the action (in-progress)

    class Impl;
private:
    std::shared_ptr<Impl> _impl; ///< PIMPL class
};

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_QUERYACTION_H
