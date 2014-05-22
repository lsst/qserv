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

#ifndef LSST_QSERV_CCONTROL_USERQUERY_H
#define LSST_QSERV_CCONTROL_USERQUERY_H
/**
  * @file
  *
  * @brief Umbrella container for user query state
   *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

// Local headers
#include "ccontrol/QueryState.h"
#include "css/StripingParams.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace qdisp {
class Executive;
class MessageStore;
}
namespace qproc {
class QuerySession;
}
namespace rproc {
class TableMerger;
class TableMergerConfig;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {
class UserQueryFactory;

/// UserQuery : top-level class for user query data. Not thread-safe, although
/// its delegates are thread-safe as appropriate.
class UserQuery : public boost::noncopyable {
public:
    typedef boost::shared_ptr<UserQuery> Ptr;
    friend class UserQueryFactory;

    std::string const& getError() const;
    lsst::qserv::query::ConstraintVec getConstraints() const;
    std::string const& getDominantDb() const;
    lsst::qserv::css::StripingParams getDbStriping() const;
    std::string getExecDesc() const;

    void addChunk(qproc::ChunkSpec const& cs);
    void submit();
    QueryState join();
    void kill(); //< Stop a query in progress (for immediate shutdowns)
    void discard(); //< Release resources related to user query

    // Exists only to help app.py with permission checking
    // TODO: Eliminate after geometry is pushed to c++
    bool containsDb(std::string const& dbName) const;

    // Delegate objects
    boost::shared_ptr<qdisp::Executive> getExecutive() {
        return _executive; }
    boost::shared_ptr<qdisp::MessageStore> getMessageStore() {
        return _messageStore; }

private:
    explicit UserQuery(boost::shared_ptr<qproc::QuerySession> qs);
    void setSessionId(int session) { _sessionId = session; }
    void _setupMerger();

    boost::shared_ptr<qdisp::Executive> _executive;
    boost::shared_ptr<qdisp::MessageStore> _messageStore;
    boost::shared_ptr<qproc::QuerySession> _qSession;
    boost::shared_ptr<rproc::TableMergerConfig> _mergerConfig;
    boost::shared_ptr<rproc::TableMerger> _merger;
    int _sessionId;
    int _sequence;
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERY_H
