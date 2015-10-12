// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

#ifndef LSST_QSERV_WBASE_MSGPROCESSOR_H
#define LSST_QSERV_WBASE_MSGPROCESSOR_H

// System headers
#include <memory>

// Qserv headers

// Forward declarations
namespace lsst {
namespace qserv {
namespace proto {
    class TaskMsg;
}
namespace wbase {
    class SendChannel;
    class Task;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wbase {

/** MsgProcessor implementations handle incoming TaskMsg objects by creating a Task to write their
 * results over a SendChannel
 */
struct MsgProcessor {
	virtual ~MsgProcessor() {}
    /// @return a pointer to the Task so it can be cancelled or tracked.
	virtual std::shared_ptr<Task> processMsg(std::shared_ptr<proto::TaskMsg> const& taskMsg,
	                                         std::shared_ptr<SendChannel> const& replyChannel) = 0;

};

}}} // namespace
#endif // LSST_QSERV_WBASE_MSGPROCESSOR_H
