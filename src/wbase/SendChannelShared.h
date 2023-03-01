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

#ifndef LSST_QSERV_WBASE_SENDCHANNELSHARED_H
#define LSST_QSERV_WBASE_SENDCHANNELSHARED_H

// System headers
#include <condition_variable>
#include <memory>
#include <mutex>

// Qserv headers
#include "qmeta/types.h"
#include "wbase/ChannelShared.h"

namespace lsst::qserv::wbase {
class SendChannel;
class Task;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wcontrol {
class TransmitMgr;
}

namespace lsst::qserv::util {
class MultiError;
}

namespace lsst::qserv::wbase {

/// A class that provides a SendChannel object with synchronization so it can be
/// shared by across multiple threads.
/// This class is now also responsible for assembling transmit messages from
/// mysql result rows as well as error messages.
///
/// When building messages for result rows, multiple tasks may add to the
/// the TransmitData object before it is transmitted to the czar. All the
/// tasks adding rows to the TransmitData object must be operating on
/// the same chunk. This only happens for near-neighbor queries, which
/// have one task per subchunk.
///
/// Error messages cause the existing TransmitData object to be thrown away
/// as the contents cannot be used. This is one of many reasons TransmitData
/// objects can only be shared among a single chunk.
///
/// An important concept for this class is '_lastRecvd'. This means that
/// the last TransmitData object needed is on the queue.
/// '_taskCount' is set with the number of Tasks that will add to this instance.
/// As each task sends its 'last' message, '_lastCount' is incremented.
/// When '_lastCount' == '_taskCount', the instance knows the '_lastRecvd'
/// message has been received and all queued up messages should be sent.
///
/// '_lastRecvd' is also set to true when an error message is sent. When
/// there's an error, the czar will throw out all data related to the
/// chunk, since it is unreliable. The error needs to be sent immediately to
/// waste as little time processing useless results as possible
///
/// Cancellation is tricky, it's easy to introduce race conditions that would
/// result in deadlock. It should work correctly given the following:
///    - buildAndTransmitResult() continues transmitting unless the Task
///      that called it is cancelled. Having a different Task break the loop
///      would be risky.
///    - buildAndTransmitError() error must be allowed to attempt to transmit
///      even if the Task has been cancelled. This prevents other Tasks getting
///      wedged waiting for data to be queued.
class SendChannelShared : public ChannelShared {
public:
    using Ptr = std::shared_ptr<SendChannelShared>;

    static Ptr create(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr, qmeta::CzarId czarId);

    SendChannelShared() = delete;
    SendChannelShared(SendChannelShared const&) = delete;
    SendChannelShared& operator=(SendChannelShared const&) = delete;
    virtual ~SendChannelShared() override = default;

    virtual bool buildAndTransmitResult(MYSQL_RES* mResult, std::shared_ptr<Task> const& task,
                                        util::MultiError& multiErr, std::atomic<bool>& cancelled) override;

private:
    /// Private constructor to protect shared pointer integrity.
    SendChannelShared(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr, qmeta::CzarId czarId);
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_SENDCHANNELSHARED_H
