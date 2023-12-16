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

#ifndef LSST_QSERV_WBASE_CHANNELSHARED_H
#define LSST_QSERV_WBASE_CHANNELSHARED_H

// System headers
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <stdexcept>
#include <thread>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "qmeta/types.h"
#include "util/InstanceCount.h"
#include "wbase/SendChannel.h"
#include "wbase/TransmitData.h"

namespace lsst::qserv::wbase {
class Task;
}

namespace lsst::qserv::util {
class MultiError;
}

namespace lsst::qserv::wbase {

/// The base class for a family of the shared channels
class ChannelShared {
public:
    using Ptr = std::shared_ptr<ChannelShared>;

    static std::atomic<uint64_t> scsSeqId;  ///< Source for unique _scsId numbers

    ChannelShared() = delete;
    ChannelShared(ChannelShared const&) = delete;
    ChannelShared& operator=(ChannelShared const&) = delete;

    /// Non-trival d-tor is needed to close the channel.
    virtual ~ChannelShared();

    /// Wrappers for wbase::SendChannel public functions that may need to be used
    /// by threads.
    /// @see wbase::SendChannel::send
    bool send(char const* buf, int bufLen);

    /// @see wbase::SendChannel::sendError
    bool sendError(std::string const& msg, int code);

    /// @see wbase::SendChannel::sendFile
    bool sendFile(int fd, wbase::SendChannel::Size fSize);

    /// @see wbase::SendChannel::sendStream
    bool sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last);

    /// @see wbase::SendChannel::kill
    bool kill(std::string const& note);

    /// @see wbase::SendChannel::isDead
    bool isDead();

    /// Set the number of Tasks that will be sent using this wbase::SendChannel.
    /// This should not be changed once set.
    void setTaskCount(int taskCount);
    int getTaskCount() const { return _taskCount; }

    /// @return true if this is the last task to call this
    bool transmitTaskLast();

    /// Return a normalized id string.
    static std::string makeIdStr(int qId, int jId);

    /// @return the sendChannelShared sequence number, which is always valid.
    uint64_t getScsId() const { return _scsId; }

    /// @return the current sql connection count
    int getSqlConnectionCount() { return _sqlConnectionCount; }

    /// @return the sql connection count after incrementing by 1.
    int incrSqlConnectionCount() { return ++_sqlConnectionCount; }

    /// @return true if this is the first time this function has been called.
    bool getFirstChannelSqlConn() { return _firstChannelSqlConn.exchange(false); }

    /// @return a transmit data object indicating the errors in 'multiErr'.
    bool buildAndTransmitError(util::MultiError& multiErr, std::shared_ptr<Task> const& task, bool cancelled);

    /// Put the SQL results in a TransmitData object and transmit it to the czar
    /// if appropriate.
    /// @return true if there was an error.
    virtual bool buildAndTransmitResult(MYSQL_RES* mResult, std::shared_ptr<Task> const& task,
                                        util::MultiError& multiErr, std::atomic<bool>& cancelled) = 0;

    /// @return a log worthy string describing transmitData.
    std::string dumpTransmit() const;

protected:
    /// Protected constructor is seen by subclasses only.
    ChannelShared(std::shared_ptr<wbase::SendChannel> const& sendChannel, qmeta::CzarId czarId);

    std::shared_ptr<wbase::SendChannel> const sendChannel() const { return _sendChannel; }

    /// Dumps transmitData into a string within the thread-safe context.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    std::string dumpTransmit(std::lock_guard<std::mutex> const& tMtxLock) const;

    /// @return a new TransmitData::Ptr object.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    TransmitData::Ptr createTransmit(std::lock_guard<std::mutex> const& tMtxLock, Task& task);

    /// Create a new transmitData object if needed.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    void initTransmit(std::lock_guard<std::mutex> const& tMtxLock, Task& task);

    /// Prepare the transmit data and then call addTransmit.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    bool prepTransmit(std::lock_guard<std::mutex> const& tMtxLock, std::shared_ptr<Task> const& task,
                      bool cancelled, bool lastIn);

    /// Try to transmit the data in tData.
    /// If the queue already has at least 2 TransmitData objects, addTransmit
    /// may wait before returning. Result rows are read from the
    /// database until there are no more rows or the buffer is
    /// sufficiently full. addTransmit waits until that buffer has been
    /// sent to the czar before reading more rows. Without the wait,
    /// the worker may read in too many result rows, run out of memory,
    /// and crash.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    /// @return true if transmit was added successfully.
    /// @see ChannelShared::_transmit code for further explanation.
    bool addTransmit(std::lock_guard<std::mutex> const& tMtxLock, std::shared_ptr<Task> const& task,
                     bool cancelled, bool erred, bool lastIn, TransmitData::Ptr const& tData, int qId,
                     int jId);

    std::shared_ptr<TransmitData> transmitData;  ///< TransmitData object
    mutable std::mutex tMtx;                     ///< protects transmitData

private:
    /// @see wbase::SendChannel::kill
    /// @param streamMutexLock - Lock on mutex _streamMutex to be acquired before calling the method.
    bool _kill(std::lock_guard<std::mutex> const& streamMutexLock, std::string const& note);

    /// Encode TransmitData items from _transmitQueue and pass them to XrdSsi
    /// to be sent to the czar.
    /// The header for the 'nextTransmit' item is appended to the result of
    /// 'thisTransmit', with a specially constructed header appended for the
    /// 'reallyLast' transmit.
    /// The specially constructed header for the 'reallyLast' transmit just
    /// says that there's no more data, this wbase::SendChannel is done.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    /// @param queueMtxLock - Lock on mutex _queueMtx to be acquired before calling the method.
    bool _transmit(std::lock_guard<std::mutex> const& tMtxLock,
                   std::lock_guard<std::mutex> const& queueMtxLock, bool erred,
                   std::shared_ptr<Task> const& task);

    /// Send the buffer 'streamBuffer' using xrdssi.
    /// 'last' should only be true if this is the last buffer to be sent with this _sendChannel.
    /// 'note' is just a log note about what/who is sending the buffer.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    /// @param queueMtxLock - Lock on mutex _queueMtx to be acquired before calling the method.
    /// @param streamMutexLock - Lock on mutex _streamMutex to be acquired before calling the method.
    /// @return true if the buffer was sent.
    bool _sendBuf(std::lock_guard<std::mutex> const& tMtxLock,
                  std::lock_guard<std::mutex> const& queueMtxLock,
                  std::lock_guard<std::mutex> const& streamMutexLock, xrdsvc::StreamBuffer::Ptr& streamBuf,
                  bool last, std::string const& note);

    std::shared_ptr<wbase::SendChannel> const _sendChannel;  ///< Used to pass encoded information to XrdSsi.

    /// streamMutex is used to protect _lastCount and messages that are sent
    /// using ChannelShared.
    std::mutex _streamMutex;

    std::queue<TransmitData::Ptr> _transmitQueue;  ///< Queue of data to be encoded and sent.
    std::mutex _queueMtx;                          ///< protects _transmitQueue

    /// metadata buffer. Once set, it cannot change until after Finish() has been called.
    std::string _metadataBuf;

    int _taskCount = 0;                      ///< The number of tasks to be sent over this wbase::SendChannel.
    int _lastCount = 0;                      ///< The number of 'last' buffers received.
    std::atomic<bool> _lastRecvd{false};     ///< The truly 'last' transmit message is in the queue.
    std::atomic<bool> _firstTransmit{true};  ///< True until the first transmit has been sent.

    qmeta::CzarId const _czarId;  ///< id of the czar that requested this task(s).
    uint64_t const _scsId;        ///< id number for this ChannelShared

    /// The number of sql connections opened to handle the Tasks using this ChannelShared.
    /// Once this is greater than 0, this object needs free access to sql connections to avoid
    // system deadlock. @see SqlConnMgr::_take() and SqlConnMgr::_release().
    std::atomic<int> _sqlConnectionCount{0};

    /// true until getFirstChannelSqlConn() is called.
    std::atomic<bool> _firstChannelSqlConn{true};

    std::shared_ptr<util::InstanceCount> _icPtr;  ///< temporary for LockupDB
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_CHANNELSHARED_H
