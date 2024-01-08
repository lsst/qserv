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

#ifndef LSST_QSERV_WBASE_FILECHANNELSHARED_H
#define LSST_QSERV_WBASE_FILECHANNELSHARED_H

// System headers
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

// Third-party headers
#include <mysql/mysql.h>
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "wbase/SendChannel.h"

// Forward declarations

namespace lsst::qserv::proto {
class TaskMsg;
}  // namespace lsst::qserv::proto

namespace lsst::qserv::wbase {
class Task;
class TransmitData;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::util {
class InstanceCount;
class MultiError;
}  // namespace lsst::qserv::util

namespace lsst::qserv::xrdsvc {
class StreamBuffer;
}  // namespace lsst::qserv::xrdsvc

namespace lsst::qserv::wbase {

/// The class is responsible for writing mysql result rows as Protobuf
/// serialized messages into an output file. Once a task (or all sub-chunk
/// tasks) finished writing data a short reply message is sent back to Czar using
/// SSI request's SendChannel that was provided to the factory method
/// of the class. Error messages would be also sent via te same channel.
/// A partially written file will be automatically deleted in case of
/// errors.
///
/// When building messages for result rows, multiple tasks may add to the
/// the output file before it gets closed and a reply is transmitted to the czar.
/// All the tasks adding rows to the TransmitData object must be operating on
/// the same chunk. This only happens for near-neighbor queries, which
/// have one task per subchunk.
class FileChannelShared {
public:
    using Ptr = std::shared_ptr<FileChannelShared>;

    static std::atomic<uint64_t> scsSeqId;  ///< Source for unique _scsId numbers

    /**
     * This method gets called upon receiving a notification from Czar about
     * the Czar service restart. The method will clean result files corresponding
     * to the older (including the specified one) queries.
     * @note The method may be called 0 or many times during the lifetime of the worker service.
     * @note The Czar identifier is required to avoid possble interferences in the multi-Czar
     *   deployments where different instances of Czars may restart independently.
     * @param czarId The unique identifier of Czar that initiated the query.
     * @param queryId The most recent user query registered before restart.
     */
    static void cleanUpResultsOnCzarRestart(uint32_t czarId, QueryId queryId);

    /**
     * This method gets called exactly one time during the initial startup
     * initialization of the worker service.
     */
    static void cleanUpResultsOnWorkerRestart();

    /**
     * Clean up result files of the specified query.
     * @param czarId The unique identifier of Czar that initiated the query.
     * @param queryId The most recent user query registered before restart.
     */
    static void cleanUpResults(uint32_t czarId, QueryId queryId);

    /// @return Status and statistics on the results folder (capacity, usage, etc.)
    static nlohmann::json statusToJson();

    /**
     * Locate existing result files.
     * @param queryIds The optional selector for queries. If the collection is empty
     *   then all queries will be considered.
     * @param maxFiles The optional limit for maximum number of files to be reported.
     *   If 0 then no limit is set.
     * @return A collection of the results files matching the optional filter.
     */
    static nlohmann::json filesToJson(std::vector<QueryId> const& queryIds, unsigned int maxFiles);

    /// The factory method for the channel class.
    static Ptr create(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<proto::TaskMsg> const& taskMsg);

    FileChannelShared() = delete;
    FileChannelShared(FileChannelShared const&) = delete;
    FileChannelShared& operator=(FileChannelShared const&) = delete;

    /// Non-trivial d-tor is needed to close the channel and to garbage collect
    /// the file after failures.
    ~FileChannelShared();

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
    bool buildAndTransmitResult(MYSQL_RES* mResult, std::shared_ptr<Task> const& task,
                                util::MultiError& multiErr, std::atomic<bool>& cancelled);

    /// Wrappers for wbase::SendChannel public functions that may need to be used
    /// by threads.
    /// @see wbase::SendChannel::send
    bool send(char const* buf, int bufLen);

    /// @see wbase::SendChannel::sendError
    bool sendError(std::string const& msg, int code);

    /// @see wbase::SendChannel::sendFile
    bool sendFile(int fd, wbase::SendChannel::Size fSize);

    /// @see wbase::SendChannel::sendStream
    bool sendStream(std::shared_ptr<xrdsvc::StreamBuffer> const& sBuf, bool last);

    /// @see wbase::SendChannel::kill
    bool kill(std::string const& note);

    /// @see wbase::SendChannel::isDead
    bool isDead();

    /// @return a log worthy string describing transmitData.
    std::string dumpTransmit() const;

private:
    /// Private constructor to protect shared pointer integrity.
    FileChannelShared(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<proto::TaskMsg> const& taskMsg);

    std::shared_ptr<wbase::SendChannel> const sendChannel() const { return _sendChannel; }

    /// Dumps transmitData into a string within the thread-safe context.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    std::string dumpTransmit(std::lock_guard<std::mutex> const& tMtxLock) const;

    /// @return a pointer to a new TransmitData> object.
    /// @param tMtxLock - Lock on mutex tMtx to be acquired before calling the method.
    std::shared_ptr<TransmitData> createTransmit(std::lock_guard<std::mutex> const& tMtxLock, Task& task);

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
    /// @see _transmit code for further explanation.
    bool addTransmit(std::lock_guard<std::mutex> const& tMtxLock, std::shared_ptr<Task> const& task,
                     bool cancelled, bool erred, bool lastIn, std::shared_ptr<TransmitData> const& tData,
                     int qId, int jId);

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
                  std::lock_guard<std::mutex> const& streamMutexLock,
                  std::shared_ptr<xrdsvc::StreamBuffer>& streamBuf, bool last, std::string const& note);

    /**
     * Write a message into the output file. The file will be created at the first call
     * to the method.
     * @param tMtxLock - a lock on the base class's mutex tMtx
     * @param task - a task that produced the result set
     * @param msg - data to be written
     * @throws std::runtime_error for problems encountered when attemting to create the file
     *   or write into the file.
     */
    void _writeToFile(std::lock_guard<std::mutex> const& tMtxLock, std::shared_ptr<Task> const& task,
                      std::string const& msg);

    /**
     * Unconditionaly close and remove (potentially - the partially written) file.
     * This method gets called in case of any failure detected while processing
     * a query, sending a response back to Czar, or in case of a query cancellation.
     * @note For succesfully completed requests the files are deleted remotely
     *   upon special requests made explicitly by Czar after uploading and consuming
     *   result sets. Unclaimed files that might be still remaining at the results
     *   folder would need to be garbage collected at the startup time of the worker.
     * @param tMtxLock - a lock on the base class's mutex tMtx
     */
    void _removeFile(std::lock_guard<std::mutex> const& tMtxLock);

    std::shared_ptr<TransmitData> transmitData;  ///< TransmitData object
    mutable std::mutex tMtx;                     ///< protects transmitData

    std::shared_ptr<wbase::SendChannel> const _sendChannel;  ///< Used to pass encoded information to XrdSsi.

    /// streamMutex is used to protect _lastCount and messages that are sent
    /// using FileChannelShared.
    std::mutex _streamMutex;

    std::queue<std::shared_ptr<TransmitData>> _transmitQueue;  ///< Queue of data to be encoded and sent.
    std::mutex _queueMtx;                                      ///< protects _transmitQueue

    /// metadata buffer. Once set, it cannot change until after Finish() has been called.
    std::string _metadataBuf;

    int _taskCount = 0;                      ///< The number of tasks to be sent over this wbase::SendChannel.
    int _lastCount = 0;                      ///< The number of 'last' buffers received.
    std::atomic<bool> _lastRecvd{false};     ///< The truly 'last' transmit message is in the queue.
    std::atomic<bool> _firstTransmit{true};  ///< True until the first transmit has been sent.

    qmeta::CzarId const _czarId;  ///< id of the czar that requested this task(s).
    uint64_t const _scsId;        ///< id number for this FileChannelShared

    /// The number of sql connections opened to handle the Tasks using this FileChannelShared.
    /// Once this is greater than 0, this object needs free access to sql connections to avoid
    // system deadlock. @see SqlConnMgr::_take() and SqlConnMgr::_release().
    std::atomic<int> _sqlConnectionCount{0};

    /// true until getFirstChannelSqlConn() is called.
    std::atomic<bool> _firstChannelSqlConn{true};

    std::shared_ptr<util::InstanceCount> _icPtr;  ///< temporary for LockupDB

    /// The mutex is locked by the following static methods which require exclusive
    /// access to the results folder: create(), cleanUpResultsOnCzarRestart(),
    /// cleanUpResultsOnWorkerRestart(), and cleanUpResults().
    static std::mutex _resultsDirCleanupMtx;

    std::string _fileName;  ///< The name is set when opening the file
    std::fstream _file;

    // Counters reported to Czar in the only ("summary") message sent upon the completion
    // of all tasks of a query.

    uint32_t _rowcount = 0;      ///< The total numnber of rows in all result sets of a query.
    uint64_t _transmitsize = 0;  ///< The total amount of data (bytes) in all result sets of a query.
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_FILECHANNELSHARED_H
