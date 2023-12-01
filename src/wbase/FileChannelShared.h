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
#include <vector>

// Third-party headers
#include <mysql/mysql.h>
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "wbase/ChannelShared.h"

namespace lsst::qserv::proto {
class TaskMsg;
}

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
class FileChannelShared : public ChannelShared {
public:
    using Ptr = std::shared_ptr<FileChannelShared>;

    /**
     * This method gets called upon receiving a notification from Czar about
     * the Czar service restart. The method will clean result files corresponding
     * to the older (including the specified one) queries.
     * @note The method may be called 0 or many times during the lifetime of the worker service.
     * @param queryId The most recent user query registered before restart.
     */
    static void cleanUpResultsOnCzarRestart(QueryId queryId);

    /**
     * This method gets called exactly one time during the initial startup
     * initialization of the worker service.
     */
    static void cleanUpResultsOnWorkerRestart();

    /**
     * Clean up result files of the specified query.
     * @param queryId The most recent user query registered before restart.
     */
    static void cleanUpResults(QueryId queryId);

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
                      std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                      std::shared_ptr<proto::TaskMsg> const& taskMsg);

    FileChannelShared() = delete;
    FileChannelShared(FileChannelShared const&) = delete;
    FileChannelShared& operator=(FileChannelShared const&) = delete;

    // Non-trivial d-tor is needed to garbage collect the file after failures.
    virtual ~FileChannelShared() override;

    /// @see ChannelShared::buildAndTransmitResult()
    virtual bool buildAndTransmitResult(MYSQL_RES* mResult, std::shared_ptr<Task> const& task,
                                        util::MultiError& multiErr, std::atomic<bool>& cancelled) override;

private:
    /// Private constructor to protect shared pointer integrity.
    FileChannelShared(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                      std::shared_ptr<proto::TaskMsg> const& taskMsg);

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
