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

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
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
/// Partially writtent file will be automatically deleted in case of
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

    static Ptr create(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                      std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                      std::shared_ptr<proto::TaskMsg> const& taskMsg);

    FileChannelShared() = delete;
    FileChannelShared(FileChannelShared const&) = delete;
    FileChannelShared& operator=(FileChannelShared const&) = delete;

    /// Non-trivial d-tor is required to close and delete the currently open file
    /// if the one is still open. Normally the file must be closed instantly after
    /// writing the last set of rows of the very last contributor (task).
    /// And this should happen before this destructor gets called. If it didn't
    /// happen then the file is meaningless and it must be gone.
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

    std::string _fileName;  ///< The name is set when opening the file
    std::fstream _file;

    // Counters reported to Czar in the only ("summary") message sent upon the completion
    // of all tasks of a query.

    uint32_t _rowcount = 0;      ///< The total numnber of rows in all result sets of a query.
    uint64_t _transmitsize = 0;  ///< The total amount of data (bytes) in all result sets of a query.
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_FILECHANNELSHARED_H
