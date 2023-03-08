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

// Class header
#include "wbase/FileChannelShared.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "util/MultiError.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;
namespace proto = lsst::qserv::proto;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.FileChannelShared");
}  // namespace

namespace lsst::qserv::wbase {

FileChannelShared::Ptr FileChannelShared::create(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                                 shared_ptr<proto::TaskMsg> const& taskMsg) {
    return shared_ptr<FileChannelShared>(new FileChannelShared(sendChannel, transmitMgr, taskMsg));
}

FileChannelShared::FileChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                                     shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                     shared_ptr<proto::TaskMsg> const& taskMsg)
        : ChannelShared(sendChannel, transmitMgr, taskMsg->czarid()) {}

FileChannelShared::~FileChannelShared() {
    if (!_fileName.empty() && _file.is_open()) {
        boost::system::error_code ec;
        fs::remove_all(fs::path(_fileName), ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_WARN,
                 "FileChannelShared::" << __func__ << " failed to remove the result file '" << _fileName
                                       << "', ec: " << ec << ".");
        }
    }
}

bool FileChannelShared::buildAndTransmitResult(MYSQL_RES* mResult, shared_ptr<Task> const& task,
                                               util::MultiError& multiErr, atomic<bool>& cancelled) {
    lock_guard<mutex> const tMtxLock(tMtx);
    bool hasMoreRows = true;
    try {
        // Keep reading rows and converting those into messages while any
        // are still left in the result set. The row processing method
        // will write rows into the output file. The final "summary" message
        // will be sant back to Czar after processing the very last set of rows
        // of the last task of a request.
        while (hasMoreRows && !cancelled) {
            // Initialize transmitData, if needed.
            initTransmit(tMtxLock, *task);

            // Transwer rows from a result set into the data buffer. Note that tSize
            // is set by fillRows. A value of this variable is poresently not used by
            // the code.
            size_t tSize = 0;
            hasMoreRows = !transmitData->fillRows(mResult, tSize);

            // Serialize the content of the data buffer into the Protobuf data message
            // that will be writen into the output file.
            transmitData->buildDataMsg(*task, multiErr);
            _writeToFile(tMtxLock, task, transmitData->dataMsg());

            _rowcount += transmitData->getResultRowCount();
            _transmitsize += transmitData->getResultTransmitSize();

            // If no more rows are left in the task's result set then we need to check
            // if this is last task in a logical group of ones created for processing
            // the current request (note that certain classes of requests may require
            // more than one task for processing).
            if (!hasMoreRows && transmitTaskLast()) {
                // Make sure the file is sync to disk before notifying Czar.
                _file.flush();
                _file.close();

                // Only the last ("summary") message w/o any rows is sent to Czar to notify
                // the about completion of the request.
                transmitData->emptyRows(*task, _rowcount, _transmitsize);
                bool const lastIn = true;
                if (!prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                    throw runtime_error("FileChannelShared::" + string(__func__) +
                                        " Could not transmit the summary message to Czar.");
                }
            } else {
                // Scrap the transmit buffer to be ready for processing the next set of rows
                // of the current or the next task of the request.
                transmitData.reset();
            }
        }
    } catch (runtime_error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
        return true;
    }
    return false;
}

void FileChannelShared::_writeToFile(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     string const& msg) {
    if (!_file.is_open()) {
        _fileName = task->resultFilePath();
        _file.open(_fileName, ios::out | ios::trunc | ios::binary);
        if (!(_file.is_open() && _file.good())) {
            throw runtime_error("FileChannelShared::" + string(__func__) +
                                "failed to create/truncate the file '" + _fileName + "'.");
        }
    }
    // Write 32-bit length of the subsequent message first before writing
    // the message itself.
    uint32_t const msgSizeBytes = msg.size();
    _file.write(reinterpret_cast<char const*>(&msgSizeBytes), sizeof msgSizeBytes);
    _file.write(msg.data(), msgSizeBytes);
    if (!(_file.is_open() && _file.good())) {
        throw runtime_error("FileChannelShared::" + string(__func__) + "failed to write " +
                            to_string(msg.size()) + " bytes into the file '" + _fileName + "'.");
    }
}

}  // namespace lsst::qserv::wbase
