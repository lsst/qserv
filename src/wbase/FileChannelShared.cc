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
#include <functional>
#include <limits>
#include <set>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"
#include "boost/range/iterator_range.hpp"

// Qserv headers
#include "global/LogContext.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "wbase/TransmitData.h"
#include "wbase/Task.h"
#include "wconfig/WorkerConfig.h"
#include "wpublish/QueriesAndChunks.h"
#include "util/Bug.h"
#include "util/Error.h"
#include "util/MultiError.h"
#include "util/ResultFileNameParser.h"
#include "util/Timer.h"
#include "util/TimeUtils.h"
#include "xrdsvc/StreamBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;
namespace fs = boost::filesystem;
namespace util = lsst::qserv::util;
namespace wconfig = lsst::qserv::wconfig;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.FileChannelShared");

bool isResultFile(fs::path const& filePath) {
    return filePath.has_filename() && filePath.has_extension() &&
           (filePath.extension() == util::ResultFileNameParser::fileExt);
}

/**
 * Iterate over the result files at the results folder and remove those
 * which satisfy the desired criteria.
 * @note The folder must exist when this function gets called. Any other
 *   scenario means a configuration error or a problem with the infrastructure.
 *   Running into either of these problems should result in the abort of
 *   the application.
 * @param context The calling context (used for logging purposes).
 * @param fileCanBeRemoved The optional validator to be called for each candidate file.
 *   Note that missing validator means "yes" the candidate file can be removed.
 * @return The total number of removed files.
 * @throws std::runtime_error If the results folder doesn't exist.
 */
size_t cleanUpResultsImpl(string const& context, fs::path const& dirPath,
                          function<bool(string const&)> fileCanBeRemoved = nullptr) {
    size_t numFilesRemoved = 0;
    boost::system::error_code ec;
    auto itr = fs::directory_iterator(dirPath, ec);
    if (ec.value() != 0) {
        string const err = context + "failed to open the results folder '" + dirPath.string() +
                           "', ec: " + to_string(ec.value()) + ".";
        LOGS(_log, LOG_LVL_ERROR, err);
        throw runtime_error(err);
    }
    for (auto&& entry : boost::make_iterator_range(itr, {})) {
        auto filePath = entry.path();
        bool const removeIsCleared =
                ::isResultFile(filePath) &&
                ((fileCanBeRemoved == nullptr) || fileCanBeRemoved(filePath.filename().string()));
        if (removeIsCleared) {
            fs::remove_all(filePath, ec);
            if (ec.value() != 0) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "failed to remove result file " << filePath << ", ec: " << ec << ".");
            } else {
                LOGS(_log, LOG_LVL_INFO, context << "removed result file " << filePath << ".");
                ++numFilesRemoved;
            }
        }
    }
    return numFilesRemoved;
}

}  // namespace

namespace lsst::qserv::wbase {

atomic<uint64_t> FileChannelShared::scsSeqId{0};

mutex FileChannelShared::_resultsDirCleanupMtx;

void FileChannelShared::cleanUpResultsOnCzarRestart(uint32_t czarId, QueryId queryId) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    fs::path const dirPath = wconfig::WorkerConfig::instance()->resultsDirname();
    LOGS(_log, LOG_LVL_INFO,
         context << "removing result files from " << dirPath << " for czarId=" << czarId
                 << " queryId=" << queryId << " or older.");
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    size_t const numFilesRemoved = ::cleanUpResultsImpl(
            context, dirPath, [czarId, queryId, &context](string const& fileName) -> bool {
                try {
                    auto const fileAttributes = util::ResultFileNameParser(fileName);
                    return (fileAttributes.czarId == czarId) && (fileAttributes.queryId <= queryId);
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_WARN,
                         context << "failed to parse the file name " << fileName << ", ex: " << ex.what());
                }
                return false;
            });
    LOGS(_log, LOG_LVL_INFO,
         context << "removed " << numFilesRemoved << " result files from " << dirPath << ".");
}

void FileChannelShared::cleanUpResultsOnWorkerRestart() {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    fs::path const dirPath = wconfig::WorkerConfig::instance()->resultsDirname();
    LOGS(_log, LOG_LVL_INFO, context << "removing all result files from " << dirPath << ".");
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    size_t const numFilesRemoved = ::cleanUpResultsImpl(context, dirPath);
    LOGS(_log, LOG_LVL_INFO,
         context << "removed " << numFilesRemoved << " result files from " << dirPath << ".");
}

void FileChannelShared::cleanUpResults(uint32_t czarId, QueryId queryId) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    fs::path const dirPath = wconfig::WorkerConfig::instance()->resultsDirname();
    LOGS(_log, LOG_LVL_INFO,
         context << "removing result files from " << dirPath << " for czarId=" << czarId
                 << " and queryId=" << queryId << ".");
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    size_t const numFilesRemoved = ::cleanUpResultsImpl(
            context, dirPath, [&context, czarId, queryId](string const& fileName) -> bool {
                try {
                    auto const fileAttributes = util::ResultFileNameParser(fileName);
                    return (fileAttributes.czarId == czarId) && (fileAttributes.queryId == queryId);
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_WARN,
                         context << "failed to parse the file name " << fileName << ", ex: " << ex.what());
                }
                return false;
            });
    LOGS(_log, LOG_LVL_INFO,
         context << "removed " << numFilesRemoved << " result files from " << dirPath << ".");
}

json FileChannelShared::statusToJson() {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    auto const config = wconfig::WorkerConfig::instance();
    string const protocol = wconfig::WorkerConfig::protocol2str(config->resultDeliveryProtocol());
    fs::path const dirPath = config->resultsDirname();
    json result = json::object({{"protocol", protocol},
                                {"folder", dirPath.string()},
                                {"capacity_bytes", -1},
                                {"free_bytes", -1},
                                {"available_bytes", -1},
                                {"num_result_files", -1},
                                {"size_result_files_bytes", -1}});
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    try {
        auto const space = fs::space(dirPath);
        result["capacity_bytes"] = space.capacity;
        result["free_bytes"] = space.free;
        result["available_bytes"] = space.available;
        uintmax_t sizeResultFilesBytes = 0;
        uintmax_t numResultFiles = 0;
        auto itr = fs::directory_iterator(dirPath);
        for (auto&& entry : boost::make_iterator_range(itr, {})) {
            auto const filePath = entry.path();
            if (::isResultFile(filePath)) {
                numResultFiles++;
                sizeResultFilesBytes += fs::file_size(filePath);
            }
        }
        result["num_result_files"] = numResultFiles;
        result["size_result_files_bytes"] = sizeResultFilesBytes;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             context << "failed to get folder stats for " << dirPath << ", ex: " << ex.what());
    }
    return result;
}

json FileChannelShared::filesToJson(vector<QueryId> const& queryIds, unsigned int maxFiles) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    set<QueryId> queryIdsFilter;
    for (auto const queryId : queryIds) {
        queryIdsFilter.insert(queryId);
    }
    auto const config = wconfig::WorkerConfig::instance();
    fs::path const dirPath = config->resultsDirname();
    unsigned int numTotal = 0;
    unsigned int numSelected = 0;
    json files = json::array();
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    try {
        auto itr = fs::directory_iterator(dirPath);
        for (auto&& entry : boost::make_iterator_range(itr, {})) {
            auto const filePath = entry.path();
            if (::isResultFile(filePath)) {
                ++numTotal;

                // Skip files not matching the query criteria if the one was requested.
                json const jsonTask = util::ResultFileNameParser(filePath).toJson();
                QueryId const queryId = jsonTask.at("query_id");
                if (!queryIdsFilter.empty() && !queryIdsFilter.contains(queryId)) continue;

                // Stop collecting files after reaching the limit (if any). And keep counting.
                ++numSelected;
                if ((maxFiles != 0) && (files.size() >= maxFiles)) continue;

                // A separate exception handler to avoid and ignore race conditions if
                // the current file gets deleted. In this scenario the file will not be
                // reported in the result.
                try {
                    files.push_back(json::object({{"filename", filePath.filename().string()},
                                                  {"size", fs::file_size(filePath)},
                                                  {"ctime", fs::creation_time(filePath)},
                                                  {"mtime", fs::last_write_time(filePath)},
                                                  {"current_time_ms", util::TimeUtils::now()},
                                                  {"task", jsonTask}}));
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_WARN,
                         context << "failed to get info on files at " << dirPath << ", ex: " << ex.what());
                }
            }
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             context << "failed to iterate over files at " << dirPath << ", ex: " << ex.what());
    }
    return json::object({{"files", files}, {"num_selected", numSelected}, {"num_total", numTotal}});
}

shared_ptr<FileChannelShared> FileChannelShared::create(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                        shared_ptr<proto::TaskMsg> const& taskMsg) {
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    return shared_ptr<FileChannelShared>(new FileChannelShared(sendChannel, taskMsg));
}

FileChannelShared::FileChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                                     shared_ptr<proto::TaskMsg> const& taskMsg)
        : _sendChannel(sendChannel), _czarId(taskMsg->czarid()), _scsId(scsSeqId++) {
    if (_sendChannel == nullptr) {
        throw util::Bug(ERR_LOC, "FileChannelShared constructor given nullptr");
    }
}

FileChannelShared::~FileChannelShared() {
    // Normally, the channel should not be dead at this time. If it's already
    // dead it means there was a problem to process a query or send back a response
    // to Czar. In either case, the file would be useless and it has to be deleted
    // in order to avoid leaving unclaimed result files within the results folder.
    if (isDead()) {
        _removeFile(lock_guard<mutex>(tMtx));
    }
    if (_sendChannel != nullptr) {
        _sendChannel->setDestroying();
        if (!_sendChannel->isDead()) {
            _sendChannel->kill("~FileChannelShared()");
        }
    }
}

void FileChannelShared::setTaskCount(int taskCount) { _taskCount = taskCount; }

bool FileChannelShared::transmitTaskLast() {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    ++_lastCount;
    bool lastTaskDone = _lastCount >= _taskCount;
    return lastTaskDone;
}

bool FileChannelShared::send(char const* buf, int bufLen) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->send(buf, bufLen);
}

bool FileChannelShared::sendError(string const& msg, int code) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendError(msg, code);
}

bool FileChannelShared::sendFile(int fd, wbase::SendChannel::Size fSize) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendFile(fd, fSize);
}

bool FileChannelShared::sendStream(shared_ptr<xrdsvc::StreamBuffer> const& sBuf, bool last) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _sendChannel->sendStream(sBuf, last);
}

bool FileChannelShared::kill(string const& note) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _kill(streamMutexLock, note);
}

bool FileChannelShared::isDead() {
    if (_sendChannel == nullptr) return true;
    return _sendChannel->isDead();
}

string FileChannelShared::makeIdStr(int qId, int jId) {
    string str("QID" + (qId == 0 ? "" : to_string(qId) + "#" + to_string(jId)));
    return str;
}

string FileChannelShared::dumpTransmit() const {
    lock_guard<mutex> const tMtxLock(tMtx);
    return dumpTransmit(tMtxLock);
}

bool FileChannelShared::buildAndTransmitError(util::MultiError& multiErr, shared_ptr<Task> const& task,
                                              bool cancelled) {
    lock_guard<mutex> const tMtxLock(tMtx);
    // Ignore the existing transmitData object as it is irrelevant now
    // that there's an error. Create a new one to send the error.
    auto tData = createTransmit(tMtxLock, *task);
    transmitData = tData;
    transmitData->buildDataMsg(*task, multiErr);
    LOGS(_log, LOG_LVL_DEBUG, "FileChannelShared::buildAndTransmitError " << dumpTransmit(tMtxLock));
    bool lastIn = true;
    return prepTransmit(tMtxLock, task, cancelled, lastIn);
}

bool FileChannelShared::buildAndTransmitResult(MYSQL_RES* mResult, shared_ptr<Task> const& task,
                                               util::MultiError& multiErr, atomic<bool>& cancelled) {
    // Operation stats. Note that "buffer fill time" included the amount
    // of time needed to write the result set to disk.
    util::Timer transmitT;
    transmitT.start();

    double bufferFillSecs = 0.0;
    int64_t bytesTransmitted = 0;
    int rowsTransmitted = 0;

    // Keep reading rows and converting those into messages while any
    // are still left in the result set. The row processing method
    // will write rows into the output file. The final "summary" message
    // will be sant back to Czar after processing the very last set of rows
    // of the last task of a request.
    bool erred = false;
    bool hasMoreRows = true;

    // This lock is to protect transmitData from having other Tasks mess with it
    // while data is loading.
    lock_guard<mutex> const tMtxLock(tMtx);

    while (hasMoreRows && !cancelled) {
        util::Timer bufferFillT;
        bufferFillT.start();

        // Initialize transmitData, if needed.
        initTransmit(tMtxLock, *task);

        // Transfer rows from a result set into the data buffer. Note that tSize
        // is set by fillRows. A value of this variable is presently not used by
        // the code.
        size_t tSize = 0;
        hasMoreRows = !transmitData->fillRows(mResult, tSize);

        // Serialize the content of the data buffer into the Protobuf data message
        // that will be writen into the output file.
        transmitData->buildDataMsg(*task, multiErr);
        _writeToFile(tMtxLock, task, transmitData->dataMsg());

        bufferFillT.stop();
        bufferFillSecs += bufferFillT.getElapsed();

        int const bytes = transmitData->getResultSize();
        int const rows = transmitData->getResultRowCount();
        bytesTransmitted += bytes;
        rowsTransmitted += rows;
        _rowcount += rows;
        _transmitsize += bytes;

        // Fail the operation if the amount of data in the result set exceeds the requested
        // "large result" limit (in case if the one was specified).
        if (int64_t const maxTableSize = task->getMaxTableSize();
            maxTableSize > 0 && bytesTransmitted > maxTableSize) {
            string const err = "The result set size " + to_string(bytesTransmitted) +
                               " of a job exceeds the requested limit of " + to_string(maxTableSize) +
                               " bytes, task: " + task->getIdStr();
            multiErr.push_back(util::Error(util::ErrorCode::WORKER_RESULT_TOO_LARGE, err));
            LOGS(_log, LOG_LVL_ERROR, err);
            erred = true;
            break;
        }

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
            transmitData->prepareResponse(*task, _rowcount, _transmitsize);
            bool const lastIn = true;
            if (!prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                LOGS(_log, LOG_LVL_ERROR, "Could not transmit the summary message to Czar.");
                erred = true;
                break;
            }
        } else {
            // Scrap the transmit buffer to be ready for processing the next set of rows
            // of the current or the next task of the request.
            transmitData.reset();
        }
    }
    transmitT.stop();
    double timeSeconds = transmitT.getElapsed();
    auto qStats = task->getQueryStats();
    if (qStats == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "No statistics for " << task->getIdStr());
    } else {
        qStats->addTaskTransmit(timeSeconds, bytesTransmitted, rowsTransmitted, bufferFillSecs);
        LOGS(_log, LOG_LVL_TRACE,
             "TaskTransmit time=" << timeSeconds << " bufferFillSecs=" << bufferFillSecs);
    }

    // No reason to keep the file after a failure (hit while processing a query,
    // extracting a result set into the file) or query cancellation. This also
    // includes problems encountered while sending a response back to Czar after
    // sucesufully processing the query and writing all results into the file.
    // The file is not going to be used by Czar in either of these scenarios.
    if (cancelled || erred || isDead()) {
        _removeFile(tMtxLock);
    }
    return erred;
}

void FileChannelShared::_writeToFile(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     string const& msg) {
    if (!_file.is_open()) {
        _fileName = task->resultFilePath();
        _file.open(_fileName, ios::out | ios::trunc | ios::binary);
        if (!(_file.is_open() && _file.good())) {
            throw runtime_error("FileChannelShared::" + string(__func__) +
                                " failed to create/truncate the file '" + _fileName + "'.");
        }
    }
    // Write 32-bit length of the subsequent message first before writing
    // the message itself.
    uint32_t const msgSizeBytes = msg.size();
    _file.write(reinterpret_cast<char const*>(&msgSizeBytes), sizeof msgSizeBytes);
    _file.write(msg.data(), msgSizeBytes);
    if (!(_file.is_open() && _file.good())) {
        throw runtime_error("FileChannelShared::" + string(__func__) + " failed to write " +
                            to_string(msg.size()) + " bytes into the file '" + _fileName + "'.");
    }
}

void FileChannelShared::_removeFile(lock_guard<mutex> const& tMtxLock) {
    if (!_fileName.empty() && _file.is_open()) {
        _file.close();
        boost::system::error_code ec;
        fs::remove_all(fs::path(_fileName), ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_WARN,
                 "FileChannelShared::" << __func__ << " failed to remove the result file '" << _fileName
                                       << "', ec: " << ec << ".");
        }
    }
}

string FileChannelShared::dumpTransmit(lock_guard<mutex> const& lock) const {
    return string("ChannelShared::dumpTransmit ") +
           (transmitData == nullptr ? "nullptr" : transmitData->dump());
}

void FileChannelShared::initTransmit(lock_guard<mutex> const& tMtxLock, Task& task) {
    LOGS(_log, LOG_LVL_TRACE, "initTransmit " << task.getIdStr());
    if (transmitData == nullptr) {
        transmitData = createTransmit(tMtxLock, task);
    }
}

shared_ptr<TransmitData> FileChannelShared::createTransmit(lock_guard<mutex> const& tMtxLock, Task& task) {
    LOGS(_log, LOG_LVL_TRACE, "createTransmit " << task.getIdStr());
    auto tData = wbase::TransmitData::createTransmitData(_czarId, task.getIdStr());
    tData->initResult(task);
    return tData;
}

bool FileChannelShared::prepTransmit(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     bool cancelled, bool lastIn) {
    auto qId = task->getQueryId();
    int jId = task->getJobId();

    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    LOGS(_log, LOG_LVL_DEBUG, "_transmit lastIn=" << lastIn);
    if (isDead()) {
        LOGS(_log, LOG_LVL_INFO, "aborting transmit since sendChannel is dead.");
        return false;
    }

    // Have all rows already been read, or an error?
    bool erred = transmitData->hasErrormsg();

    bool success = addTransmit(tMtxLock, task, cancelled, erred, lastIn, transmitData, qId, jId);

    // Now that transmitData is on the queue, reset and initialize a new one.
    transmitData.reset();
    initTransmit(tMtxLock, *task);  // reset transmitData

    return success;
}

bool FileChannelShared::addTransmit(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                    bool cancelled, bool erred, bool lastIn,
                                    shared_ptr<TransmitData> const& tData, int qId, int jId) {
    QSERV_LOGCONTEXT_QUERY_JOB(qId, jId);
    assert(tData != nullptr);

    // This lock may be held for a very long time.
    lock_guard<mutex> const queueMtxLock(_queueMtx);
    _transmitQueue.push(tData);

    // If _lastRecvd is true, the last message has already been transmitted and
    // this SendChannel is effectively dead.
    bool reallyLast = _lastRecvd;
    string idStr(makeIdStr(qId, jId));
    if (_icPtr == nullptr) {
        _icPtr = make_shared<util::InstanceCount>(to_string(qId) + "_SCS_LDB");
    }

    // If something bad already happened, just give up.
    if (reallyLast || isDead()) {
        // If there's been some kind of error, make sure that nothing hangs waiting
        // for this.
        LOGS(_log, LOG_LVL_WARN, "addTransmit getting messages after isDead or reallyLast " << idStr);
        _lastRecvd = true;
        return false;
    }

    // If lastIn is true, all tasks for this job have run to completion and
    // finished building their transmit messages.
    if (lastIn) {
        reallyLast = true;
    }
    if (reallyLast || erred || cancelled) {
        _lastRecvd = true;
        LOGS(_log, LOG_LVL_DEBUG,
             "addTransmit lastRecvd=" << _lastRecvd << " really=" << reallyLast << " erred=" << erred
                                      << " cancelled=" << cancelled);
    }

    return _transmit(tMtxLock, queueMtxLock, erred, task);
}

bool FileChannelShared::_kill(lock_guard<mutex> const& streamMutexLock, string const& note) {
    LOGS(_log, LOG_LVL_DEBUG, "FileChannelShared::kill() called " << note);
    bool ret = _sendChannel->kill(note);
    _lastRecvd = true;
    return ret;
}

bool FileChannelShared::_transmit(lock_guard<mutex> const& tMtxLock, lock_guard<mutex> const& queueMtxLock,
                                  bool erred, shared_ptr<Task> const& task) {
    string idStr = "QID?";

    // Result data is transmitted in messages containing data and headers.
    // data - is the result data
    // header - contains information about the next chunk of result data,
    //          most importantly the size of the next data message.
    //          The header has a fixed size (about 255 bytes)
    // header_END - indicates there will be no more msg.
    // msg - contains data and header.
    // metadata - special xrootd buffer that can only be set once per ChannelShared
    //            instance. It is used to send the first header.
    // A complete set of results to the czar looks like
    //    metadata[header_A] -> msg_A[data_A, header_END]
    // or
    //    metadata[header_A] -> msg_A[data_A, header_B]
    //          -> msg_B[data_B, header_C] -> ... -> msg_X[data_x, header_END]
    //
    // Since you can't send msg_A until you know the size of data_B, you can't
    // transmit until there are at least 2 msg in the queue, or you know
    // that msg_A is the last msg in the queue.
    // Note that the order of result rows does not matter, but data_B must come after header_B.
    // Keep looping until nothing more can be transmitted.
    while (_transmitQueue.size() >= 2 || _lastRecvd) {
        shared_ptr<TransmitData> thisTransmit = _transmitQueue.front();
        _transmitQueue.pop();
        if (thisTransmit == nullptr) {
            throw util::Bug(ERR_LOC, "_transmitLoop() _transmitQueue had nullptr!");
        }

        auto sz = _transmitQueue.size();
        // Is this really the last message for this SharedSendChannel?
        bool reallyLast = (_lastRecvd && sz == 0);

        shared_ptr<TransmitData> nextTr;
        if (sz != 0) {
            nextTr = _transmitQueue.front();
            if (nextTr->getResultSize() == 0) {
                LOGS(_log, LOG_LVL_ERROR,
                     "RESULT SIZE IS 0, this should not happen thisTr=" << thisTransmit->dump()
                                                                        << " nextTr=" << nextTr->dump());
            }
        }
        thisTransmit->attachNextHeader(nextTr, reallyLast);

        // The first message needs to put its header data in metadata as there's
        // no previous message it could attach its header to.
        {
            lock_guard<mutex> const streamMutexLock(_streamMutex);  // Must keep meta and buffer together.
            if (_firstTransmit.exchange(false)) {
                // Put the header for the first message in metadata
                // _metaDataBuf must remain valid until Finished() is called.
                string thisHeaderString = thisTransmit->getHeaderString();
                _metadataBuf = proto::ProtoHeaderWrap::wrap(thisHeaderString);
                bool metaSet = _sendChannel->setMetadata(_metadataBuf.data(), _metadataBuf.size());
                if (!metaSet) {
                    LOGS(_log, LOG_LVL_ERROR, "Failed to setMeta " << idStr);
                    _kill(streamMutexLock, "metadata");
                    return false;
                }
            }

            // Put the data for the transmit in a StreamBuffer and send it.
            // Since the StreamBuffer's lifetime is beyond our control, it needs
            // its own Task pointer.
            auto streamBuf = thisTransmit->getStreamBuffer(task);
            streamBuf->startTimer();
            bool sent = _sendBuf(tMtxLock, queueMtxLock, streamMutexLock, streamBuf, reallyLast,
                                 "transmitLoop " + idStr);

            if (!sent) {
                LOGS(_log, LOG_LVL_ERROR, "Failed to send " << idStr);
                _kill(streamMutexLock, "FileChannelShared::_transmit b");
                return false;
            }
        }
        // If that was the last message, break the loop.
        if (reallyLast) return true;
    }
    return true;
}

bool FileChannelShared::_sendBuf(lock_guard<mutex> const& tMtxLock, lock_guard<mutex> const& queueMtxLock,
                                 lock_guard<mutex> const& streamMutexLock,
                                 shared_ptr<xrdsvc::StreamBuffer>& streamBuf, bool last, string const& note) {
    bool sent = _sendChannel->sendStream(streamBuf, last);
    if (!sent) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to transmit " << note << "!");
        return false;
    } else {
        LOGS(_log, LOG_LVL_INFO, "_sendbuf wait start " << note);
        streamBuf->waitForDoneWithThis();  // Block until this buffer has been sent.
    }
    return sent;
}

}  // namespace lsst::qserv::wbase
