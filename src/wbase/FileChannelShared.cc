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
#include "mysql/MySqlUtils.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "wbase/UberJobData.h"
#include "wconfig/WorkerConfig.h"
#include "wpublish/QueriesAndChunks.h"
#include "util/Bug.h"
#include "util/Error.h"
#include "util/MultiError.h"
#include "util/ResultFileName.h"
#include "util/Timer.h"
#include "util/TimeUtils.h"

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
           (filePath.extension() == util::ResultFileName::fileExt);
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
                    auto const fileAttributes = util::ResultFileName(fileName);
                    return (fileAttributes.czarId() == czarId) && (fileAttributes.queryId() <= queryId);
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
                    auto const fileAttributes = util::ResultFileName(fileName);
                    return (fileAttributes.czarId() == czarId) && (fileAttributes.queryId() == queryId);
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
    fs::path const dirPath = config->resultsDirname();
    json result = json::object({{"folder", dirPath.string()},
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
                json const jsonTask = util::ResultFileName(filePath).toJson();
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
                                                        qmeta::CzarId czarId, string const& workerId) {
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    return shared_ptr<FileChannelShared>(new FileChannelShared(sendChannel, czarId, workerId));
}

FileChannelShared::FileChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel, qmeta::CzarId czarId,
                                     string const& workerId)
        : _sendChannel(sendChannel),
          _uberJobId(0),
          _czarId(czarId),
          _czarHostName(""),  ///< Name of the czar host.
          _czarPort(-1),
          _workerId(workerId) {
    LOGS(_log, LOG_LVL_DEBUG, "FileChannelShared created");
    if (_sendChannel == nullptr) {
        throw util::Bug(ERR_LOC, "FileChannelShared constructor given nullptr");
    }
}

FileChannelShared::Ptr FileChannelShared::create(std::shared_ptr<wbase::UberJobData> const& uberJob,
                                                 qmeta::CzarId czarId, string const& czarHostName,
                                                 int czarPort, string const& workerId) {
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    return Ptr(new FileChannelShared(uberJob, czarId, czarHostName, czarPort, workerId));
}

FileChannelShared::FileChannelShared(std::shared_ptr<wbase::UberJobData> const& uberJobData,
                                     qmeta::CzarId czarId, string const& czarHostName, int czarPort,
                                     string const& workerId)
        : _sendChannel(nullptr),
          _uberJobData(uberJobData),
          _uberJobId(uberJobData->getUberJobId()),
          _czarId(czarId),
          _czarHostName(czarHostName),
          _czarPort(czarPort),
          _workerId(workerId) {
    LOGS(_log, LOG_LVL_TRACE, "FileChannelShared created ujId=" << _uberJobId);
}

FileChannelShared::~FileChannelShared() {
    LOGS(_log, LOG_LVL_TRACE, "~FileChannelShared ujId=" << _uberJobId);
    // Normally, the channel should not be dead at this time. If it's already
    // dead it means there was a problem to process a query or send back a response
    // to Czar. In either case, the file would be useless and it has to be deleted
    // in order to avoid leaving unclaimed result files within the results folder.
    //
    // _rowLimitComplete confuses things as it can cause other Tasks using this
    // file to be cancelled, but the file should not be deleted until collected.
    // In any case, the WorkerQueryStatusData message from the czar will delete
    // the file when the user query completes.
    if (isDead() && !_rowLimitComplete) {
        _removeFile(lock_guard<mutex>(_tMtx));
    }
}

void FileChannelShared::setTaskCount(int taskCount) { _taskCount = taskCount; }

bool FileChannelShared::transmitTaskLast(bool rowLimitComplete) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    ++_lastCount;
    if (rowLimitComplete) {
        // There are enough rows in the file so other tasks can be ignored.
        if (_rowLimitComplete.exchange(true) == false) {
            // This is TaskLast.
            return true;
        } else {
            // A different task set _rowLimitComplete before
            // this one. Since there can be only one TaskLast,
            // it is not this one.
            return false;
        }
    }
    bool lastTaskDone = _lastCount >= _taskCount;
    return lastTaskDone;
}

bool FileChannelShared::kill(string const& note) {
    lock_guard<mutex> const streamMutexLock(_streamMutex);
    return _kill(streamMutexLock, note);
}

bool FileChannelShared::isDead() const { return _dead; }

string FileChannelShared::makeIdStr(int qId, int jId) {
    string str("QID" + (qId == 0 ? "" : to_string(qId) + "#" + to_string(jId)));
    return str;
}

bool FileChannelShared::isRowLimitComplete() const {
    lock_guard<mutex> const tMtxLock(_tMtx);
    return _rowLimitComplete;
}

void FileChannelShared::buildAndTransmitError(util::MultiError& multiErr, shared_ptr<Task> const& task,
                                              bool cancelled) {
    lock_guard<mutex> const tMtxLock(_tMtx);
    if (_rowLimitComplete) {
        LOGS(_log, LOG_LVL_WARN,
             __func__ << " already enough rows, this call likely a side effect" << task->getIdStr());
        return;
    }
    // Delete the result file as nobody will come looking for it.
    _kill(tMtxLock, " buildAndTransmitError");
    auto ujd = _uberJobData.lock();
    if (ujd != nullptr) {
        ujd->responseError(multiErr, task->getChunkId(), cancelled, task->getLvlET());
    }
}

bool FileChannelShared::buildAndTransmitResult(MYSQL_RES* mResult, shared_ptr<Task> const& task,
                                               util::MultiError& multiErr, atomic<bool>& cancelled) {
    // Operation stats. Note that "buffer fill time" included the amount
    // of time needed to write the result set to disk.
    util::Timer transmitT;
    transmitT.start();

    double bufferFillSecs = 0.0;
    uint64_t bytes = 0;
    uint64_t rows = 0;

    // Keep reading rows and converting those into messages while any
    // are still left in the result set. The row processing method
    // will write rows into the output file. The final "summary" message
    // will be sant back to Czar after processing the very last set of rows
    // of the last task of a request.
    bool erred = false;

    if (!cancelled) {
        // This lock is to protect the stream from having other Tasks mess with it
        // while data is loading.
        lock_guard<mutex> const tMtxLockA(_tMtx);
        if (_rowLimitComplete) {
            LOGS(_log, LOG_LVL_DEBUG, __func__ << " already enough rows, returning " << task->getIdStr());
            // Deleting the file now could be risky.
            return erred;
        }

        // Extract the result set and write it into the file.
        util::Timer bufferFillT;
        bufferFillT.start();

        _writeToFile(tMtxLockA, task, mResult, bytes, rows, multiErr);
        _rowcount += rows;
        _transmitsize += bytes;
        LOGS(_log, LOG_LVL_TRACE,
             __func__ << " " << task->getIdStr() << " bytesT=" << bytes << " _tsz=" << _transmitsize);

        bufferFillT.stop();
        bufferFillSecs += bufferFillT.getElapsed();

        uint64_t const maxTableSize = task->getMaxTableSize();
        // Fail the operation if the amount of data in the result set exceeds the requested
        // "large result" limit (in case one was specified).
        LOGS(_log, LOG_LVL_TRACE, "bytesWritten=" << _bytesWritten << " max=" << maxTableSize);
        if (maxTableSize > 0 && _bytesWritten > maxTableSize) {
            string const err = "The result set size " + to_string(_bytesWritten) +
                               " of a job exceeds the requested limit of " + to_string(maxTableSize) +
                               " bytes, task: " + task->getIdStr();
            multiErr.push_back(util::Error(util::ErrorCode::WORKER_RESULT_TOO_LARGE, err));
            LOGS(_log, LOG_LVL_ERROR, err);
            erred = true;
            return erred;
        }

        int const ujRowLimit = task->getRowLimit();
        bool rowLimitComplete = false;
        if (ujRowLimit > 0 && _rowcount >= ujRowLimit) {
            // There are enough rows to satisfy the query, so stop reading
            rowLimitComplete = true;
            LOGS(_log, LOG_LVL_DEBUG,
                 __func__ << " enough rows for query rows=" << _rowcount << " " << task->getIdStr());
        }

        // If no more rows are left in the task's result set then we need to check
        // if this is last task in a logical group of ones created for processing
        // the current request (note that certain classes of requests may require
        // more than one task for processing).
        if (transmitTaskLast(rowLimitComplete)) {
            // Make sure the file is sync to disk before notifying Czar.
            _file.flush();
            _file.close();

            // Only the last ("summary") message, w/o any rows, is sent to the Czar to notify
            // it about the completion of the request.
            LOGS(_log, LOG_LVL_DEBUG, "FileChannelShared " << task->cName(__func__) << " sending start");
            if (!_sendResponse(tMtxLockA, task, cancelled, multiErr, rowLimitComplete)) {
                LOGS(_log, LOG_LVL_ERROR, "Could not transmit the request completion message to Czar.");
                erred = true;
            } else {
                LOGS(_log, LOG_LVL_TRACE, __func__ << " " << task->getIdStr() << " sending done!!!");
            }
        }
    }
    transmitT.stop();
    double timeSeconds = transmitT.getElapsed();
    auto qStats = task->getQueryStats();
    if (qStats == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "No statistics for " << task->getIdStr());
    } else {
        qStats->addTaskTransmit(timeSeconds, bytes, rows, bufferFillSecs);
        LOGS(_log, LOG_LVL_TRACE,
             "TaskTransmit time=" << timeSeconds << " bufferFillSecs=" << bufferFillSecs);
    }

    // No reason to keep the file after a failure (hit while processing a query,
    // extracting a result set into the file) or query cancellation. This also
    // includes problems encountered while sending a response back to Czar after
    // successfully processing the query and writing all results into the file.
    // The file is not going to be used by Czar in either of these scenarios.
    if ((cancelled || erred || isDead()) && !_rowLimitComplete) {
        lock_guard<mutex> const tMtxLockA(_tMtx);
        _removeFile(tMtxLockA);
    }
    return erred;
}

bool FileChannelShared::_kill(lock_guard<mutex> const& streamMutexLock, string const& note) {
    LOGS(_log, LOG_LVL_TRACE, "FileChannelShared::" << __func__ << " " << note);
    bool oldVal = _dead.exchange(true);
    if (!oldVal) {
        LOGS(_log, LOG_LVL_WARN, "FileChannelShared::" << __func__ << " first kill call " << note);
    }
    return oldVal;
}

void FileChannelShared::_writeToFile(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     MYSQL_RES* mResult, uint64_t& bytes, uint64_t& rows,
                                     util::MultiError& multiErr) {
    if (!_file.is_open()) {
        _fileName = task->getUberJobData()->resultFilePath();
        _file.open(_fileName, ios::out | ios::trunc | ios::binary);
        if (!(_file.is_open() && _file.good())) {
            throw runtime_error("FileChannelShared::" + string(__func__) +
                                " failed to create/truncate the file '" + _fileName + "'.");
        }
    }

    // Transfer rows from a result set into the file. Count the number of bytes
    // written into the file and the number of rows processed.
    string const fieldEndsWith = "\t";
    string const rowEndsWith = "\n";
    string const mysqlNull("\\N");

    int const numFields = mysql_num_fields(mResult);
    bytes = 0;
    rows = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(mResult))) {
        auto lengths = mysql_fetch_lengths(mResult);
        for (int i = 0; i < numFields; ++i) {
            if (i != 0) {
                bytes += _writeStringToFile(fieldEndsWith);
            }
            if (row[i] == nullptr) {
                bytes += _writeStringToFile(mysqlNull);
            } else {
                string escapedQuotedString;
                mysql::escapeAppendString(escapedQuotedString, row[i], lengths[i]);
                bytes += _writeStringToFile(escapedQuotedString);
            }
        }
        bytes += _writeStringToFile(rowEndsWith);
        ++rows;
    }
    if (!(_file.is_open() && _file.good())) {
        throw runtime_error("FileChannelShared::" + string(__func__) + " failed to write " +
                            to_string(bytes) + " bytes into the file '" + _fileName + "'.");
    }
}

void FileChannelShared::_removeFile(lock_guard<mutex> const& tMtxLock) {
    LOGS(_log, LOG_LVL_TRACE, "FileChannelShared::_removeFile " << _fileName);
    if (!_fileName.empty()) {
        if (_file.is_open()) {
            _file.close();
        }
        boost::system::error_code ec;
        LOGS(_log, LOG_LVL_DEBUG, "FileChannelShared::" << __func__ << " removing " << _fileName);
        fs::remove_all(fs::path(_fileName), ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_WARN,
                 "FileChannelShared::" << __func__ << " failed to remove the result file '" << _fileName
                                       << "', ec: " << ec << ".");
            return;
        }
    }
    _fileName.clear();
}

bool FileChannelShared::_sendResponse(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                      bool cancelled, util::MultiError const& multiErr, bool mustSend) {
    auto const queryId = task->getQueryId();
    auto const jobId = task->getJobId();  // TODO:UJ this should be UberJobId
    auto const idStr(makeIdStr(queryId, jobId));

    // This lock is required for making consistent modifications and usage of the metadata
    // and response buffers.
    lock_guard<mutex> const streamMutexLock(_streamMutex);

    QSERV_LOGCONTEXT_QUERY_JOB(queryId, jobId);
    LOGS(_log, LOG_LVL_DEBUG, __func__);

    if (isDead() && !mustSend) {
        LOGS(_log, LOG_LVL_INFO, __func__ << ": aborting transmit since sendChannel is dead.");
        return false;
    }

    // Prepare the response object and put into a message that will
    // be sent to the Czar.
    string httpFileUrl = task->getUberJobData()->resultFileHttpUrl();
    auto ujd = _uberJobData.lock();
    if (ujd != nullptr) {
        ujd->responseFileReady(httpFileUrl, _rowcount, _transmitsize);
    }
    return true;
}

}  // namespace lsst::qserv::wbase
