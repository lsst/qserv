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
#include <set>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"
#include "boost/range/iterator_range.hpp"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "wconfig/WorkerConfig.h"
#include "wpublish/QueriesAndChunks.h"
#include "util/MultiError.h"
#include "util/String.h"
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

string const resultFileExt = ".proto";

bool isResultFile(fs::path const& filePath) {
    return filePath.has_filename() && filePath.has_extension() && (filePath.extension() == resultFileExt);
}

/**
 * Extract task attributes from the file path.
 * The file path is required to have the following format:
 * @code
 *   [<folder>/]<query-id>-<job-id>-<chunk-id>-<attemptcount>[.<ext>]
 * @code
 * @param filePath The file to be evaluated.
 * @return nlohmann::json::object Task attributes.
 * @throw std::invalid_argument If the file path did not match expectations.
 */
json file2task(fs::path const& filePath) {
    vector<std::uint64_t> const taskAttributes =
            util::String::parseToVectUInt64(filePath.stem().string(), "-");
    if (taskAttributes.size() != 4) {
        throw invalid_argument("FileChannelShared::" + string(__func__) +
                               " not a valid result file: " + filePath.string());
    }
    return json::object({{"query_id", taskAttributes[0]},
                         {"job_id", taskAttributes[1]},
                         {"chunk_id", taskAttributes[2]},
                         {"attemptcount", taskAttributes[3]}});
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

void FileChannelShared::cleanUpResultsOnCzarRestart(QueryId queryId) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    fs::path const dirPath = wconfig::WorkerConfig::instance()->resultsDirname();
    LOGS(_log, LOG_LVL_INFO,
         context << "removing result files from " << dirPath << " for queryId=" << queryId << " or older.");
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    size_t const numFilesRemoved =
            ::cleanUpResultsImpl(context, dirPath, [queryId, &context](string const& fileName) -> bool {
                try {
                    // Names of the result files begin with identifiers of the corresponding queries:
                    // '<id>-...'
                    auto const pos = fileName.find_first_of('-');
                    return (pos != string::npos) && (pos != 0) &&
                           (stoull(fileName.substr(0, pos)) <= queryId);
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_WARN,
                         context << "failed to locate queryId in the file name " << fileName
                                 << ", ex: " << ex.what());
                    return false;
                }
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

void FileChannelShared::cleanUpResults(QueryId queryId) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    fs::path const dirPath = wconfig::WorkerConfig::instance()->resultsDirname();
    string const queryIdPrefix = to_string(queryId) + "-";
    LOGS(_log, LOG_LVL_INFO,
         context << "removing result files from " << dirPath << " for queryId=" << queryId << ".");
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    size_t const numFilesRemoved =
            ::cleanUpResultsImpl(context, dirPath, [&queryIdPrefix](string const& fileName) -> bool {
                // Names of the result files begin with identifiers of the corresponding queries:
                // '<id>-...'
                return fileName.substr(0, queryIdPrefix.size()) == queryIdPrefix;
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
                json const jsonTask = ::file2task(filePath);
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

FileChannelShared::Ptr FileChannelShared::create(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                                 shared_ptr<proto::TaskMsg> const& taskMsg) {
    lock_guard<mutex> const lock(_resultsDirCleanupMtx);
    return shared_ptr<FileChannelShared>(new FileChannelShared(sendChannel, transmitMgr, taskMsg));
}

FileChannelShared::FileChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                                     shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                     shared_ptr<proto::TaskMsg> const& taskMsg)
        : ChannelShared(sendChannel, transmitMgr, taskMsg->czarid()) {}

FileChannelShared::~FileChannelShared() {
    // Normally, the channel should not be dead before the base class's d-tor
    // gets called. If it's already dead it means there was a problem to process
    // a query or send back a response to Czar. In either case, the file
    // would be useless and it has to be deleted to avoid leaving unclaimed
    // result files at the results folder.
    if (isDead()) {
        _removeFile(lock_guard<mutex>(tMtx));
    }
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

}  // namespace lsst::qserv::wbase
