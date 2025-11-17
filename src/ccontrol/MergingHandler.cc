// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "ccontrol/MergingHandler.h"

// System headers
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

// Third-party headers
#include "curl/curl.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "ccontrol/msgCode.h"
#include "global/clock_defs.h"
#include "global/debugUtil.h"
#include "http/Client.h"
#include "http/ClientConnPool.h"
#include "http/Method.h"
#include "mysql/CsvMemDisk.h"
#include "qdisp/CzarStats.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "qdisp/UberJob.h"
#include "rproc/InfileMerger.h"
#include "util/Bug.h"
#include "util/common.h"

namespace http = lsst::qserv::http;
namespace qdisp = lsst::qserv::qdisp;

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.MergingHandler");

/**
 * Instances of this class are used to update statistic counter on starting
 * and finishing operations with the result files.
 */
class ResultFileTracker {
public:
    ResultFileTracker() { lsst::qserv::qdisp::CzarStats::get()->addResultFile(); }
    ~ResultFileTracker() { lsst::qserv::qdisp::CzarStats::get()->deleteResultFile(); }
};

// The logging function employed by the transmit rate tracker to report
// the data transfer rates in a histogram. The histogram is used in
// the performance monitoring of the application.
lsst::qserv::TimeCountTracker<double>::CALLBACKFUNC const reportFileRecvRate =
        [](lsst::qserv::TIMEPOINT start, lsst::qserv::TIMEPOINT end, double bytes, bool success) {
            if (!success) return;
            if (chrono::duration<double> const seconds = end - start; seconds.count() > 0) {
                lsst::qserv::qdisp::CzarStats::get()->addFileReadRate(bytes / seconds.count());
            }
        };

string readHttpFileAndMerge(lsst::qserv::qdisp::UberJob::Ptr const& uberJob, string const& httpUrl,
                            size_t fileSize, function<void(char const*, uint32_t)> const& messageIsReady,
                            shared_ptr<http::ClientConnPool> const& httpConnPool) {
    string const context = "MergingHandler::" + string(__func__) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context << "httpUrl=" << httpUrl);

    // Track the file while the control flow is staying within the function.
    ResultFileTracker const resultFileTracker;

    // The data transmit rate tracker is set up before reading each data message.
    unique_ptr<lsst::qserv::TimeCountTracker<double>> transmitRateTracker;

    // A location of the next byte to be read from the input file. The variable
    // is used for error reporting.
    size_t offset = 0;

    try {
        auto exec = uberJob->getExecutive();
        if (exec == nullptr || exec->getCancelled()) {
            throw runtime_error(context + " query was cancelled");
        }
        string const noClientData;
        vector<string> const noClientHeaders;
        http::ClientConfig clientConfig;
        clientConfig.httpVersion = CURL_HTTP_VERSION_1_1;  // same as in qhttp
        clientConfig.bufferSize = CURL_MAX_READ_SIZE;      // 10 MB in the current version of libcurl
        clientConfig.tcpKeepAlive = true;
        clientConfig.tcpKeepIdle = 5;   // the default is 60 sec
        clientConfig.tcpKeepIntvl = 5;  // the default is 60 sec
        http::Client reader(http::Method::GET, httpUrl, noClientData, noClientHeaders, clientConfig,
                            httpConnPool);

        // Starts the tracker to measure the performance of the network I/O.
        transmitRateTracker = make_unique<lsst::qserv::TimeCountTracker<double>>(reportFileRecvRate);

        // Start reading the file. The read() method will call the callback function
        // for each chunk of data read from the file.
        reader.read([&](char const* inBuf, size_t inBufSize) {
            // Check if the end of the file has been reached.
            // Destroying the tracker will result in stopping the tracker's timer and
            // reporting the file read rate before proceeding to the merge.
            transmitRateTracker->addToValue(inBufSize);
            transmitRateTracker->setSuccess();
            transmitRateTracker.reset();
            messageIsReady(inBuf, inBufSize);
            offset += inBufSize;
            // Restart the tracker to measure the reading performance of the next chunk of data.
            transmitRateTracker = make_unique<lsst::qserv::TimeCountTracker<double>>(reportFileRecvRate);
        });

        if (offset != fileSize) {
            throw runtime_error(context + "short read");
        }
    } catch (exception const& ex) {
        string const errMsg = "failed to open/read: " + httpUrl + ", fileSize: " + to_string(fileSize) +
                              ", offset: " + to_string(offset) + ", ex: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, context << errMsg);
        return errMsg;
    }

    // Remove the file from the worker if it still exists. Report and ignore errors.
    // The files will be garbage-collected by workers.
    try {
        http::Client remover(http::Method::DELETE, httpUrl);
        remover.read([](char const* inBuf, size_t inBufSize) {});
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, context << "failed to remove " << httpUrl << ", ex: " << ex.what());
    }

    return string();
}

}  // namespace

namespace lsst::qserv::ccontrol {

shared_ptr<http::ClientConnPool> MergingHandler::_httpConnPool;
mutex MergingHandler::_httpConnPoolMutex;

shared_ptr<http::ClientConnPool> const& MergingHandler::_getHttpConnPool() {
    lock_guard<mutex> const lock(_httpConnPoolMutex);
    if (nullptr == _httpConnPool) {
        _httpConnPool = make_shared<http::ClientConnPool>(
                cconfig::CzarConfig::instance()->getResultMaxHttpConnections());
    }
    return _httpConnPool;
}

MergingHandler::MergingHandler(std::shared_ptr<rproc::InfileMerger> const& merger,
                               std::shared_ptr<qdisp::Executive> const& exec)
        : _infileMerger(merger), _executive(exec) {}

MergingHandler::~MergingHandler() { LOGS(_log, LOG_LVL_TRACE, __func__); }

void MergingHandler::errorFlush(std::string const& msg, int code) {
    _setError(code, msg, util::ErrorCode::RESULT_IMPORT);
    // Might want more info from result service.
    // Do something about the error. FIXME.
    LOGS(_log, LOG_LVL_ERROR, "Error receiving result.");
}

std::ostream& MergingHandler::print(std::ostream& os) const {
    return os << "MergingRequester(flushed=" << (_flushed ? "true)" : "false)");
}

qdisp::MergeEndStatus MergingHandler::_mergeHttp(qdisp::UberJob::Ptr const& uberJob, string const& fileUrl,
                                                 uint64_t fileSize) {
    if (_flushed) {
        throw util::Bug(ERR_LOC, "already flushed");
    }

    if (fileSize == 0) return qdisp::MergeEndStatus(true);
    auto csvMemDisk = mysql::CsvMemDisk::create(fileSize, uberJob->getQueryId(), uberJob->getUjId());
    _csvMemDisk = csvMemDisk;

    // This must be after setting _csvStream to avoid cancelFileMerge()
    // race issues, and it needs to be before the thread starts.
    auto exec = uberJob->getExecutive();
    if (exec == nullptr || exec->getCancelled() || exec->isRowLimitComplete()) {
        return qdisp::MergeEndStatus(true);
    }

    string fileReadErrorMsg;
    auto transferFunc = [&]() {
        size_t bytesRead = 0;
        fileReadErrorMsg = ::readHttpFileAndMerge(
                uberJob, fileUrl, fileSize,
                [&](char const* buf, uint32_t size) {
                    bool last = false;
                    if (buf == nullptr || size == 0) {
                        last = true;
                    } else {
                        csvMemDisk->push(buf, size);
                        bytesRead += size;
                        last = bytesRead >= fileSize;
                    }
                    if (last) {
                        csvMemDisk->push(nullptr, 0);
                    }
                },
                MergingHandler::_getHttpConnPool());
        // Push the stream terminator to indicate the end of the stream.
        // It may be needed to unblock the table merger which may be still attempting to read
        // from the CSV stream.
        if (!fileReadErrorMsg.empty()) {
            csvMemDisk->push(nullptr, 0);
        }
    };
    csvMemDisk->transferDataFromWorker(transferFunc);
    if (csvMemDisk->isCancelled()) {
        // Since csvMemDisk was cancelled, avoid merging to avoid risks of contamination.
        LOGS(_log, LOG_LVL_DEBUG, __func__ << " csvMemDisk cancelled");
        return qdisp::MergeEndStatus(false);
    }

    bool mergeOk = _startMerge();
    if (!mergeOk) {
        LOGS(_log, LOG_LVL_DEBUG, __func__ << " merge cancelled");
        return qdisp::MergeEndStatus(false);
    }

    // Attempt the actual merge.
    bool fileMergeSuccess = _infileMerger->mergeHttp(uberJob, fileSize, csvMemDisk);
    if (!fileMergeSuccess) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " merge failed");
        util::Error const& err = _infileMerger->getError();
        _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg(), util::ErrorCode::RESULT_IMPORT);
    }
    if (csvMemDisk->getContaminated()) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " merge stream contaminated");
        fileMergeSuccess = false;
        _setError(ccontrol::MSG_RESULT_ERROR, "merge stream contaminated", util::ErrorCode::RESULT_IMPORT);
    }

    if (!fileReadErrorMsg.empty()) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " result file read failed");
        _setError(ccontrol::MSG_HTTP_RESULT, fileReadErrorMsg, util::ErrorCode::RESULT_IMPORT);
    }
    _flushed = true;

    qdisp::MergeEndStatus mergeEStatus(fileMergeSuccess && fileReadErrorMsg.empty());
    if (!mergeEStatus.success) {
        // This error check needs to come after the csvThread.join() to ensure writing
        // is finished. If any bytes were written, the result table is ruined.
        mergeEStatus.contaminated = csvMemDisk->getBytesFetched() > 0;
    }

    return mergeEStatus;
}

bool MergingHandler::cancelFileMerge() {
    lock_guard mergeStateLock(_mergeStateMtx);
    if (_mergeState == PREMERGE || _mergeState == CANCELLED) {
        _mergeState = CANCELLED;
        auto csvStrm = _csvMemDisk.lock();
        if (csvStrm != nullptr) {
            csvStrm->cancel();
        }
        // Merging to the result table hasn't been started, so
        // this can be cancelled.
        return true;
    }
    // Cancelling at this point would probably corrupt the result table.
    return false;
}

bool MergingHandler::_startMerge() {
    lock_guard mergeStateLock(_mergeStateMtx);
    if (_mergeState == PREMERGE) {
        _mergeState = MERGING;
        // Merging hasn't been cancelled, so it's ok to start.
        return true;
    }
    // Merge was cancelled.
    return false;
}

void MergingHandler::_setError(int code, std::string const& msg, int errorState) {
    LOGS(_log, LOG_LVL_DEBUG, "_setError: code: " << code << ", message: " << msg);
    auto exec = _executive.lock();
    if (exec == nullptr) return;
    exec->addMultiError(code, msg, errorState);
}

qdisp::MergeEndStatus MergingHandler::flushHttp(string const& fileUrl, uint64_t fileSize) {
    // This is needed to ensure the job query would be staying alive for the duration
    // of the operation to prevent inconsistency within the application.
    auto const uberJob = getUberJob().lock();
    if (uberJob == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " failed, uberJob was NULL");
        return qdisp::MergeEndStatus(false);
    }

    LOGS(_log, LOG_LVL_TRACE,
         "MergingHandler::" << __func__ << " uberJob=" << uberJob->getIdStr() << " fileUrl=" << fileUrl);

    qdisp::MergeEndStatus mergeStatus = _mergeHttp(uberJob, fileUrl, fileSize);
    return mergeStatus;
}

void MergingHandler::flushHttpError(int errorCode, std::string const& errorMsg, int errState) {
    if (!_errorSet.exchange(true)) {
        _setError(errorCode, errorMsg, errState);
    }
}

}  // namespace lsst::qserv::ccontrol
