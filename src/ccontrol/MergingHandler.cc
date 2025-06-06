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
#include "XrdCl/XrdClFile.hh"

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
#include "mysql/CsvBuffer.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "qdisp/CzarStats.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "rproc/InfileMerger.h"
#include "util/Bug.h"
#include "util/common.h"

using lsst::qserv::proto::ProtoHeaderWrap;
using lsst::qserv::proto::ResponseData;
using lsst::qserv::proto::ResponseSummary;
namespace http = lsst::qserv::http;

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

/**
 * This exception is used by the merging handler to signal the file reader
 * that the query has been ended before the file has been completely read.
 * The exception is meant to tell the reader to stop reading the file
 * and return control to the caller.
 */
class QueryEnded : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

string readHttpFileAndMerge(string const& httpUrl, size_t fileSize,
                            function<void(char const*, uint32_t)> const& messageIsReady,
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
    } catch (QueryEnded const& ex) {
        // This is a normal condition which should be handled gracefully by the algorithm.
        LOGS(_log, LOG_LVL_DEBUG, context << ex.what() << ", httpUrl=" << httpUrl);
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

MergingHandler::MergingHandler(std::shared_ptr<rproc::InfileMerger> merger, std::string const& tableName)
        : _infileMerger{merger}, _tableName{tableName} {
    _initState();
}

MergingHandler::~MergingHandler() { LOGS(_log, LOG_LVL_DEBUG, __func__); }

bool MergingHandler::flush(proto::ResponseSummary const& responseSummary) {
    _wName = responseSummary.wname();

    // This is needed to ensure the job query would be staying alive for the duration
    // of the operation to prevent inconsistency witin the application.
    auto const jobQuery = getJobQuery().lock();
    if (jobQuery == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " failed, jobQuery was NULL");
        return false;
    }
    LOGS(_log, LOG_LVL_TRACE,
         "MergingHandler::" << __func__ << " jobid=" << responseSummary.jobid()
                            << " transmitsize=" << responseSummary.transmitsize()
                            << " rowcount=" << responseSummary.rowcount() << " rowSize="
                            << " attemptcount=" << responseSummary.attemptcount() << " errorcode="
                            << responseSummary.errorcode() << " errormsg=" << responseSummary.errormsg());

    if (responseSummary.errorcode() != 0 || !responseSummary.errormsg().empty()) {
        _error = util::Error(responseSummary.errorcode(), responseSummary.errormsg(),
                             util::ErrorCode::MYSQLEXEC);
        _setError(ccontrol::MSG_RESULT_ERROR, _error.getMsg());
        LOGS(_log, LOG_LVL_ERROR,
             "MergingHandler::" << __func__ << " error from worker:" << responseSummary.wname()
                                << " error: " << _error);
        return false;
    }

    bool const success = _merge(responseSummary, jobQuery);
    if (success) {
        _infileMerger->mergeCompleteFor(responseSummary.jobid());
        qdisp::CzarStats::get()->addTotalRowsRecv(responseSummary.rowcount());
        qdisp::CzarStats::get()->addTotalBytesRecv(responseSummary.transmitsize());
    }
    return success;
}

void MergingHandler::errorFlush(std::string const& msg, int code) {
    _setError(code, msg);
    // Might want more info from result service.
    // Do something about the error. FIXME.
    LOGS(_log, LOG_LVL_ERROR, "Error receiving result.");
}

bool MergingHandler::finished() const { return _flushed; }

bool MergingHandler::reset() {
    // If we've pushed any bits to the merger successfully, we have to undo them
    // to reset to a fresh state. For now, we will just fail if we've already
    // begun merging. If we implement the ability to retract a partial result
    // merge, then we can use it and do something better.
    if (_flushed) {
        return false;  // Can't reset if we have already pushed state.
    }
    _initState();
    return true;
}

std::ostream& MergingHandler::print(std::ostream& os) const {
    return os << "MergingRequester(" << _tableName << ", flushed=" << (_flushed ? "true)" : "false)");
}

void MergingHandler::_initState() { _setError(util::ErrorCode::NONE, string()); }

bool MergingHandler::_queryIsNoLongerActive(shared_ptr<qdisp::JobQuery> const& jobQuery) const {
    // Check if the query got cancelled for any reason.
    if (jobQuery->isQueryCancelled()) return true;

    // Check for other indicators that the query may have cancelled or finished.
    auto executive = jobQuery->getExecutive();
    if (executive == nullptr || executive->getCancelled() || executive->isLimitRowComplete()) {
        return true;
    }

    // The final test is to see if any errors have been reported in a context
    // of the merger. A presence of errors means that further attempting of merging
    // makes no sense.
    return !getError().isNone();
}

bool MergingHandler::_merge(proto::ResponseSummary const& responseSummary,
                            shared_ptr<qdisp::JobQuery> const& jobQuery) {
    if (_flushed) throw util::Bug(ERR_LOC, "already flushed");
    if (responseSummary.transmitsize() == 0) return true;

    // After this final test the job's result processing can't be interrupted.
    if (_queryIsNoLongerActive(jobQuery)) return true;

    // Read from the http stream and push records into the CSV stream in a separate thread.
    // Note the fixed capacity of the stream which allows up to 2 records to be buffered
    // in the stream. This is enough to hide the latency of the HTTP connection and
    // the time needed to read the file.
    auto csvStream = mysql::CsvStream::create(2);
    string fileReadErrorMsg;
    thread csvThread([&]() {
        size_t bytesRead = 0;
        fileReadErrorMsg = ::readHttpFileAndMerge(
                responseSummary.fileresource_http(), responseSummary.transmitsize(),
                [&](char const* buf, uint32_t size) {
                    bool const queryEnded = _queryIsNoLongerActive(jobQuery);
                    bool last = false;
                    if (buf == nullptr || size == 0 || queryEnded) {
                        last = true;
                    } else {
                        csvStream->push(buf, size);
                        bytesRead += size;
                        last = bytesRead >= responseSummary.transmitsize();
                    }
                    if (last) {
                        csvStream->push(nullptr, 0);
                        if (queryEnded) {
                            throw ::QueryEnded(
                                    "query " + jobQuery->getIdStr() +
                                    " ended while reading the file, bytesRead=" + to_string(bytesRead) +
                                    ", transmitsize=" + to_string(responseSummary.transmitsize()));
                        }
                    }
                },
                MergingHandler::_getHttpConnPool());
        // Push the stream terminator to indicate the end of the stream.
        // It may be neeeded to unblock the table merger which may be still attempting to read
        // from the CSV stream.
        if (!fileReadErrorMsg.empty()) {
            csvStream->push(nullptr, 0);
        }
    });

    // Attempt the actual merge.
    bool const fileMergeSuccess = _infileMerger->merge(responseSummary, csvStream);
    if (!fileMergeSuccess) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " merge failed");
        util::Error const& err = _infileMerger->getError();
        _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg());
    }
    csvThread.join();
    if (!fileReadErrorMsg.empty()) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " result file read failed");
        _setError(ccontrol::MSG_HTTP_RESULT, fileReadErrorMsg);
    }
    _flushed = true;
    return fileMergeSuccess && fileReadErrorMsg.empty();
}

void MergingHandler::_setError(int code, std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_setErr: code: " << code << ", message: " << msg);
    std::lock_guard<std::mutex> lock(_errorMutex);
    _error = Error(code, msg);
}

}  // namespace lsst::qserv::ccontrol
