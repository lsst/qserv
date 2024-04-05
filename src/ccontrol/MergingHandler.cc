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
#include "qdisp/QueryRequest.h"
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

/**
 * The function for reading result files from workers over the HTTP protocol.
 * The function reads the file in chunks and calls the callback function
 * for each chunk of data read from the file.
 */
string readHttpFileAndMerge(string const& httpUrl, size_t fileSize,
                            function<void(char const*, uint32_t)> const& messageIsReady) {
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

        http::Client reader(http::Method::GET, httpUrl, noClientData, noClientHeaders,
                            qdisp::QueryRequest::makeHttpClientConfig(),
                            qdisp::QueryRequest::getHttpConnPool());

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
    return string();
}

std::tuple<bool, bool> readHttpFileAndMergeHttp(
        lsst::qserv::qdisp::UberJob::Ptr const& uberJob, string const& httpUrl,
        function<bool(char const*, uint32_t, bool&)> const& messageIsReady,
        shared_ptr<http::ClientConnPool> const& httpConnPool) {
    string const context = "MergingHandler::" + string(__func__) + " " + " qid=" + uberJob->getIdStr() + " ";

    LOGS(_log, LOG_LVL_DEBUG, context << "httpUrl=" << httpUrl);

    // Track the file while the control flow is staying within the function.
    ResultFileTracker const resultFileTracker;

    // The data transmit rate tracker is set up before reading each data message.
    unique_ptr<lsst::qserv::TimeCountTracker<double>> transmitRateTracker;

    // A location of the next byte to be read from the input file. The variable
    // is used for error reporting.
    uint64_t offset = 0;

    // Temporary buffer for messages read from the file. The buffer gets automatically
    // resized to fit the largest message.
    unique_ptr<char[]> msgBuf;
    size_t msgBufSize = 0;
    size_t msgBufNext = 0;  // An index of the next character in the buffer.

    // Fixed-size buffer to store the message size.
    string msgSizeBuf(sizeof(uint32_t), '\0');
    size_t msgSizeBufNext = 0;  // An index of the next character in the buffer.

    // The size of the next/current message. The variable is set after succesfully parsing
    // the message length header and is reset back to 0 after parsing the message body.
    // The value is stays 0 while reading the frame header.
    uint32_t msgSizeBytes = 0;
    bool success = true;
    bool mergeSuccess = true;
    int headerCount = 0;
    uint64_t totalBytesRead = 0;
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
        reader.read([&](char const* inBuf, size_t inBufSize) {
            // A value of the flag is set by the message processor when it's time to finish
            // or abort reading  the file.
            bool last = false;
            char const* next = inBuf;
            char const* const end = inBuf + inBufSize;
            while ((next < end) && !last) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "TODO:UJ next=" << (uint64_t)next << " end=" << (uint64_t)end
                             << " last=" << last);
                if (msgSizeBytes == 0) {
                    // Continue or finish reading the frame header.
                    size_t const bytes2read =
                            std::min(sizeof(uint32_t) - msgSizeBufNext, (size_t)(end - next));
                    std::memcpy(msgSizeBuf.data() + msgSizeBufNext, next, bytes2read);
                    next += bytes2read;
                    offset += bytes2read;
                    msgSizeBufNext += bytes2read;
                    if (msgSizeBufNext == sizeof(uint32_t)) {
                        ++headerCount;
                        // Done reading the frame header.
                        msgSizeBufNext = 0;
                        // Parse and evaluate the message length.
                        msgSizeBytes = *(reinterpret_cast<uint32_t*>(msgSizeBuf.data()));
                        if (msgSizeBytes == 0) {
                            throw runtime_error("message size is 0 at offset " +
                                                to_string(offset - sizeof(uint32_t)) + ", file: " + httpUrl);
                        }
                        if (msgSizeBytes > ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
                            throw runtime_error("message size " + to_string(msgSizeBytes) + " at offset " +
                                                to_string(offset - sizeof(uint32_t)) +
                                                " exceeds the hard limit of " +
                                                to_string(ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) +
                                                ", file: " + httpUrl);
                        }
                        // Extend the message buffer (if needed). Note that buffer never gets
                        // truncated to avoid excessive memory deallocations/allocations.
                        if (msgBufSize < msgSizeBytes) {
                            msgBufSize = msgSizeBytes;
                            msgBuf.reset(new char[msgBufSize]);
                        }
                        // Starts the tracker to measure the performance of the network I/O.
                        transmitRateTracker =
                                make_unique<lsst::qserv::TimeCountTracker<double>>(reportFileRecvRate);
                    }
                } else {
                    // Continue or finish reading the message body.
                    size_t const bytes2read =
                            std::min((size_t)msgSizeBytes - msgBufNext, (size_t)(end - next));
                    std::memcpy(msgBuf.get() + msgBufNext, next, bytes2read);
                    next += bytes2read;
                    offset += bytes2read;
                    msgBufNext += bytes2read;
                    if (msgBufNext == msgSizeBytes) {
                        // Done reading message body.
                        msgBufNext = 0;

                        // Destroying the tracker will result in stopping the tracker's timer and
                        // reporting the file read rate before proceeding to the merge.
                        if (transmitRateTracker != nullptr) {
                            transmitRateTracker->addToValue(msgSizeBytes);
                            transmitRateTracker->setSuccess();
                            transmitRateTracker.reset();
                        }

                        // Parse and evaluate the message.
                        mergeSuccess = messageIsReady(msgBuf.get(), msgSizeBytes, last);
                        totalBytesRead += msgSizeBytes;
                        if (!mergeSuccess) {
                            success = false;
                            throw runtime_error("message processing failed at offset " +
                                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
                        }
                        // Reset the variable to prepare for reading the next header & message (if any).
                        msgSizeBytes = 0;
                    } else {
                        LOGS(_log, LOG_LVL_WARN,
                             context << " headerCount=" << headerCount
                                     << " incomplete read diff=" << (msgSizeBytes - msgBufNext));
                    }
                }
            }
        });
        LOGS(_log, LOG_LVL_DEBUG,
             context << " headerCount=" << headerCount << " msgSizeBytes=" << msgSizeBytes
                     << " totalBytesRead=" << totalBytesRead);
        if (msgSizeBufNext != 0) {
            throw runtime_error("short read of the message header at offset " +
                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
        }
        if (msgBufNext != 0) {
            throw runtime_error("short read of the message body at offset " +
                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context + " " + ex.what());
        success = false;
    }

    // Remove the file from the worker if it still exists. Report and ignore errors.
    // The files will be garbage-collected by workers.
    try {
        http::Client remover(http::Method::DELETE, httpUrl);
        remover.read([](char const* inBuf, size_t inBufSize) {});
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, context << "failed to remove " << httpUrl << ", ex: " << ex.what());
    }
    // If the merge failed, that indicates something went wrong in the local database table,
    // is likely this user query is doomed and should be cancelled.
    LOGS(_log, LOG_LVL_DEBUG, context << " end succes=" << success << " mergeSuccess=" << mergeSuccess);
    return {success, mergeSuccess};
}

}  // namespace

namespace lsst::qserv::ccontrol {

MergingHandler::MergingHandler(std::shared_ptr<rproc::InfileMerger> merger, std::string const& tableName)
        : _infileMerger{merger}, _tableName{tableName} {
    _initState();
}

MergingHandler::~MergingHandler() { LOGS(_log, LOG_LVL_DEBUG, __func__ << " " << _tableName); }

bool MergingHandler::flush(proto::ResponseSummary const& resp) {
    _wName = resp.wname();

    // This is needed to ensure the job query would be staying alive for the duration
    // of the operation to prevent inconsistency within the application.
    auto const jobQuery = getJobQuery().lock();
    if (jobQuery == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " failed, jobQuery was NULL");
        return false;
    }
    auto const jobQuery = std::dynamic_pointer_cast<qdisp::JobQuery>(jobBase);

    LOGS(_log, LOG_LVL_TRACE,
         "MergingHandler::" << __func__ << " jobid=" << resp.jobid() << " transmitsize="
                            << resp.transmitsize() << " rowcount=" << resp.rowcount() << " rowSize="
                            << " attemptcount=" << resp.attemptcount() << " errorcode=" << resp.errorcode()
                            << " errormsg=" << resp.errormsg());

    if (resp.errorcode() != 0 || !resp.errormsg().empty()) {
        _error = util::Error(resp.errorcode(), resp.errormsg(), util::ErrorCode::MYSQLEXEC);
        _setError(ccontrol::MSG_RESULT_ERROR, _error.getMsg());
        LOGS(_log, LOG_LVL_ERROR,
             "MergingHandler::" << __func__ << " error from worker:" << resp.wname() << " error: " << _error);
        // This way we can track if the worker has reported this error. The current implementation
        // requires the large result size to be reported as an error via the InfileMerger regardless
        // of an origin of the error (Czar or the worker). Note that large results can be produced
        // by the Czar itself, e.g., when the aggregate result of multiple worker queries is too large
        // or by the worker when the result set of a single query is too large.
        // The error will be reported to the Czar as a part of the response summary.
        if (resp.errorcode() == util::ErrorCode::WORKER_RESULT_TOO_LARGE) {
            _infileMerger->setResultSizeLimitExceeded();
        }
        return false;
    }

    bool const success = _merge(resp, jobQuery);
    if (success) {
        _infileMerger->mergeCompleteFor(resp.jobid());
        qdisp::CzarStats::get()->addTotalRowsRecv(resp.rowcount());
        qdisp::CzarStats::get()->addTotalBytesRecv(resp.transmitsize());
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

bool MergingHandler::_merge(proto::ResponseSummary const& resp, shared_ptr<qdisp::JobQuery> const& jobQuery) {
    if (_flushed) throw util::Bug(ERR_LOC, "already flushed");
    if (resp.transmitsize() == 0) return true;

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
                resp.fileresource_http(), resp.transmitsize(), [&](char const* buf, uint32_t size) {
                    bool const queryEnded = _queryIsNoLongerActive(jobQuery);
                    bool last = false;
                    if (buf == nullptr || size == 0 || queryEnded) {
                        last = true;
                    } else {
                        csvStream->push(buf, size);
                        bytesRead += size;
                        last = bytesRead >= resp.transmitsize();
                    }
                    if (last) {
                        csvStream->push(nullptr, 0);
                        if (queryEnded) {
                            throw ::QueryEnded(
                                    "query " + jobQuery->getIdStr() +
                                    " ended while reading the file, bytesRead=" + to_string(bytesRead) +
                                    ", transmitsize=" + to_string(resp.transmitsize()));
                        }
                    }
                });
        // Push the stream terminator to indicate the end of the stream.
        // It may be neeeded to unblock the table merger which may be still attempting to read
        // from the CSV stream.
        if (!fileReadErrorMsg.empty()) {
            csvStream->push(nullptr, 0);
        }
    });

    // Attempt the actual merge.
    bool const fileMergeSuccess = _infileMerger->merge(resp, csvStream);
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

bool MergingHandler::_mergeHttp(shared_ptr<qdisp::UberJob> const& uberJob,
                                proto::ResponseData const& responseData) {
    if (_flushed) {
        throw util::Bug(ERR_LOC, "already flushed");
    }
    bool const success = _infileMerger->mergeHttp(uberJob, responseData);
    if (!success) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " failed");
        util::Error const& err = _infileMerger->getError();
        _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg());
    }
    return success;
}

void MergingHandler::_setError(int code, std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_setErr: code: " << code << ", message: " << msg);
    std::lock_guard<std::mutex> lock(_errorMutex);
    _error = Error(code, msg);
}

tuple<bool, bool> MergingHandler::flushHttp(string const& fileUrl, uint64_t expectedRows,
                                            uint64_t& resultRows) {
    bool success = false;
    bool shouldCancel = false;

    // This is needed to ensure the job query would be staying alive for the duration
    // of the operation to prevent inconsistency within the application.
    auto const jobBase = getJobBase().lock();
    if (jobBase == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, __func__ << " failed, jobBase was NULL");
        return {success, shouldCancel};  // both should still be false
    }
    auto const uberJob = std::dynamic_pointer_cast<qdisp::UberJob>(jobBase);

    LOGS(_log, LOG_LVL_TRACE,
         "MergingHandler::" << __func__ << " uberJob=" << uberJob->getIdStr() << " fileUrl=" << fileUrl);

    // Dispatch result processing to the corresponidng method which depends on
    // the result delivery protocol configured at the worker.
    // Notify the file reader when all rows have been read by setting 'last = true'.
    auto const dataMergerHttp = [&](char const* buf, uint32_t bufSize, bool& last) {
        LOGS(_log, LOG_LVL_TRACE, "dataMergerHttp");
        last = true;
        proto::ResponseData responseData;
        if (responseData.ParseFromArray(buf, bufSize) && responseData.IsInitialized()) {
            bool const mergeSuccess = _mergeHttp(uberJob, responseData);
            if (mergeSuccess) {
                resultRows += responseData.row_size();
                last = resultRows >= expectedRows;
            }
            return mergeSuccess;
        }
        throw runtime_error("MergingHandler::flush ** message deserialization failed **");
    };

    tie(success, shouldCancel) =
            ::readHttpFileAndMergeHttp(uberJob, fileUrl, dataMergerHttp, MergingHandler::_getHttpConnPool());

    if (!success || shouldCancel) {
        LOGS(_log, LOG_LVL_WARN, __func__ << " success=" << success << " shouldCancel=" << shouldCancel);
    }

    if (success) {
        _infileMerger->mergeCompleteFor(uberJob->getJobId());
    }
    return {success, shouldCancel};
}

void MergingHandler::flushHttpError(int errorCode, std::string const& errorMsg, int status) {
    if (!_errorSet.exchange(true)) {
        _error = util::Error(errorCode, errorMsg, util::ErrorCode::MYSQLEXEC);
        _setError(ccontrol::MSG_RESULT_ERROR, _error.getMsg());
    }
}

}  // namespace lsst::qserv::ccontrol
