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

bool readHttpFileAndMerge(string const& httpUrl,
                          function<bool(char const*, uint32_t, bool&)> const& messageIsReady,
                          shared_ptr<http::ClientConnPool> const& httpConnPool) {
    string const context = "MergingHandler::" + string(__func__) + " ";

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
                if (msgSizeBytes == 0) {
                    // Continue or finish reading the frame header.
                    size_t const bytes2read =
                            std::min(sizeof(uint32_t) - msgSizeBufNext, (size_t)(end - next));
                    std::memcpy(msgSizeBuf.data() + msgSizeBufNext, next, bytes2read);
                    next += bytes2read;
                    offset += bytes2read;
                    msgSizeBufNext += bytes2read;
                    if (msgSizeBufNext == sizeof(uint32_t)) {
                        // Done reading the frame header.
                        msgSizeBufNext = 0;
                        // Parse and evaluate the message length.
                        msgSizeBytes = *(reinterpret_cast<uint32_t*>(msgSizeBuf.data()));
                        if (msgSizeBytes == 0) {
                            throw runtime_error(context + "message size is 0 at offset " +
                                                to_string(offset - sizeof(uint32_t)) + ", file: " + httpUrl);
                        }
                        if (msgSizeBytes > ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
                            throw runtime_error(context + "message size " + to_string(msgSizeBytes) +
                                                " at offset " + to_string(offset - sizeof(uint32_t)) +
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
                        bool const success = messageIsReady(msgBuf.get(), msgSizeBytes, last);
                        if (!success) {
                            throw runtime_error(context + "message processing failed at offset " +
                                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
                        }
                        // Reset the variable to prepare for reading the next header & message (if any).
                        msgSizeBytes = 0;
                    }
                }
            }
        });
        if (msgSizeBufNext != 0) {
            throw runtime_error(context + "short read of the message header at offset " +
                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
        }
        if (msgBufNext != 0) {
            throw runtime_error(context + "short read of the message body at offset " +
                                to_string(offset - msgSizeBytes) + ", file: " + httpUrl);
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
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
    return success;
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

bool MergingHandler::flush(proto::ResponseSummary const& responseSummary, uint32_t& resultRows) {
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

    // Dispatch result processing to the corresponidng method which depends on
    // the result delivery protocol configured at the worker.
    // Notify the file reader when all rows have been read by setting 'last = true'.
    auto const dataMerger = [&](char const* buf, uint32_t size, bool& last) {
        last = true;
        proto::ResponseData responseData;
        if (responseData.ParseFromArray(buf, size) && responseData.IsInitialized()) {
            bool const success = _merge(responseSummary, responseData, jobQuery);
            if (success) {
                resultRows += responseData.row_size();
                last = resultRows >= responseSummary.rowcount();
            }
            return success;
        }
        throw runtime_error("MergingHandler::flush ** message deserialization failed **");
    };

    bool const success = ::readHttpFileAndMerge(responseSummary.fileresource_http(), dataMerger,
                                                MergingHandler::_getHttpConnPool());
    if (success) {
        _infileMerger->mergeCompleteFor(responseSummary.jobid());
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

void MergingHandler::_initState() { _setError(0, ""); }

bool MergingHandler::_merge(proto::ResponseSummary const& responseSummary,
                            proto::ResponseData const& responseData,
                            shared_ptr<qdisp::JobQuery> const& jobQuery) {
    if (_flushed) throw util::Bug(ERR_LOC, "already flushed");

    // Nothing to do if size is zero.
    if (responseData.row_size() == 0) return true;

    // Do nothing if the query got cancelled for any reason.
    if (jobQuery->isQueryCancelled()) return true;

    // Check for other indicators that the query may have cancelled or finished.
    auto executive = jobQuery->getExecutive();
    if (executive == nullptr || executive->getCancelled() || executive->isLimitRowComplete()) {
        return true;
    }

    // Attempt the actual merge.
    if (_infileMerger->merge(responseSummary, responseData)) return true;

    LOGS(_log, LOG_LVL_WARN, __func__ << " failed");
    util::Error const& err = _infileMerger->getError();
    _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg());
    return false;
}

void MergingHandler::_setError(int code, std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_setErr: code: " << code << ", message: " << msg);
    std::lock_guard<std::mutex> lock(_errorMutex);
    _error = Error(code, msg);
}

}  // namespace lsst::qserv::ccontrol
