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

// Third-party headers
#include "XrdCl/XrdClFile.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/msgCode.h"
#include "global/clock_defs.h"
#include "global/debugUtil.h"
#include "global/MsgReceiver.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/ProtoImporter.h"
#include "proto/WorkerResponse.h"
#include "qdisp/CzarStats.h"
#include "qdisp/JobQuery.h"
#include "replica/HttpClient.h"
#include "rproc/InfileMerger.h"
#include "util/Bug.h"
#include "util/common.h"
#include "util/StringHash.h"

using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::ProtoHeaderWrap;
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::Result;
using lsst::qserv::proto::WorkerResponse;
using lsst::qserv::replica::HttpClient;

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.MergingHandler");

string xrootdStatus2str(XrdCl::XRootDStatus const& s) {
    return "status=" + to_string(s.status) + ", code=" + to_string(s.code) + ", errNo=" + to_string(s.errNo) +
           ", message='" + s.GetErrorMessage() + "'";
}

/**
 * Extract the file path (including both slashes) from the XROOTD-style URL.
 * Input:
 *   @code
 *   "xroot://<server>:<port>//<path>""
 *   @code
 * Output:
 *   @code
 *   "//<path>""
 *   @code
 */
string xrootUrl2path(string const& xrootUrl) {
    string const delim = "//";
    auto firstPos = xrootUrl.find(delim, 0);
    if (string::npos != firstPos) {
        // Resume serching at the first character following the delimiter.
        auto secondPos = xrootUrl.find(delim, firstPos + 2);
        if (string::npos != secondPos) {
            return xrootUrl.substr(secondPos);
        }
    }
    throw runtime_error("MergingHandler::" + string(__func__) + " illegal file resource url: " + xrootUrl);
}

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

bool readXrootFileResourceAndMerge(lsst::qserv::proto::Result const& result,
                                   function<bool(char const*, uint32_t)> const& messageIsReady) {
    string const context = "MergingHandler::" + string(__func__) + " ";

    // Extract data from the input result object before modifying the one.
    string const xrootUrl = result.fileresource_xroot();

    // Track the file while the control flow is staying within the function.
    ResultFileTracker const resultFileTracker;

    // The algorithm will read the input file to locate result objects containing rows
    // and call the provided callback for each such row.
    XrdCl::File file;
    XrdCl::XRootDStatus status;
    status = file.Open(xrootUrl, XrdCl::OpenFlags::Read);
    if (!status.IsOK()) {
        LOGS(_log, LOG_LVL_ERROR,
             context << "failed to open " << xrootUrl << ", " << xrootdStatus2str(status));
        return false;
    }

    // Temporary buffer for messages read from the file. The buffer will be (re-)allocated
    // as needed to get the largest message. Note that a size of the messages won't exceed
    // a limit set in ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT.
    unique_ptr<char[]> buf;
    size_t bufSize = 0;

    uint64_t offset = 0;  // A location of the next byte to be read from the input file.
    bool success = true;
    try {
        while (true) {
            // This starts a timer of the data transmit rate tracker.
            auto transmitRateTracker = make_unique<lsst::qserv::TimeCountTracker<double>>(reportFileRecvRate);

            // Read the frame header that carries a size of the subsequent message.
            uint32_t msgSizeBytes = 0;
            uint32_t bytesRead = 0;
            status = file.Read(offset, sizeof(uint32_t), reinterpret_cast<char*>(&msgSizeBytes), bytesRead);
            if (!status.IsOK()) {
                throw runtime_error(context + "failed to read next frame header (" +
                                    to_string(sizeof(uint32_t)) + " bytes) at offset " + to_string(offset) +
                                    " from " + xrootUrl + ", " + xrootdStatus2str(status));
            }
            offset += bytesRead;

            if (bytesRead == 0) break;
            if (bytesRead != sizeof(uint32_t)) {
                throw runtime_error(context + "read " + to_string(bytesRead) + " bytes instead of " +
                                    to_string(sizeof(uint32_t)) +
                                    " bytes when reading next frame header at offset " +
                                    to_string(offset - bytesRead) + " from " + xrootUrl + ", " +
                                    xrootdStatus2str(status));
            }
            if (msgSizeBytes == 0) break;
            if (msgSizeBytes > ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
                throw runtime_error(context + "message size of " + to_string(msgSizeBytes) +
                                    " bytes at the frame header read at offset " +
                                    to_string(offset - bytesRead) + " exceeds the hard limit set to " +
                                    to_string(ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) + " bytes, from " +
                                    xrootUrl + ", " + xrootdStatus2str(status));
            }

            // (Re-)allocate the buffer if needed.
            if (bufSize < msgSizeBytes) {
                bufSize = msgSizeBytes;
                buf.reset(new char[bufSize]);
            }

            // Read the message.
            size_t bytes2read = msgSizeBytes;
            while (bytes2read != 0) {
                uint32_t bytesRead = 0;
                status = file.Read(offset, bytes2read, buf.get(), bytesRead);
                if (!status.IsOK()) {
                    throw runtime_error(context + "failed to read " + to_string(bytes2read) +
                                        " bytes at offset " + to_string(offset) + " from " + xrootUrl + ", " +
                                        xrootdStatus2str(status));
                }
                if (bytesRead == 0) {
                    throw runtime_error(context + "read 0 bytes instead of " + to_string(bytes2read) +
                                        " bytes at offset " + to_string(offset) + " from " + xrootUrl + ", " +
                                        xrootdStatus2str(status));
                }
                offset += bytesRead;
                bytes2read -= bytesRead;
            }

            // Destroying the tracker will result in stopping the tracker's timer and
            // reporting the file read rate before proceeding to the merge.
            transmitRateTracker->addToValue(msgSizeBytes);
            transmitRateTracker->setSuccess();
            transmitRateTracker.reset();

            // Proceed to the result merge
            success = messageIsReady(buf.get(), msgSizeBytes);
            if (!success) break;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
        success = false;
    }
    status = file.Close();
    if (!status.IsOK()) {
        LOGS(_log, LOG_LVL_WARN,
             context << "failed to close " << xrootUrl << ", " << xrootdStatus2str(status));
    }

    // Remove the file from the worker if it still exists. Report and ignore errors.
    // The files will be garbage-collected by workers.
    XrdCl::FileSystem fileSystem(xrootUrl);
    status = fileSystem.Rm(xrootUrl2path(xrootUrl));
    if (!status.IsOK()) {
        LOGS(_log, LOG_LVL_WARN,
             context << "failed to remove " << xrootUrl << ", " << xrootdStatus2str(status));
    }
    return success;
}

bool readHttpFileAndMerge(lsst::qserv::proto::Result const& result,
                          function<bool(char const*, uint32_t)> const& messageIsReady) {
    string const context = "MergingHandler::" + string(__func__) + " ";

    // Extract data from the input result object before modifying the one.
    string const httpUrl = result.fileresource_http();

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
        HttpClient reader("GET", httpUrl);
        reader.read([&](char const* inBuf, size_t inBufSize) {
            char const* next = inBuf;
            char const* const end = inBuf + inBufSize;
            while (next < end) {
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
                        bool const success = messageIsReady(msgBuf.get(), msgSizeBytes);
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
        HttpClient remover("DELETE", httpUrl);
        remover.read([](char const* inBuf, size_t inBufSize) {});
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, context << "failed to remove " << httpUrl << ", ex: " << ex.what());
    }
    return success;
}

}  // namespace

namespace lsst::qserv::ccontrol {

////////////////////////////////////////////////////////////////////////
// MergingHandler public
////////////////////////////////////////////////////////////////////////
MergingHandler::MergingHandler(std::shared_ptr<MsgReceiver> msgReceiver,
                               std::shared_ptr<rproc::InfileMerger> merger, std::string const& tableName)
        : _msgReceiver{msgReceiver},
          _infileMerger{merger},
          _tableName{tableName},
          _response{new WorkerResponse()} {
    _initState();
}

MergingHandler::~MergingHandler() { LOGS(_log, LOG_LVL_DEBUG, "~MergingHandler()"); }

const char* MergingHandler::getStateStr(MsgState const& state) {
    switch (state) {
        case MsgState::HEADER_WAIT:
            return "HEADER_WAIT";
        case MsgState::RESULT_WAIT:
            return "RESULT_WAIT";
        case MsgState::RESULT_RECV:
            return "RESULT_RECV";
        case MsgState::HEADER_ERR:
            return "HEADER_ERR";
        case MsgState::RESULT_ERR:
            return "RESULT_ERR";
    }
    return "unknown";
}

bool MergingHandler::flush(int bLen, BufPtr const& bufPtr, bool& last, int& nextBufSize, int& resultRows) {
    LOGS(_log, LOG_LVL_DEBUG,
         "From:" << _wName << " flush state=" << getStateStr(_state) << " blen=" << bLen << " last=" << last);
    resultRows = 0;

    if (bLen < 0) {
        throw util::Bug(ERR_LOC, "MergingHandler invalid blen=" + to_string(bLen) + " from " + _wName);
    }

    switch (_state) {
        case MsgState::HEADER_WAIT:
            _response->headerSize = static_cast<unsigned char>((*bufPtr)[0]);
            if (!proto::ProtoHeaderWrap::unwrap(_response, *bufPtr)) {
                std::string sErr =
                        "From:" + _wName + "Error decoding proto header for " + getStateStr(_state);
                _setError(ccontrol::MSG_RESULT_DECODE, sErr);
                _state = MsgState::HEADER_ERR;
                return false;
            }
            if (_wName == "~") {
                _wName = _response->protoHeader.wname();
            }

            {
                nextBufSize = _response->protoHeader.size();
                bool endNoData = _response->protoHeader.endnodata();
                int seq = -1;
                int scsSeq = -1;
                if (_response->protoHeader.has_seq()) {
                    seq = _response->protoHeader.seq();
                }
                if (_response->protoHeader.has_scsseq()) {
                    scsSeq = _response->protoHeader.scsseq();
                }
                LOGS(_log, LOG_LVL_DEBUG,
                     "HEADER_WAIT: From:" << _wName << " nextBufSize=" << nextBufSize << " endNoData="
                                          << endNoData << " seq=" << seq << " scsseq=" << scsSeq);

                _state = MsgState::RESULT_WAIT;
                if (endNoData || nextBufSize == 0) {
                    if (!endNoData || nextBufSize != 0) {
                        throw util::Bug(ERR_LOC, "inconsistent msg termination endNoData=" +
                                                         std::to_string(endNoData) +
                                                         " nextBufSize=" + std::to_string(nextBufSize));
                    }
                    // Nothing to merge, but some bookkeeping needs to be done.
                    _infileMerger->mergeCompleteFor(_jobIds);
                    last = true;
                    _state = MsgState::RESULT_RECV;
                }
            }
            return true;
        case MsgState::RESULT_WAIT: {
            nextBufSize = proto::ProtoHeaderWrap::getProtoHeaderSize();
            auto jobQuery = getJobQuery().lock();
            if (!_verifyResult(bufPtr, bLen)) {
                return false;
            }
            if (!_setResult(bufPtr, bLen)) {  // This sets _response->result
                LOGS(_log, LOG_LVL_WARN, "setResult failure " << _wName);
                return false;
            }
            LOGS(_log, LOG_LVL_DEBUG, "From:" << _wName << " _mBuf " << util::prettyCharList(*bufPtr, 5));
            _state = MsgState::HEADER_WAIT;

            int jobId = _response->result.jobid();
            _jobIds.insert(jobId);
            LOGS(_log, LOG_LVL_DEBUG, "Flushed last=" << last << " for tableName=" << _tableName);

            // Dispatch result processing to the corresponidng method which depends on
            // the result delivery protocol configured at the worker.
            auto const mergeCurrentResult = [this, &resultRows]() {
                resultRows += _response->result.row_size();
                bool const success = _merge();
                // A fresh instance may be needed to process the next message of the results stream.
                // Note that _merge() resets the object.
                _response.reset(new WorkerResponse());
                return success;
            };
            bool success = false;
            if (!_response->result.fileresource_xroot().empty()) {
                success = _noErrorsInResult() &&
                          ::readXrootFileResourceAndMerge(
                                  _response->result, [&](char const* buf, uint32_t messageLength) -> bool {
                                      if (_response->result.ParseFromArray(buf, messageLength) &&
                                          _response->result.IsInitialized()) {
                                          return mergeCurrentResult();
                                      }
                                      throw runtime_error(
                                              "MergingHandler::flush ** message deserialization failed **");
                                  });
            } else if (!_response->result.fileresource_http().empty()) {
                success = _noErrorsInResult() &&
                          ::readHttpFileAndMerge(
                                  _response->result, [&](char const* buf, uint32_t messageLength) -> bool {
                                      if (_response->result.ParseFromArray(buf, messageLength) &&
                                          _response->result.IsInitialized()) {
                                          return mergeCurrentResult();
                                      }
                                      throw runtime_error(
                                              "MergingHandler::flush ** message deserialization failed **");
                                  });
            } else {
                success = mergeCurrentResult();
            }
            return success;
        }
        case MsgState::RESULT_RECV:
            // We shouldn't wind up here. _buffer.size(0) and last=true should end communication.
            [[fallthrough]];
        case MsgState::HEADER_ERR:
            [[fallthrough]];
        case MsgState::RESULT_ERR: {
            std::ostringstream eos;
            eos << "Unexpected message From:" << _wName << " flush state=" << getStateStr(_state)
                << " last=" << last;
            LOGS(_log, LOG_LVL_ERROR, eos.str());
            _setError(ccontrol::MSG_RESULT_ERROR, eos.str());
        }
            return false;
        default:
            break;
    }
    _setError(ccontrol::MSG_RESULT_ERROR, "Unexpected message (invalid)");
    return false;
}

bool MergingHandler::_noErrorsInResult() {
    if (_response->result.has_errorcode() || _response->result.has_errormsg()) {
        _setError(_response->result.errorcode(), _response->result.errormsg());
        LOGS(_log, LOG_LVL_ERROR,
             "Error from worker:" << _response->protoHeader.wname() << " in response data: " << _error);
        return false;
    }
    return true;
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

// Note that generally we always have an _infileMerger object except during
// a unit test. I suppose we could try to figure out how to create one.
//
void MergingHandler::prepScrubResults(int jobId, int attemptCount) {
    if (_infileMerger) _infileMerger->prepScrub(jobId, attemptCount);
}

std::ostream& MergingHandler::print(std::ostream& os) const {
    return os << "MergingRequester(" << _tableName << ", flushed=" << (_flushed ? "true)" : "false)");
}
////////////////////////////////////////////////////////////////////////
// MergingRequester private
////////////////////////////////////////////////////////////////////////

void MergingHandler::_initState() {
    _state = MsgState::HEADER_WAIT;
    _setError(0, "");
}

bool MergingHandler::_merge() {
    if (auto job = getJobQuery().lock()) {
        if (_flushed) {
            throw util::Bug(ERR_LOC, "MergingRequester::_merge : already flushed");
        }
        bool success = _infileMerger->merge(_response);
        if (!success) {
            LOGS(_log, LOG_LVL_WARN, "_merge() failed");
            rproc::InfileMergerError const& err = _infileMerger->getError();
            _setError(ccontrol::MSG_RESULT_ERROR, err.getMsg());
            _state = MsgState::RESULT_ERR;
        }
        _response.reset();

        return success;
    }
    LOGS(_log, LOG_LVL_ERROR, "MergingHandler::_merge() failed, jobQuery was NULL");
    return false;
}

void MergingHandler::_setError(int code, std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_setErr: code: " << code << ", message: " << msg);
    std::lock_guard<std::mutex> lock(_errorMutex);
    _error = Error(code, msg);
}

bool MergingHandler::_setResult(BufPtr const& bufPtr, int blen) {
    auto start = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> lg(_setResultMtx);
    auto& buf = *bufPtr;
    if (!ProtoImporter<proto::Result>::setMsgFrom(_response->result, &(buf[0]), blen)) {
        LOGS(_log, LOG_LVL_ERROR, "_setResult decoding error");
        _setError(ccontrol::MSG_RESULT_DECODE, "Error decoding result msg");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    auto protoEnd = std::chrono::system_clock::now();
    auto protoDur = std::chrono::duration_cast<std::chrono::milliseconds>(protoEnd - start);
    LOGS(_log, LOG_LVL_DEBUG, "protoDur=" << protoDur.count());
    return true;
}

bool MergingHandler::_verifyResult(BufPtr const& bufPtr, int blen) {
    auto& buf = *bufPtr;
    if (_response->protoHeader.md5() != util::StringHash::getMd5(&(buf[0]), blen)) {
        LOGS(_log, LOG_LVL_ERROR, "_verifyResult MD5 mismatch");
        _setError(ccontrol::MSG_RESULT_MD5, "Result message MD5 mismatch");
        _state = MsgState::RESULT_ERR;
        return false;
    }
    return true;
}

}  // namespace lsst::qserv::ccontrol
