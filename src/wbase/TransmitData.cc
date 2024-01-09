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
#include "wbase/TransmitData.h"

// System headers

// Third-party headers
#include <google/protobuf/arena.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/debugUtil.h"
#include "global/intTypes.h"
#include "global/LogContext.h"
#include "proto/ProtoHeaderWrap.h"
#include "util/Bug.h"
#include "util/InstanceCount.h"
#include "util/MultiError.h"
#include "util/StringHash.h"
#include "wbase/Task.h"
#include "xrdsvc/StreamBuffer.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.TransmitData");
}

namespace lsst::qserv::wbase {

atomic<int> seqSource{0};

TransmitData::Ptr TransmitData::createTransmitData(qmeta::CzarId const& czarId_, string const& idStr) {
    return shared_ptr<TransmitData>(new TransmitData(czarId_, make_shared<google::protobuf::Arena>(), idStr));
}

TransmitData::TransmitData(qmeta::CzarId const& czarId_, shared_ptr<google::protobuf::Arena> const& arena,
                           string const& idStr)
        : _czarId(czarId_), _arena(arena), _idStr(idStr), _trSeq(seqSource++) {
    lock_guard<mutex> const lock(_trMtx);
    _header = _createHeader(lock);
    _result = _createResult(lock);
}

proto::ProtoHeader* TransmitData::_createHeader(lock_guard<mutex> const& lock) {
    proto::ProtoHeader* hdr = google::protobuf::Arena::CreateMessage<proto::ProtoHeader>(_arena.get());
    hdr->set_size(0);
    hdr->set_wname(getHostname());
    hdr->set_endnodata(true);
    return hdr;
}

proto::Result* TransmitData::_createResult(lock_guard<mutex> const& lock) {
    proto::Result* rst = google::protobuf::Arena::CreateMessage<proto::Result>(_arena.get());
    return rst;
}

void TransmitData::attachNextHeader(TransmitData::Ptr const& nextTr, bool reallyLast) {
    _icPtr = make_shared<util::InstanceCount>(_idStr + "_td_LDB_" + to_string(reallyLast));
    lock_guard<mutex> const lock(_trMtx);
    if (_result == nullptr) {
        throw util::Bug(ERR_LOC, _idStr + "_transmitLoop() had nullptr result!");
    }

    string nextHeaderString;
    if (reallyLast) {
        // Need a special header to indicate there are no more messages.
        LOGS(_log, LOG_LVL_TRACE, _dump(lock) << " attachNextHeader reallyLast=" << reallyLast);
        // this _tMtx is already locked, so call private member
        nextHeaderString = _makeHeaderString(lock, reallyLast);
    } else {
        // Need the header from the next TransmitData object in the queue.
        // Using public version to lock its mutex.
        LOGS(_log, LOG_LVL_TRACE,
             _dump(lock) << "attachNextHeader reallyLast=" << reallyLast << " next=" << nextTr->dump());
        // next _tMtx is not locked, so call public member
        nextHeaderString = nextTr->makeHeaderString(reallyLast);
    }
    // Append the next header to this data.
    _dataMsg += proto::ProtoHeaderWrap::wrap(nextHeaderString);
}

string TransmitData::makeHeaderString(bool reallyLast) {
    lock_guard<mutex> const lock(_trMtx);
    return _makeHeaderString(lock, reallyLast);
}

string TransmitData::_makeHeaderString(lock_guard<mutex> const& lock, bool reallyLast) {
    proto::ProtoHeader* pHeader;
    if (reallyLast) {
        // Create a header for an empty dataMsg using the protobuf arena from thisTransmit.
        // This is the signal to the czar that this FileChannelShared is finished.
        pHeader = _createHeader(lock);
    } else {
        pHeader = _header;
    }
    pHeader->set_endnodata(reallyLast);
    string headerString;
    pHeader->SerializeToString(&headerString);
    return headerString;
}

string TransmitData::getHeaderString() {
    lock_guard<mutex> const lock(_trMtx);
    proto::ProtoHeader* thisPHdr = _header;
    string thisHeaderString;
    thisPHdr->SerializeToString(&thisHeaderString);
    return thisHeaderString;
}

xrdsvc::StreamBuffer::Ptr TransmitData::getStreamBuffer(Task::Ptr const& task) {
    lock_guard<mutex> const lock(_trMtx);
    // createWithMove invalidates _dataMsg
    return xrdsvc::StreamBuffer::createWithMove(_dataMsg, task);
}

void TransmitData::_buildHeader(lock_guard<mutex> const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, _idStr << "TransmitData::_buildHeader");
    // The size of the dataMsg must include space for the header for the next dataMsg.
    _header->set_size(_dataMsg.size() + proto::ProtoHeaderWrap::getProtoHeaderSize());
    _header->set_endnodata(false);
}

void TransmitData::buildDataMsg(Task const& task, util::MultiError& multiErr) {
    lock_guard<mutex> const lock(_trMtx);
    _buildDataMsg(lock, task, multiErr);
}

void TransmitData::_buildDataMsg(lock_guard<mutex> const& lock, Task const& task,
                                 util::MultiError& multiErr) {
    QSERV_LOGCONTEXT_QUERY_JOB(task.getQueryId(), task.getJobId());
    LOGS(_log, LOG_LVL_INFO,
         _idStr << "TransmitData::_buildDataMsg rowCount=" << _rowCount << " tSize=" << _tSize);
    assert(_result != nullptr);

    _result->set_rowcount(_rowCount);
    _result->set_transmitsize(_tSize);
    _result->set_attemptcount(task.getAttemptCount());

    if (!multiErr.empty()) {
        string msg = "Error(s) in result for chunk #" + to_string(task.getChunkId()) + ": " +
                     multiErr.toOneLineString();
        _result->set_errormsg(msg);
        _result->set_errorcode(multiErr.firstErrorCode());
        LOGS(_log, LOG_LVL_ERROR, _idStr << "buildDataMsg adding " << msg);
    }
    _result->SerializeToString(&_dataMsg);
    // Build the header for this message, but this message can't be transmitted until the
    // next header has been built and appended to _transmitData->dataMsg. That happens
    // later in FileChannelShared.
    _buildHeader(lock);
}

void TransmitData::initResult(Task& task) {
    lock_guard<mutex> const lock(_trMtx);
    _result->set_queryid(task.getQueryId());
    _result->set_jobid(task.getJobId());
    _result->set_fileresource_xroot(task.resultFileXrootUrl());
    _result->set_fileresource_http(task.resultFileHttpUrl());
}

bool TransmitData::hasErrormsg() const { return _result->has_errormsg(); }

bool TransmitData::fillRows(MYSQL_RES* mResult) {
    lock_guard<mutex> const lock(_trMtx);
    MYSQL_ROW row;

    int const numFields = mysql_num_fields(mResult);
    unsigned int szLimit = min(proto::ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT,
                               proto::ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT);
    while ((row = mysql_fetch_row(mResult))) {
        auto lengths = mysql_fetch_lengths(mResult);
        proto::RowBundle* rawRow = _result->add_row();
        for (int i = 0; i < numFields; ++i) {
            if (row[i]) {
                rawRow->add_column(row[i], lengths[i]);
                rawRow->add_isnull(false);
            } else {
                rawRow->add_column();
                rawRow->add_isnull(true);
            }
        }
        _tSize += rawRow->ByteSizeLong();
        ++_rowCount;

        // Each element needs to be mysql-sanitized
        // Break the loop if the result is too big so this part can be transmitted.
        if (_tSize > szLimit) {
            return false;
        }
    }
    return true;
}

void TransmitData::prepareResponse(Task const& task, uint32_t rowcount, uint64_t transmitsize) {
    lock_guard<mutex> const lock(_trMtx);
    _rowCount = rowcount;
    _tSize = transmitsize;
    _result->clear_row();
    // Rebuild the message
    util::MultiError multiErr;
    _buildDataMsg(lock, task, multiErr);
}

size_t TransmitData::getResultTransmitSize() const {
    lock_guard<mutex> const lock(_trMtx);
    return _tSize;
}

int TransmitData::getResultSize() const {
    lock_guard<mutex> const lock(_trMtx);
    return _dataMsg.size();
}

int TransmitData::getResultRowCount() const {
    lock_guard<mutex> const lock(_trMtx);
    return _rowCount;
}

string TransmitData::dump() const {
    lock_guard<mutex> const lock(_trMtx);
    return _dump(lock);
}

string TransmitData::dataMsg() const {
    lock_guard<mutex> const lock(_trMtx);
    return _dataMsg;
}

string TransmitData::_dump(lock_guard<mutex> const& lock) const {
    string str = string(" trDump ") + _idStr + " trSeq=" + to_string(_trSeq) + " hdr=";
    if (_header != nullptr) {
        str += to_string(_header->size());
    } else {
        str += "nullptr";
    }
    str += " res=" + to_string(_dataMsg.size());
    return str;
}

}  // namespace lsst::qserv::wbase
