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
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "global/intTypes.h"
#include "global/LogContext.h"
#include "proto/ProtoHeaderWrap.h"
#include "util/MultiError.h"
#include "util/StringHash.h"
#include "wbase/Task.h"
#include "xrdsvc/StreamBuffer.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.TransmitData");
}

namespace lsst {
namespace qserv {
namespace wbase {

std::atomic<int> seqSource{0};

TransmitData::TransmitData(qmeta::CzarId const& czarId_, shared_ptr<google::protobuf::Arena> const& arena,
                           std::string const& idStr)
    : _czarId(czarId_), _arena(arena), _idStr(idStr), _trSeq(seqSource++) {
    _header = _createHeader();
    _result = _createResult();
}


TransmitData::Ptr TransmitData::createTransmitData(qmeta::CzarId const& czarId_, string const& idStr) {
    shared_ptr<google::protobuf::Arena> arena = make_shared<google::protobuf::Arena>();
    auto ptr = shared_ptr<TransmitData>(new TransmitData(czarId_, arena, idStr));
    LOGS(_log, LOG_LVL_TRACE, idStr << "&&&TransmitData::createTransmitData " << ptr->dump() );
    return ptr;
}


/// Note: _trMtx must be held before calling this.
proto::ProtoHeader* TransmitData::_createHeader() {
    proto::ProtoHeader* hdr = google::protobuf::Arena::CreateMessage<proto::ProtoHeader>(_arena.get());
    hdr->set_protocol(2); // protocol 2: row-by-row message
    hdr->set_size(0);
    hdr->set_md5(util::StringHash::getMd5("", 0));
    hdr->set_wname(getHostname());
    hdr->set_largeresult(false);
    hdr->set_endnodata(true);
    return hdr;
}


proto::Result* TransmitData::_createResult() {
    proto::Result* rst = google::protobuf::Arena::CreateMessage<proto::Result>(_arena.get());
    return rst;
}


void TransmitData::attachNextHeader(TransmitData::Ptr const& nextTr, bool reallyLast,
                                    uint32_t seq, int scsSeq) {
    lock_guard<mutex> lock(_trMtx);
    if (_result == nullptr) {
        throw Bug(_idStr + "_transmitLoop() had nullptr result!");
    }

    string nextHeaderString;
    if (reallyLast) {
        // Need a special header to indicate there are no more messages.
        LOGS(_log, LOG_LVL_TRACE, _dump() << "&&& attachNextHeader a reallyLast=" << reallyLast);
        // this _tMtx is already locked, so call private member
        nextHeaderString = _makeHeaderString(reallyLast, seq, scsSeq);
    } else {
        // Need the header from the next TransmitData object in the queue.
        // Using public version to lock its mutex.
        LOGS(_log, LOG_LVL_TRACE, _dump() << "&&& attachNextHeader b reallyLast=" << reallyLast << " next=" << nextTr->dump());
        // next _tMtx is not locked, so call public member
        nextHeaderString = nextTr->makeHeaderString(reallyLast, seq, scsSeq);
    }
    // Append the next header to this data.
    _dataMsg += proto::ProtoHeaderWrap::wrap(nextHeaderString);
}


string TransmitData::makeHeaderString(bool reallyLast, uint32_t seq, int scsSeq) {
    lock_guard<mutex> lock(_trMtx);
    return _makeHeaderString(reallyLast, seq, scsSeq);
}


string TransmitData::_makeHeaderString(bool reallyLast, uint32_t seq, int scsSeq) {
    // Note: _trMtx must be held before calling this.
    proto::ProtoHeader* pHeader;
    if (reallyLast) {
        // Create a header for an empty dataMsg using the protobuf arena from thisTransmit.
        // This is the signal to the czar that this SharedSendChannel is finished.
        pHeader = _createHeader();
    } else {
        pHeader = _header;
    }
    pHeader->set_endnodata(reallyLast);
    LOGS(_log, LOG_LVL_TRACE, _idStr << "&&& _makeHeaderString reallyLast=" << reallyLast << " h.size=" << pHeader->size() << " h.endnodata=" << pHeader->endnodata());
    pHeader->set_seq(seq);
    pHeader->set_scsseq(scsSeq);
    string headerString;
    pHeader->SerializeToString(&headerString);
    return headerString;
}


string TransmitData::getHeaderString(uint32_t seq, int scsSeq) {
    lock_guard<mutex> lock(_trMtx);
    proto::ProtoHeader* thisPHdr = _header;
    thisPHdr->set_seq(seq);
    thisPHdr->set_scsseq(scsSeq); // should always be 0
    string thisHeaderString;
    thisPHdr->SerializeToString(&thisHeaderString);
    return thisHeaderString;
}


xrdsvc::StreamBuffer::Ptr TransmitData::getStreamBuffer() {
    lock_guard<mutex> lock(_trMtx);
    // createWithMove invalidates _dataMsg
    return xrdsvc::StreamBuffer::createWithMove(_dataMsg);
}


void TransmitData::_buildHeader(bool largeResult) {
    LOGS(_log, LOG_LVL_DEBUG, _idStr << "TransmitData::_buildHeader");

    // The size of the dataMsg must include space for the header for the next dataMsg.
    _header->set_size(_dataMsg.size() + proto::ProtoHeaderWrap::getProtoHeaderSize());
    LOGS(_log, LOG_LVL_TRACE, _idStr << "&&&TransmitData::_buildHeader size=" << _dataMsg.size() + proto::ProtoHeaderWrap::getProtoHeaderSize());
    // The md5 hash must not include the header for the next dataMsg.
    _header->set_md5(util::StringHash::getMd5(_dataMsg.data(), _dataMsg.size()));
    _header->set_largeresult(largeResult);
    _header->set_endnodata(false);
}


void TransmitData::buildDataMsg(Task const& task, bool largeResult, util::MultiError& multiErr) {
    lock_guard<mutex> lock(_trMtx);
    _buildDataMsg(task, largeResult, multiErr);
}


void TransmitData::_buildDataMsg(Task const& task, bool largeResult, util::MultiError& multiErr) {
    QSERV_LOGCONTEXT_QUERY_JOB(task.getQueryId(), task.getJobId());
    LOGS(_log, LOG_LVL_INFO, _idStr << "TransmitData::_buildDataMsg rowCount=" << _rowCount << " tSize=" << _tSize);
    assert(_result != nullptr);

    _result->set_rowcount(_rowCount);
    _result->set_transmitsize(_tSize);
    _result->set_attemptcount(task.getAttemptCount());

    if (!multiErr.empty()) {
        string chunkId = to_string(task.msg->chunkid());
        string msg = "Error(s) in result for chunk #" + chunkId + ": " + multiErr.toOneLineString();
        _result->set_errormsg(msg);
        LOGS(_log, LOG_LVL_ERROR, _idStr << "buildDataMsg adding " << msg);
    }
    _result->SerializeToString(&_dataMsg);
    LOGS(_log, LOG_LVL_TRACE, _idStr << "&&& TransmitData::_buildDataMsg dataMsg.sz=" << _dataMsg.size());
    // Build the header for this message, but this message can't be transmitted until the
    // next header has been built and appended to _transmitData->dataMsg. That happens
    // later in SendChannelShared.
    _buildHeader(largeResult);
}


void TransmitData::initResult(Task& task, std::vector<SchemaCol>& schemaCols) {
    LOGS(_log, LOG_LVL_TRACE, _idStr << "&&&TransmitData::initResult");
    lock_guard<mutex> lock(_trMtx);
    _result->set_queryid(task.getQueryId());
    _result->set_jobid(task.getJobId());
    _result->mutable_rowschema();
    if (task.msg->has_session()) {
        _result->set_session(task.msg->session());
    }
    // If no queries have been run, schemaCols will be empty at this point.
    if (!schemaCols.empty()) {
        _addSchemaCols(schemaCols);
    }
}


bool TransmitData::hasErrormsg() const {
    return _result->has_errormsg();
}


void TransmitData::addSchemaCols(std::vector<SchemaCol>& schemaCols) {
    lock_guard<mutex> lock(_trMtx);
    _addSchemaCols(schemaCols);
}


void TransmitData::_addSchemaCols(std::vector<SchemaCol>& schemaCols) {
    // Load schema from _schemaCols into _result, this should only happen once
    // per TransmitData object.
    LOGS(_log, LOG_LVL_TRACE, _dump() << "&&&TransmitData::_addSchemaCols");
    if (_schemaColsSet.exchange(true) == false) {
        for(auto&& col:schemaCols) {
            proto::ColumnSchema* cs = _result->mutable_rowschema()->add_columnschema();
            cs->set_name(col.colName);
            cs->set_sqltype(col.colSqlType);
            cs->set_mysqltype(col.colMysqlType);
        }
    } else {
        LOGS(_log, LOG_LVL_WARN, _idStr << "TransmitData::_addSchemaCols called multiple times.");
    }
}


bool TransmitData::fillRows(MYSQL_RES* mResult, int numFields, size_t &sz) {
    LOGS(_log, LOG_LVL_TRACE, _idStr << "&&&TransmitData::fillRows");
    lock_guard<mutex> lock(_trMtx);
    LOGS(_log, LOG_LVL_TRACE, _dump() << "&&&TransmitData::fillRows");
    MYSQL_ROW row;

    unsigned int szLimit = std::min(proto::ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT,
                                    proto::ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT);

    while ((row = mysql_fetch_row(mResult))) {
        auto lengths = mysql_fetch_lengths(mResult);
        proto::RowBundle* rawRow = _result->add_row();
        for(int i=0; i < numFields; ++i) {
            if (row[i]) {
                rawRow->add_column(row[i], lengths[i]);
                rawRow->add_isnull(false);
            } else {
                rawRow->add_column();
                rawRow->add_isnull(true);
            }
        }
        _tSize += rawRow->ByteSizeLong();
        sz = _tSize;
        ++_rowCount;

        // Each element needs to be mysql-sanitized
        // Break the loop if the result is too big so this part can be transmitted.
        if (_tSize > szLimit) {
            return false;
        }
    }
    return true;
}


int TransmitData::getResultSize() const {
    lock_guard<mutex> lock(_trMtx);
    return _dataMsg.size();
}


string TransmitData::_dump() const {
    string str = string(" trDump ") + _idStr + " trSeq=" + to_string(_trSeq) + " hdr=";
    if (_header != nullptr) {
        str += to_string(_header->size());
    } else {
        str += "nullptr";
    }
    str += " res=" + to_string(_dataMsg.size());
    return str;
}


}}} // namespace lsst::qserv::wbase

