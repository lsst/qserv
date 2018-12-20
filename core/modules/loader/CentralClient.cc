// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
#include "CentralClient.h"
#include "ClientConfig.h"

// system headers
#include <iostream>

// Third-party headers
#include <boost/asio.hpp>

// qserv headers
#include "loader/LoaderMsg.h"
#include "proto/loader.pb.h"
#include "proto/ProtoImporter.h"

// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.CentralClient");
}

namespace lsst {
namespace qserv {
namespace loader {

CentralClient::CentralClient(boost::asio::io_service& ioService_,
                             std::string const& hostName, ClientConfig const& cfg)
    : Central(ioService_, cfg.getMasterHost(), cfg.getMasterPortUdp(), cfg.getThreadPoolSize(), cfg.getLoopSleepTime()),
      _hostName(hostName), _udpPort(cfg.getClientPortUdp()),
      _defWorkerHost(cfg.getDefWorkerHost()),
      _defWorkerPortUdp(cfg.getDefWorkerPortUdp()),
      _doListMaxLookups(cfg.getMaxLookups()),
      _doListMaxInserts(cfg.getMaxInserts()) {
}


void CentralClient::start() {
    _server = std::make_shared<ClientServer>(ioService, _hostName, _udpPort, this);
}


void CentralClient::handleKeyInfo(LoaderMsg const& inMsg, BufferUdp::Ptr const& data) {
    LOGS(_log, LOG_LVL_DEBUG, "CentralClient::handleKeyInfo");

    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralClient::handleKeyInsertComplete Failed to parse list");
        return;
    }
    auto protoData = sData->protoParse<proto::KeyInfo>();
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralClient::handleKeyInsertComplete Failed to parse list");
        return;
    }

    // TODO put in separate thread
    _handleKeyInfo(inMsg, protoData);
}


void CentralClient::_handleKeyInfo(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf) {
    std::unique_ptr<proto::KeyInfo> protoData(std::move(protoBuf));

    std::string key = protoData->key();
    ChunkSubchunk chunkInfo(protoData->chunk(), protoData->subchunk());

    LOGS(_log, LOG_LVL_INFO, "trying to remove oneShot for lookup key=" << key << " " << chunkInfo);
    /// Locate the original one shot and mark it as done.
    CentralClient::KeyInfoReqOneShot::Ptr keyInfoOneShot;
    {
        std::lock_guard<std::mutex> lck(_waitingKeyInfoMtx);
        auto iter = _waitingKeyInfoMap.find(key);
        if (iter == _waitingKeyInfoMap.end()) {
            LOGS(_log, LOG_LVL_WARN, "handleKeyInfoComplete could not find key=" << key);
            return;
        }
        keyInfoOneShot = iter->second;
        _waitingKeyInfoMap.erase(iter);
    }
    keyInfoOneShot->keyInfoComplete(key, chunkInfo.chunk, chunkInfo.subchunk, protoData->success());
    LOGS(_log, LOG_LVL_INFO, "Successfully found key=" << key << " " << chunkInfo);
}


void CentralClient::handleKeyInsertComplete(LoaderMsg const& inMsg, BufferUdp::Ptr const& data) {
    LOGS(_log, LOG_LVL_DEBUG, "CentralClient::handleKeyInsertComplete");

    auto sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralClient::handleKeyInsertComplete Failed to retrieve element");
        return;
    }
    auto protoData = sData->protoParse<proto::KeyInfo>();
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralClient::handleKeyInsertComplete Failed to parse list");
        return;
    }

    // TODO put in separate thread
    _handleKeyInsertComplete(inMsg, protoData);
}


void CentralClient::_handleKeyInsertComplete(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf) {
    std::unique_ptr<proto::KeyInfo> protoData(std::move(protoBuf));

    std::string key = protoData->key();
    ChunkSubchunk chunkInfo(protoData->chunk(), protoData->subchunk());

    LOGS(_log, LOG_LVL_DEBUG, "trying to remove oneShot for key=" << key << " " << chunkInfo);
    /// Locate the original one shot and mark it as done.
    CentralClient::KeyInsertReqOneShot::Ptr keyInsertOneShot;
    size_t mapSize;
    {
        std::lock_guard<std::mutex> lck(_waitingKeyInsertMtx);
        mapSize = _waitingKeyInsertMap.size();
        auto iter = _waitingKeyInsertMap.find(key);
        if (iter == _waitingKeyInsertMap.end()) {
            LOGS(_log, LOG_LVL_WARN, "handleKeyInsertComplete could not find key=" << key);
            return;
        }
        keyInsertOneShot = iter->second;
        _waitingKeyInsertMap.erase(iter);
    }
    keyInsertOneShot->keyInsertComplete();
    LOGS(_log, LOG_LVL_INFO, "Successfully inserted key=" << key << " " << chunkInfo <<
                             " mapSize=" << mapSize);
}


/// Returns a pointer to a Tracker object that can be used to track job
//  completion and the status of the job. keyInsertOneShot will call
//  _keyInsertReq until it knows the task was completed via a call
//  to _handleKeyInsertComplete
KeyInfoData::Ptr CentralClient::keyInsertReq(std::string const& key, int chunk, int subchunk) {
    // Insert a oneShot DoListItem to keep trying to add the key until
    // we get word that it has been added successfully.
    LOGS(_log, LOG_LVL_INFO, "Trying to insert key=" << key << " chunk=" << chunk <<
                             " subchunk=" << subchunk);
    auto keyInsertOneShot = std::make_shared<CentralClient::KeyInsertReqOneShot>(this, key, chunk, subchunk);
    {
        std::unique_lock<std::mutex> lck(_waitingKeyInsertMtx);
        // Limit the number of concurrent inserts.
        // If the key is already in the map, there is no point in blocking.
        int loopCount = 0;
        auto iter = _waitingKeyInsertMap.find(key);
        while (_waitingKeyInsertMap.size() > _doListMaxInserts
               && iter == _waitingKeyInsertMap.end()) {
            size_t sz = _waitingKeyInsertMap.size();
            lck.unlock();
            if (loopCount % 100 == 0) {
                LOGS(_log, LOG_LVL_INFO, "keyInsertReq waiting key=" << key <<
                        "size=" << sz << " loopCount=" << loopCount);
            }
            usleep(_maxRequestSleepTime);
            ++loopCount;
            lck.lock();
            iter = _waitingKeyInsertMap.find(key);
        }

        if (iter != _waitingKeyInsertMap.end()) {
            // There is already an entry in the map and we can just use the existing entry,
            // as long as it has the same chunk and subchunk numbers.
            auto cData = iter->second->cmdData;
            if (cData->chunk == chunk && cData->subchunk == subchunk) {
                return cData;
            } else {
                // TODO This MUST go to some form of output for the end user as it is an input data error
                //      either here or when the caller gets a nullptr response
                LOGS(_log, LOG_LVL_ERROR, "key:value does not match existing key:value key=" << key <<
                                          " orignal(" << cData->chunk << "," << cData->subchunk <<
                                          ") new(" << chunk << "," << subchunk << ")");
                return nullptr;
            }
        }
        // The key wasn't found and needs to be inserted.
        _waitingKeyInsertMap[key] = keyInsertOneShot;
    }
    runAndAddDoListItem(keyInsertOneShot);
    return keyInsertOneShot->cmdData;
}


void CentralClient::_keyInsertReq(std::string const& key, int chunk, int subchunk) {
    LOGS(_log, LOG_LVL_INFO, "CentralClient::_keyInsertReq trying key=" << key);
    LoaderMsg msg(LoaderMsg::KEY_INSERT_REQ, getNextMsgId(), getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.appendToData(msgData);
    // create the proto buffer
    lsst::qserv::proto::KeyInfoInsert protoKeyInsert;
    lsst::qserv::proto::LdrNetAddress* protoAddr =  protoKeyInsert.mutable_requester();
    protoAddr->set_ip(getHostName());
    protoAddr->set_udpport(getUdpPort());
    protoAddr->set_tcpport(getTcpPort());
    lsst::qserv::proto::KeyInfo* protoKeyInfo = protoKeyInsert.mutable_keyinfo();
    protoKeyInfo->set_key(key);
    protoKeyInfo->set_chunk(chunk);
    protoKeyInfo->set_subchunk(subchunk);
    protoKeyInsert.set_hops(0);

    StringElement strElem;
    protoKeyInsert.SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    sendBufferTo(getDefWorkerHost(), getDefWorkerPortUdp(), msgData);
}


KeyInfoData::Ptr CentralClient::keyInfoReq(std::string const& key) {
    // Returns a pointer to a Tracker object that can be used to track job
    // completion and job status. keyInsertOneShot will call _keyInsertReq until
    // it knows the task was completed. _handleKeyInfoComplete marks
    // the jobs complete as the messages come in from workers.
    // Insert a oneShot DoListItem to keep trying to add the key until
    // we get word that it has been added successfully.
    LOGS(_log, LOG_LVL_INFO, "Trying to lookup key=" << key);
    auto keyInfoOneShot = std::make_shared<CentralClient::KeyInfoReqOneShot>(this, key);
    {
        std::unique_lock<std::mutex> lck(_waitingKeyInfoMtx);
        // Limit the number of concurrent lookups.
        // If the key is already in the map, there is no point in blocking.
        int loopCount = 0;
        auto iter = _waitingKeyInfoMap.find(key);
        while (_waitingKeyInfoMap.size() > _doListMaxLookups
               && iter == _waitingKeyInfoMap.end()) {
            size_t sz = _waitingKeyInfoMap.size();
            lck.unlock();
            if (loopCount % 100 == 0) {
                LOGS(_log, LOG_LVL_INFO, "keyInfoReq waiting key=" << key <<
                        "size=" << sz << " loopCount=" << loopCount);
            }
            usleep(_maxRequestSleepTime);
            ++loopCount;
            lck.lock();
            iter = _waitingKeyInfoMap.find(key);
        }

        // Use the existing lookup, if there is one.
        if (iter != _waitingKeyInfoMap.end()) {
            auto cData = iter->second->cmdData;
            return cData;
        }

        _waitingKeyInfoMap[key] = keyInfoOneShot;
    }
    runAndAddDoListItem(keyInfoOneShot);
    return keyInfoOneShot->cmdData;
}


void CentralClient::_keyInfoReq(std::string const& key) {
    LOGS(_log, LOG_LVL_INFO, "CentralClient::_keyInfoReq trying key=" << key);
     LoaderMsg msg(LoaderMsg::KEY_INFO_REQ, getNextMsgId(), getHostName(), getUdpPort());
     BufferUdp msgData;
     msg.appendToData(msgData);
     // create the proto buffer
     lsst::qserv::proto::KeyInfoInsert protoKeyInsert;
     lsst::qserv::proto::LdrNetAddress* protoAddr =  protoKeyInsert.mutable_requester();
     protoAddr->set_ip(getHostName());
     protoAddr->set_udpport(getUdpPort());
     protoAddr->set_tcpport(getTcpPort());
     lsst::qserv::proto::KeyInfo* protoKeyInfo = protoKeyInsert.mutable_keyinfo();
     protoKeyInfo->set_key(key);
     protoKeyInfo->set_chunk(0);
     protoKeyInfo->set_subchunk(0);
     protoKeyInsert.set_hops(0);

     StringElement strElem;
     protoKeyInsert.SerializeToString(&(strElem.element));
     strElem.appendToData(msgData);

     sendBufferTo(getDefWorkerHost(), getDefWorkerPortUdp(), msgData);
}


std::ostream& operator<<(std::ostream& os, KeyInfoData const& data) {
    os << "key=" << data.key << "(" << data.chunk << "," << data.subchunk << ") " <<
          "success=" << data.success;
    return os;
}


util::CommandTracked::Ptr CentralClient::KeyInsertReqOneShot::createCommand() {
    struct KeyInsertReqCmd : public util::CommandTracked {
        KeyInsertReqCmd(KeyInfoData::Ptr& cd, CentralClient* cent_) : cData(cd), cent(cent_) {}
        void action(util::CmdData*) override {
            cent->_keyInsertReq(cData->key, cData->chunk, cData->subchunk);
        }
        KeyInfoData::Ptr cData;
        CentralClient* cent;
    };
    return std::make_shared<KeyInsertReqCmd>(cmdData, central);
}


void CentralClient::KeyInsertReqOneShot::keyInsertComplete() {
    cmdData->success = true;
    cmdData->setComplete();
    infoReceived();
}


util::CommandTracked::Ptr CentralClient::KeyInfoReqOneShot::createCommand()  {
    struct KeyInfoReqCmd : public util::CommandTracked {
        KeyInfoReqCmd(KeyInfoData::Ptr& cd, CentralClient* cent_) : cData(cd), cent(cent_) {}
        void action(util::CmdData*) override {
            cent->_keyInfoReq(cData->key);
        }
        KeyInfoData::Ptr cData;
        CentralClient* cent;
    };
    return std::make_shared<KeyInfoReqCmd>(cmdData, central);
}


void CentralClient::KeyInfoReqOneShot::keyInfoComplete(std::string const& key,
                                                       int chunk, int subchunk, bool success) {
    if (key == cmdData->key) {
        cmdData->chunk = chunk;
        cmdData->subchunk = subchunk;
        cmdData->success = success;
    }
    cmdData->setComplete();
    infoReceived();
}


}}} // namespace lsst::qserv::loader
