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

// System headers
#include <iostream>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/CentralClient.h"
#include "loader/CentralMaster.h"
#include "loader/CentralWorker.h"
#include "loader/LoaderMsg.h"
#include "loader/MasterServer.h"
#include "loader/WorkerServer.h"
#include "loader/ServerTcpBase.h"
#include "proto/loader.pb.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.test");
}

using namespace lsst::qserv::loader;
using  boost::asio::ip::udp;

struct KeyChSch {
    KeyChSch(std::string const& k, int c, int sc) : key(k), chunk(c), subchunk(sc) {}
    std::string key;
    int chunk;
    int subchunk;
};


int main(int argc, char* argv[]) {
    UInt16Element num16(1 | 2 << 8);
    uint16_t origin16 = num16.element;
    uint16_t net16  = num16.changeEndianessOnLittleEndianOnly(num16.element);
    uint16_t host16 = num16.changeEndianessOnLittleEndianOnly(net16);
    LOGS(_log, LOG_LVL_INFO,  "origin16=" << origin16 << " hex=" << std::hex << origin16);
    LOGS(_log, LOG_LVL_INFO,  "net16=" << net16 << " hex=" << std::hex << net16);
    LOGS(_log, LOG_LVL_INFO,  "host16=" << host16 << " hex=" << std::hex << host16);
    if (host16 != origin16) {
        LOGS(_log, LOG_LVL_ERROR, "UInt16NumElement did match host=" << host16 << " orig=" << origin16);
        exit(-1);
    } else {
        LOGS(_log, LOG_LVL_INFO, "UInt16NumElement match host=origin=" << host16);
    }

    UInt32Element num32(1 | 2 << 8 | 3 << 16 | 4 << 24);
    uint32_t origin32 = num32.element;
    uint32_t net32  = num32.changeEndianessOnLittleEndianOnly(num32.element);
    uint32_t host32 = num32.changeEndianessOnLittleEndianOnly(net32);
    LOGS(_log, LOG_LVL_INFO,  "origin32=" << origin32 << " hex=" << std::hex << origin32);
    LOGS(_log, LOG_LVL_INFO,  "net32=" << net32 << " hex=" << std::hex << net32);
    LOGS(_log, LOG_LVL_INFO,  "host32=" << host32 << " hex=" << std::hex << host32);
    if (host32 != origin32) {
        LOGS(_log, LOG_LVL_ERROR, "UInt32NumElement did match host=" << host32 << " orig=" << origin32);
        exit(-1);
    } else {
        LOGS(_log, LOG_LVL_INFO, "UInt32NumElement match host=origin=" << host32);
    }


    uint64_t testVal = 0;
    for (uint64_t j=0; j < 8; ++j) {
        testVal |= (j + 1) << (8*j);
    }
    UInt64Element num64(testVal);
    uint64_t origin64 = num64.element;
    uint64_t net64  = num64.changeEndianessOnLittleEndianOnly(num64.element);
    uint64_t host64 = num64.changeEndianessOnLittleEndianOnly(net64);
    LOGS(_log, LOG_LVL_INFO,  "origin64=" << origin64 << " hex=" << std::hex << origin64);
    LOGS(_log, LOG_LVL_INFO,  "net64=" << net64 << " hex=" << std::hex << net64);
    LOGS(_log, LOG_LVL_INFO,  "host64=" << host64 << " hex=" << std::hex << host64);
    if (host64 != origin64) {
        LOGS(_log, LOG_LVL_ERROR, "UInt64NumElement did match host=" << host64 << " orig=" << origin64);
        return -1;
    } else {
        LOGS(_log, LOG_LVL_INFO, "UInt64NumElement match host=origin=" << host64);
    }


    std::vector<MsgElement::Ptr> elements;
    elements.push_back(std::make_shared<StringElement>("Simple"));
    elements.push_back(std::make_shared<StringElement>(""));
    elements.push_back(std::make_shared<StringElement>(" :lakjserhrfjb;iouha93219876$%#@#\n$%^ #$#%R@##$@@@@$kjhdghrnfgh  "));
    elements.push_back(std::make_shared<UInt16Element>(25027));
    elements.push_back(std::make_shared<UInt32Element>(338999));
    elements.push_back(std::make_shared<UInt64Element>(1234567));
    elements.push_back(std::make_shared<StringElement>("One last string."));

    BufferUdp data;

    // Write to the buffer.
    try {
        std::stringstream os;
        for (auto& ele : elements) {
            if (not ele->appendToData(data)) {
                throw LoaderMsgErr(ERR_LOC, "Failed to append " + ele->getStringVal() +
                        " data:" + data.dumpStr());
            }
        }
        LOGS(_log, LOG_LVL_INFO, "data:" << data.dumpStr());
    } catch (LoaderMsgErr& ex) {
        LOGS(_log, LOG_LVL_ERROR, "Write to buffer FAILED msg=" << ex.what());
        exit(-1);
    }
    LOGS(_log, LOG_LVL_INFO, "Done writing to buffer.");

    std::vector<MsgElement> outElems;
    // Read from the buffer.
    try {
        for (auto& ele : elements) {
            // check all elements
            char elemType = MsgElement::NOTHING;
            if (not MsgElement::retrieveType(data, elemType)) {
                throw LoaderMsgErr(ERR_LOC, "Type was expected but not found!" + data.dumpStr());
            }
            MsgElement::Ptr outEle = MsgElement::create(elemType);
            if (not outEle->retrieveFromData(data)) {
                throw LoaderMsgErr(ERR_LOC, "Failed to retrieve elem=" + outEle->getStringVal() +
                                   " data:" + data.dumpStr());
            }
            if (!MsgElement::equal(ele.get(), outEle.get())) {
                LOGS(_log, LOG_LVL_ERROR,
                        "FAILED " << ele->getStringVal() << " != " << outEle->getStringVal());
                exit(-1);
            } else {
                LOGS(_log, LOG_LVL_INFO, "matched " << ele->getStringVal());
            }
        }
    } catch (LoaderMsgErr& ex) {
        LOGS(_log, LOG_LVL_ERROR, "Read from buffer FAILED msg=" << ex.what());
        exit(-1);
    }

    //////////////////////////////////////////////////////////////////////////////


    // test for LoaderMsg serialize and parse
    LoaderMsg lMsg(LoaderMsg::MAST_INFO_REQ, 1, "127.0.0.1", 9876);
    BufferUdp lBuf;
    lMsg.appendToData(lBuf);
    {
        LoaderMsg outMsg;
        outMsg.parseFromData(lBuf);
        if (  lMsg.msgKind->element    != outMsg.msgKind->element    ||
                lMsg.msgId->element      != outMsg.msgId->element      ||
                lMsg.senderHost->element != outMsg.senderHost->element ||
                lMsg.senderPort->element != outMsg.senderPort->element) {
            LOGS(_log, LOG_LVL_ERROR,
                    "FAILED messages didn't match out:" << outMsg.getStringVal() <<
                    " != lMsg" << lMsg.getStringVal());
            return -1;
        } else {
            LOGS(_log, LOG_LVL_INFO, "msgs matched " << outMsg.getStringVal());
        }
    }


    {
        try {
            LOGS(_log, LOG_LVL_INFO, "ServTcpBase a");
            boost::asio::io_context io_context;
            LOGS(_log, LOG_LVL_INFO, "ServTcpBase b");
            ServerTcpBase server(io_context, 1041);
            LOGS(_log, LOG_LVL_INFO, "ServTcpBase c");
            server.runThread();
            LOGS(_log, LOG_LVL_INFO, "ServTcpBase d");

            server.testConnect();
            LOGS(_log, LOG_LVL_INFO, "ServTcpBase e");
            sleep(5);
        }
        catch (std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
    }


    ////////////////////////////////////////////////////////////////////////////

    /// Start a master server
    std::string masterIP = "127.0.0.1";
    int masterPort = 10042;
    boost::asio::io_service ioServiceMaster;

    std::string worker1IP = "127.0.0.1";
    int worker1Port = 10043;
    int worker1TcpPort = 10143;
    boost::asio::io_service ioServiceWorker1;
    boost::asio::io_context ioContext1;

    std::string worker2IP = "127.0.0.1";
    int worker2Port = 10044;
    int worker2TcpPort = 10144;
    boost::asio::io_service ioServiceWorker2;
    boost::asio::io_context ioContext2;

    std::string client1AIP = "127.0.0.1";
    int client1APort = 10050;
    boost::asio::io_service ioServiceClient1A;

    std::string client1BIP = "127.0.0.1";
    int client1BPort = 10051;
    boost::asio::io_service ioServiceClient1B;

    std::string client2AIP = "127.0.0.1";
    int client2APort = 10053;
    boost::asio::io_service ioServiceClient2A;


    CentralMaster cMaster(ioServiceMaster, masterIP, masterPort);
    cMaster.setMaxKeysPerWorker(4);
    // Need to start several threads so messages aren't dropped while being processed.
    cMaster.run();
    cMaster.run();
    cMaster.run();
    cMaster.run();
    cMaster.run();

    /// Start worker server 1
    CentralWorker wCentral1(ioServiceWorker1, masterIP, masterPort, worker1IP, worker1Port, ioContext1, worker1TcpPort);
    wCentral1.run();
    wCentral1.run();
    wCentral1.run();


    /// Start worker server 2
    CentralWorker wCentral2(ioServiceWorker2, masterIP, masterPort, worker2IP, worker2Port, ioContext2, worker2TcpPort);
    wCentral2.run();
    wCentral2.run();
    wCentral2.run();


    CentralClient cCentral1A(ioServiceClient1A, masterIP, masterPort, worker1IP, worker1Port, client1AIP, client1APort);
    cCentral1A.run();

    CentralClient cCentral1B(ioServiceClient1B, masterIP, masterPort, worker1IP, worker1Port, client1BIP, client1BPort);
    cCentral1B.run();

    CentralClient cCentral2A(ioServiceClient1A, masterIP, masterPort, worker2IP, worker2Port, client2AIP, client2APort);
    cCentral2A.run();


    /// Unknown message kind test. Pretending to be worker1.
    {
        auto originalErrCount = wCentral1.getErrCount();
        LOGS(_log, LOG_LVL_INFO, "1TSTAGE testSendBadMessage start");
        wCentral1.testSendBadMessage();
        sleep(2); // TODO handshaking instead of sleep

        if (originalErrCount == wCentral1.getErrCount()) {
            LOGS(_log, LOG_LVL_ERROR, "testSendBadMessage errCount did not change " << originalErrCount);
            exit(-1);
        }
    }

    LOGS(_log, LOG_LVL_INFO, "sleeping");
    sleep(5); // TODO change to 20 second timeout with a check every 0.1 seconds.
    // The workers should agree on the worker list, and it should have 2 elements.
    if (wCentral1.getWorkerList()->getNameMapSize() == 0) {
        LOGS(_log, LOG_LVL_ERROR, "ERROR Worker list is empty!!!");
        exit(-1);
    }
    LOGS(_log, LOG_LVL_INFO, "MasterList " << cMaster.getWorkerList()->dump());
    LOGS(_log, LOG_LVL_INFO, "List1 " << wCentral1.getWorkerList()->dump());
    LOGS(_log, LOG_LVL_INFO, "List2 " << wCentral2.getWorkerList()->dump());
    if (not wCentral1.getWorkerList()->equal(*(wCentral2.getWorkerList()))) {
        LOGS(_log, LOG_LVL_ERROR, "ERROR Worker lists do not match!!!");
        exit(-1);
    } else {
        LOGS(_log, LOG_LVL_INFO, "Worker lists match.");
    }


    /// Client
    LOGS(_log, LOG_LVL_INFO, "3TSTAGE client register key A");
    KeyChSch keyA("asdf_1", 4001, 200001);
    auto keyAInsert = cCentral1A.keyInsertReq(keyA.key, keyA.chunk, keyA.subchunk);

    LOGS(_log, LOG_LVL_INFO, "4TSTAGE client register key B");;
    KeyChSch keyB("ndjes_bob", 9871, 65008);
    auto keyBInsert = cCentral1B.keyInsertReq(keyB.key, keyB.chunk, keyB.subchunk);

    KeyChSch keyC("asl_diebb", 422001, 7373721);

    size_t arraySz = 1000;
    std::vector<KeyChSch> keyList;
    {
        std::string bStr("a");
        for (size_t j=0; j<arraySz; ++j) {
            std::string reversed(bStr.rbegin(), bStr.rend());
            LOGS(_log, LOG_LVL_INFO, bStr << " newKey=" << reversed << " j(" << j%10 << " ," << j << ")");
            keyList.emplace_back(reversed, j%10, j);
            bStr = StringRange::incrementString(bStr, '0');
        }
    }

    std::vector<KeyChSch> keyListB;
    {
        for (size_t j=0; j<100000; ++j) {
            std::string str("z");
            str += std::to_string(j);
            keyListB.emplace_back(str, j%10, j);
        }
    }

    // retrieve keys keyA and keyB
    sleep(2); // need to sleep as it never gives up on inserts.
    if (keyAInsert->isFinished() && keyBInsert->isFinished()) {
        LOGS(_log, LOG_LVL_INFO, "both keyA and KeyB inserted.");
    } else {
        LOGS(_log, LOG_LVL_INFO, "\nkeyA and KeyB insert something did not finish");
        exit(-1);
    }

    // Retrieve keyA and keyB
    {
        LOGS(_log, LOG_LVL_INFO, "5TSTAGE client retrieve keyB keyA");
        auto keyBInfo = cCentral1A.keyInfoReq(keyB.key);
        auto keyAInfo = cCentral1A.keyInfoReq(keyA.key);
        auto keyCInfo = cCentral1A.keyInfoReq(keyC.key);

        keyAInfo->waitComplete();
        keyBInfo->waitComplete();
        LOGS(_log, LOG_LVL_INFO, "5TSTAGE client retrieve DONE keyB keyA");
        LOGS(_log, LOG_LVL_INFO, "looked up keyA " << *keyAInfo);
        LOGS(_log, LOG_LVL_INFO, "looked up keyB " << *keyBInfo);

        keyCInfo->waitComplete();
        LOGS(_log, LOG_LVL_INFO, "looked up (expect to fail) keyC " << *keyCInfo);

        if (keyAInfo->key != keyA.key || keyAInfo->chunk != keyA.chunk || keyAInfo->subchunk != keyA.subchunk || !keyAInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyA lookup got incorrect value " << *keyAInfo);
            exit(-1);
        }
        if (keyBInfo->key != keyB.key || keyBInfo->chunk != keyB.chunk || keyBInfo->subchunk != keyB.subchunk || !keyBInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyB lookup got incorrect value " << *keyBInfo);
            exit(-1);
        }
        if (keyCInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyC lookup got incorrect value " << *keyCInfo);
            exit(-1);
        }
    }



    // Add item to worker 2, test retrieval
    {
        LOGS(_log, LOG_LVL_INFO, "6TSTAGE client insert keyC lookup all keys");
        auto keyCInsert = cCentral2A.keyInsertReq(keyC.key, keyC.chunk, keyC.subchunk);
        sleep(2); // need to sleep as it never gives up on inserts.
        if (keyCInsert->isFinished()) {
            LOGS(_log, LOG_LVL_INFO, "keyC inserted.");
        }

        auto keyAInfo = cCentral1A.keyInfoReq(keyA.key);
        LOGS(_log, LOG_LVL_INFO, "6TSTAGE waiting A");
        keyAInfo->waitComplete();

        auto keyBInfo = cCentral2A.keyInfoReq(keyB.key);
        LOGS(_log, LOG_LVL_INFO, "6TSTAGE waiting B");
        keyBInfo->waitComplete();

        auto keyCInfo = cCentral2A.keyInfoReq(keyC.key);
        LOGS(_log, LOG_LVL_INFO, "6TSTAGE waiting C");
        keyCInfo->waitComplete();

        LOGS(_log, LOG_LVL_INFO, "6TSTAGE done waiting");
        if (keyAInfo->key != keyA.key || keyAInfo->chunk != keyA.chunk || keyAInfo->subchunk != keyA.subchunk || !keyAInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyA lookup got incorrect value " << *keyAInfo);
            exit(-1);
        }
        if (keyBInfo->key != keyB.key || keyBInfo->chunk != keyB.chunk || keyBInfo->subchunk != keyB.subchunk || !keyBInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyB lookup got incorrect value " << *keyBInfo);
            exit(-1);
        }
        if (keyCInfo->key != keyC.key || keyCInfo->chunk != keyC.chunk || keyCInfo->subchunk != keyC.subchunk || !keyCInfo->success) {
            LOGS(_log, LOG_LVL_ERROR, "keyC lookup got incorrect value " << *keyCInfo);
            exit(-1);
        }
    }


    size_t kPos = 0;
    {
        LOGS(_log, LOG_LVL_INFO, "7TSTAGE insert several keys");
        std::vector<KeyInfoData::Ptr> keyInfoDataList;

        for (; kPos<10; ++kPos) {
            auto& elem = keyList[kPos];
            keyInfoDataList.push_back(cCentral1A.keyInsertReq(elem.key, elem.chunk, elem.subchunk));
        }

        sleep(2); // need to sleep as it never gives up on inserts.
        bool insertSuccess = true;
        for(auto&& kiData : keyInfoDataList) {
            if (not kiData->isFinished()) {
                insertSuccess = false;
            }
        }

        if (insertSuccess) {
            LOGS(_log, LOG_LVL_INFO, "insert success kPos=" << kPos);
        } else {
            LOGS(_log, LOG_LVL_ERROR, "insert failure kPos=" << kPos);
            exit(-1);
        }

        // The number of active servers should have increased from 1 to 2
        // TODO check number of servers
    }


    {
        LOGS(_log, LOG_LVL_INFO, "8TSTAGE insert several keys");
        std::list<KeyInfoData::Ptr> keyInfoDataList;

        for (; kPos<keyList.size(); ++kPos) {
            auto& elem = keyList[kPos];
            keyInfoDataList.push_back(cCentral1A.keyInsertReq(elem.key, elem.chunk, elem.subchunk));
        }

        bool insertSuccess = true;
        int seconds = 0;
        int finished = 0;
        do {
            sleep(1);
            ++seconds;
            insertSuccess = true;
            for(auto iter = keyInfoDataList.begin(); iter != keyInfoDataList.end();) {
                auto keyIter = iter;
                ++iter;
                if (not (*keyIter)->isFinished()) {
                    insertSuccess = false;
                } else {
                    keyInfoDataList.erase(keyIter);
                    ++finished;
                }
            }
            LOGS(_log, LOG_LVL_INFO, "seconds=" << seconds << " finished=" << finished <<
                    " insertSuccess=" << insertSuccess);
        } while (not insertSuccess);

        if (insertSuccess) {
            LOGS(_log, LOG_LVL_INFO, "keyList insert success kPos=" << kPos << " sec=" << seconds);
        } else {
            LOGS(_log, LOG_LVL_ERROR, "keyList insert failure kPos=" << kPos << " sec=" << seconds);
            exit(-1);
        }

        // TODO check number of servers

    }

    {
        LOGS(_log, LOG_LVL_INFO, "9TSTAGE insert many keys");
        std::list<KeyInfoData::Ptr> keyInfoDataList;
        size_t pos = 0;
        for (; pos<keyListB.size(); ++pos) {
            auto& elem = keyListB[pos];
            keyInfoDataList.push_back(cCentral1A.keyInsertReq(elem.key, elem.chunk, elem.subchunk));
        }

        bool insertSuccess = true;
        int seconds = 0;
        int finished = 0;
        do {
            sleep(1);
            ++seconds;
            insertSuccess = true;
            for(auto iter = keyInfoDataList.begin(); iter != keyInfoDataList.end();) {
                auto keyIter = iter;
                ++iter;
                if (not (*keyIter)->isFinished()) {
                    insertSuccess = false;
                } else {
                    keyInfoDataList.erase(keyIter);
                    ++finished;
                }
            }
            LOGS(_log, LOG_LVL_INFO, "seconds=" << seconds << " finished=" << finished <<
                    " insertSuccess=" << insertSuccess << " " <<  keyInfoDataList.size());
        } while (not insertSuccess);

        if (insertSuccess) {
            LOGS(_log, LOG_LVL_INFO, "keyListB insert success pos=" << pos << " sec=" << seconds);
        } else {
            LOGS(_log, LOG_LVL_ERROR, "keyListB insert failure pos=" << pos << " sec=" << seconds);
            exit(-1);
        }

        // TODO check number of servers

    }


    //ioService.stop(); // this doesn't seem to work cleanly
    // mastT.join();

    sleep(10);
    LOGS(_log, LOG_LVL_INFO, "DONE");
    exit(0);
}
