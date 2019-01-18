// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
#include "loader/CentralClient.h"
#include "loader/ClientConfig.h"

// System headers
#include <iostream>
#include <unistd.h>

// Third-party headers


// qserv headers


// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.appClient");
}

using namespace lsst::qserv::loader;
using  boost::asio::ip::udp;

typedef std::list<KeyInfoData::Ptr> KeyInfoDataList;

/// @return true if the list is empty.
bool keyInsertListClean(KeyInfoDataList& kList, int& successCount, int& failedCount) {
    for(auto iter=kList.begin(); iter != kList.end();) {
        if (*iter == nullptr || (*iter)->isFinished()) {
            KeyInfoData::Ptr const& kPtr = *iter;
            if (kPtr->success) {
                ++successCount;
            } else {
                ++failedCount;
                LOGS(_log, LOG_LVL_WARN, "insert failed " << *kPtr);
            }
            iter = kList.erase(iter);
        } else {
            ++iter;
        }
    }
    return kList.empty();
}


KeyInfoData::Ptr clientAdd(CentralClient& central, uint64_t j) {
    CompositeKey cKey(j);
    int chunk = j%10000;
    int subchunk = j%100;
    return central.keyInsertReq(cKey, chunk, subchunk);
}


/// @return true if the list is empty.
bool keyLookupListClean(KeyInfoDataList& kList, int& successCount, int& failedCount) {
    for(auto iter=kList.begin(); iter != kList.end();) {
        if (*iter == nullptr || (*iter)->isFinished()) {
            KeyInfoData::Ptr const& kPtr = *iter;
            if (kPtr->success) {
                // check the values
                uint64_t j = kPtr->key.kInt;
                // expected chunk and subchunk values.
                int expChunk = j%10000;
                int expSubchunk = j%100;
                if (kPtr->chunk == expChunk && kPtr->subchunk == expSubchunk) {
                    ++successCount;
                } else {
                    ++failedCount;
                    LOGS(_log, LOG_LVL_WARN, "lookup failed bad values expected c=" << expChunk <<
                            " sc=" << expSubchunk << " found=" << *kPtr);
                }
            } else {
                ++failedCount;
                LOGS(_log, LOG_LVL_WARN, "lookup failed " << *kPtr);
            }
            iter = kList.erase(iter);
        } else {
            ++iter;
        }
    }
    return kList.empty();
}


KeyInfoData::Ptr clientAddLookup(CentralClient& central, uint64_t j) {
    CompositeKey cKey(j);
    return central.keyInfoReq(cKey);
}


int main(int argc, char* argv[]) {
    std::string cCfgFile("core/modules/loader/config/client1.cnf");
    if (argc < 3) {
        LOGS(_log, LOG_LVL_ERROR, "usage: appClientNum <startingNumber> <endingNumber> <optional config file name>");
        exit(-1);
    }
    uint64_t numStart = std::stoi(argv[1]);
    uint64_t numEnd   = std::stoi(argv[2]);
    if (argc > 3) {
        cCfgFile = argv[3];
    }
    LOGS(_log, LOG_LVL_INFO, "start=" << numStart << " end=" << numEnd << " cCfg=" << cCfgFile);

    std::string ourHost;
    {
        char hName[300];
        if (gethostname(hName, sizeof(hName)) < 0) {
            LOGS(_log, LOG_LVL_ERROR, "Failed to get host name errno=" << errno);
            exit(-1);
        }
        ourHost = hName;
    }

    boost::asio::io_service ioService;

    ClientConfig cCfg(cCfgFile);
    CentralClient cClient(ioService, ourHost, cCfg);
    try {
        cClient.start();
    } catch (boost::system::system_error const& e) {
        LOGS(_log, LOG_LVL_ERROR, "cWorker.start() failed e=" << e.what());
        exit(-1);
    }

    // Need to start several threads so messages aren't dropped while being processed.
    cClient.run();
    cClient.run();
    cClient.run();

    KeyInfoDataList kList;
    int successCount = 0;
    int failedCount = 0;
    if (numEnd >= numStart) {
        for (uint64_t j=numStart; j<=numEnd; ++j) {
            kList.push_back(clientAdd(cClient, j));
            // occasionally trim the list
            if (j%1000 == 0) keyInsertListClean(kList, successCount, failedCount);
        }
    } else {
        for (uint64_t j=numStart; j>=numEnd; --j) {
            kList.push_back(clientAdd(cClient, j));
            // occasionally trim the list
            if (j%1000 == 0) keyInsertListClean(kList, successCount, failedCount);
        }
    }

    int count = 0;
    // If all the requests are done, the list should be empty.
    // it should be done well before 100 seconds (TODO: maybe 1 second per 1000 keys)
    while (!keyInsertListClean(kList, successCount, failedCount) && count < 100) {
        LOGS(_log, LOG_LVL_INFO, "waiting for inserts to finish count=" << count);
        sleep(1);
        ++count;
    }


    if (!kList.empty()) {
        LOGS(_log, LOG_LVL_WARN, "kList not empty, size=" << kList.size());
        std::stringstream ss;
        for (auto kPtr:kList) {
            ss << "elem=" << *kPtr << "\n";
        }
        LOGS(_log, LOG_LVL_WARN, ss.str());
    }

    if (!kList.empty() || failedCount > 0) {
        LOGS(_log, LOG_LVL_ERROR, "FAILED to insert all elements. success=" << successCount <<
                " failed=" << failedCount << " size=" << kList.size());
        exit(-1);
    }

    LOGS(_log, LOG_LVL_INFO, "inserted all elements. success=" << successCount <<
            " failed=" << failedCount << " size=" << kList.size());


    // Lookup answers
    auto nStart = numStart;
    auto nEnd = numEnd;
    if (nEnd < nStart) {
        nStart = numEnd;
        nEnd = numStart;
    }
    successCount = 0;
    failedCount = 0;
    for (uint64_t j=nStart; j<=nEnd; ++j) {
        kList.push_back(clientAddLookup(cClient, j));
        // occasionally trim the list
        if (j%1000 == 0) keyLookupListClean(kList, successCount, failedCount);
    }

    count = 0;
    // If all the requests are done, the list should be empty.
    // it should be done well before 100 seconds (TODO: maybe 1 second per 1000 keys)
    while (!keyLookupListClean(kList, successCount, failedCount) && count < 100) {
        LOGS(_log, LOG_LVL_INFO, "waiting for lookups to finish count=" << count);
        sleep(1);
        ++count;
    }

    if (!kList.empty()) {
        LOGS(_log, LOG_LVL_WARN, "kList not empty, size=" << kList.size());
        std::stringstream ss;
        for (auto kPtr:kList) {
            ss << "elem=" << *kPtr << "\n";
        }
        LOGS(_log, LOG_LVL_WARN, ss.str());
    }

    if (!kList.empty() || failedCount > 0) {
        LOGS(_log, LOG_LVL_ERROR, "FAILED to lookup all elements. success=" << successCount <<
                " failed=" << failedCount << " size=" << kList.size());
        exit(-1);
    }

    LOGS(_log, LOG_LVL_INFO, "lookup all elements. success=" << successCount <<
            " failed=" << failedCount << " size=" << kList.size());


    ioService.stop(); // this doesn't seem to work cleanly
    LOGS(_log, LOG_LVL_INFO, "client DONE");
}


