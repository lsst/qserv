// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_CENTRAL_CLIENT_H_
#define LSST_QSERV_LOADER_CENTRAL_CLIENT_H_

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/Central.h"
#include "loader/ClientServer.h"
#include "util/Command.h"


namespace lsst {
namespace qserv {
namespace loader {

class KeyInfoData : public util::Tracker {
public:
    using Ptr = std::shared_ptr<KeyInfoData>;
    KeyInfoData(std::string const& key_, int chunk_, int subchunk_) :
        key(key_), chunk(chunk_), subchunk(subchunk_) {}

    std::string key;
    int chunk;
    int subchunk;
    bool success{false};

    friend std::ostream& operator<<(std::ostream& os, KeyInfoData const& data);
};

/// TODO Maybe base this one CentralWorker or have a common base class?
class CentralClient : public Central {
public:
    CentralClient(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort,
                  std::string const& workerHostName, int workerPort,
                  std::string const& hostName, int port)
        : Central(ioService, masterHostName, masterPort),
          _workerHostName(workerHostName), _workerPort(workerPort),
          _hostName(hostName), _udpPort(port) {
        _server = std::make_shared<ClientServer>(_ioService, _hostName, _udpPort, this);
    }

    ~CentralClient() override = default;

    std::string getHostName() const { return _hostName; }
    int getUdpPort() const { return _udpPort; }
    int getTcpPort() const { return 0; } ///< No tcp port at this time.

    std::string getWorkerHostName() const { return _workerHostName; }
    int getWorkerPort() const { return _workerPort; }


    KeyInfoData::Ptr keyInsertReq(std::string const& key, int chunk, int subchunk);
    void handleKeyInsertComplete(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    KeyInfoData::Ptr keyInfoReq(std::string const& key);
    void handleKeyInfo(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    std::string getOurLogId() override { return "client"; }

private:
    void _keyInsertReq(std::string const& key, int chunk, int subchunk);
    void _handleKeyInsertComplete(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf);

    void _keyInfoReq(std::string const& key);
    void _handleKeyInfo(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf);

    const std::string _workerHostName;
    const int         _workerPort;
    const std::string _hostName;
    const int         _udpPort;

    /// Create commands to add a key to the index and track that they are done.
    /// It should keep trying this until it works, and then drop it from _waitingKeyInsertMap.
    struct KeyInsertReqOneShot : public DoListItem {
        using Ptr = std::shared_ptr<KeyInsertReqOneShot>;

        KeyInsertReqOneShot(CentralClient* central_, std::string const& key_, int chunk_, int subchunk_) :
            cmdData(new KeyInfoData(key_, chunk_, subchunk_)), central(central_) {
            _oneShot = true;
        }

        KeyInfoData::Ptr cmdData;
        CentralClient* central;

        util::CommandTracked::Ptr createCommand() override {
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

        // TODO Have this function take result codes as arguments and put them in cmdData.
        void keyInsertComplete() {
            cmdData->success = true;
            cmdData->setComplete();
            infoReceived();
        }
    };

    std::map<std::string, KeyInsertReqOneShot::Ptr> _waitingKeyInsertMap;
    std::mutex _waitingKeyInsertMtx; ///< protects _waitingKeyInsertMap


    /// Create commands to find a key in the index and get its value.
    /// It should keep trying this until it works, and then drop it from _waitingKeyInfoMap.
    struct KeyInfoReqOneShot : public DoListItem {
        using Ptr = std::shared_ptr<KeyInfoReqOneShot>;

        KeyInfoReqOneShot(CentralClient* central_, std::string const& key_) :
            cmdData(new KeyInfoData(key_, -1, -1)), central(central_) {
            _oneShot = true;
        }

        KeyInfoData::Ptr cmdData;
        CentralClient* central;

        util::CommandTracked::Ptr createCommand() override {
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

        // TODO Have this function take result codes as arguments and put them in cmdData.
        void keyInfoComplete(std::string const& key, int chunk, int subchunk, bool success) {
            if (key == cmdData->key) {
                cmdData->chunk = chunk;
                cmdData->subchunk = subchunk;
                cmdData->success = success;
            }
            cmdData->setComplete();
            infoReceived();
        }
    };

    std::map<std::string, KeyInfoReqOneShot::Ptr> _waitingKeyInfoMap;
    std::mutex _waitingKeyInfoMtx; ///< protects _waitingKeyInfoMap

};

}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_CLIENT_H_
