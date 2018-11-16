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
#ifndef LSST_QSERV_LOADER_CENTRAL_CLIENT_H
#define LSST_QSERV_LOADER_CENTRAL_CLIENT_H

// system headers
#include <thread>
#include <vector>

// third party headers
#include <boost/asio.hpp>
#include <boost/bind.hpp>

// Qserv headers
#include "loader/Central.h"
#include "loader/ClientServer.h"
#include "util/Command.h"


namespace lsst {
namespace qserv {
namespace loader {


/// This class is used to track the status and value of jobs inserting
/// key-value pairs to the system or looking up key-value pairs. The
/// Tracker base class provides a means of notifying other threads
/// that the task is complete.
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

/// This class is 'Central' to the client. The client maintains a UDP port
/// so replies to its request can be sent directly back to it.
/// 'Central' provides access to the master and a DoList for handling requests.
/// TODO Maybe base this on CentralWorker or have a common base class?
class CentralClient : public Central {
public:
    /// The client needs to know the master's IP and its own IP.
    /// TODO The worker IP is temporary as it should be able to get
    ///      that information from the master in the future.
    CentralClient(boost::asio::io_service& ioService_,
                  std::string const& masterHostName, int masterPort,
                  std::string const& workerHostName, int workerPort,
                  std::string const& hostName, int port)
        : Central(ioService_, masterHostName, masterPort),
          _workerHostName(workerHostName), _workerPort(workerPort),
          _hostName(hostName), _udpPort(port) {
        _server = std::make_shared<ClientServer>(ioService, _hostName, _udpPort, this);
    }

    ~CentralClient() override = default;

    std::string const& getHostName() const { return _hostName; }
    int getUdpPort() const { return _udpPort; }
    int getTcpPort() const { return 0; } ///< No tcp port at this time.

    std::string getWorkerHostName() const { return _workerHostName; }
    int getWorkerPort() const { return _workerPort; }


    /// Asynchronously request a key value insert to the workers. It returns a
    /// KeyInfoData object for checking the job's status.
    KeyInfoData::Ptr keyInsertReq(std::string const& key, int chunk, int subchunk);
    /// Handle a workers response to the keyInserReq call
    void handleKeyInsertComplete(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    /// Asynchronously request a key value lookup from the workers. It returns a
    /// KeyInfoData object to be used to track job status and get the value of the key.
    KeyInfoData::Ptr keyInfoReq(std::string const& key);
    /// Handle a workers response to the keyInfoReq call.
    void handleKeyInfo(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    std::string getOurLogId() const override { return "client"; }

private:
    void _keyInsertReq(std::string const& key, int chunk, int subchunk); ///< see keyInsertReq()
    void _handleKeyInsertComplete(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf);

    void _keyInfoReq(std::string const& key); ///< see keyInfoReq()
    void _handleKeyInfo(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf);



    /// Create commands to add a key to the index and track that they are done.
    /// It should keep trying this until it works, and then drop it from _waitingKeyInsertMap.
    struct KeyInsertReqOneShot : public DoListItem {
        using Ptr = std::shared_ptr<KeyInsertReqOneShot>;

        KeyInsertReqOneShot(CentralClient* central_, std::string const& key_, int chunk_, int subchunk_) :
            cmdData(new KeyInfoData(key_, chunk_, subchunk_)), central(central_) { _oneShot = true; }

        util::CommandTracked::Ptr createCommand() override;

        /// TODO Have this function take result codes (such as 'success') as arguments
        /// and put them in cmdData.
        void keyInsertComplete();

        KeyInfoData::Ptr cmdData;
        CentralClient* central;
    };

    /// Create commands to find a key in the index and get its value.
    /// It should keep trying this until it works and then drop it from _waitingKeyInfoMap.
    struct KeyInfoReqOneShot : public DoListItem {
        using Ptr = std::shared_ptr<KeyInfoReqOneShot>;

        KeyInfoReqOneShot(CentralClient* central_, std::string const& key_) :
            cmdData(new KeyInfoData(key_, -1, -1)), central(central_) { _oneShot = true; }

        util::CommandTracked::Ptr createCommand() override;

        // TODO Have this function take result codes as arguments and put them in cmdData.
        void keyInfoComplete(std::string const& key, int chunk, int subchunk, bool success);

        KeyInfoData::Ptr cmdData;
        CentralClient* central;
    };

    const std::string _workerHostName;
    const int         _workerPort;
    const std::string _hostName;
    const int         _udpPort;

    std::map<std::string, KeyInsertReqOneShot::Ptr> _waitingKeyInsertMap;
    std::mutex _waitingKeyInsertMtx; ///< protects _waitingKeyInsertMap

    std::map<std::string, KeyInfoReqOneShot::Ptr> _waitingKeyInfoMap;
    std::mutex _waitingKeyInfoMtx; ///< protects _waitingKeyInfoMap
};

}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_CENTRAL_CLIENT_H
