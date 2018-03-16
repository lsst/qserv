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
#ifndef LSST_QSERV_LOADER_CLIENTSERVER_H_
#define LSST_QSERV_LOADER_CLIENTSERVER_H_

// system headers
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/LoaderMsg.h"
#include "loader/ServerUdpBase.h"


namespace lsst {
namespace qserv {
namespace loader {

class CentralClient;


class ClientServer : public ServerUdpBase {
public:
    ClientServer(boost::asio::io_service& ioService, std::string const& host, int port, CentralClient* centralClient)
        : ServerUdpBase(ioService, host, port), _centralClient(centralClient) {}

    ClientServer() = delete;

    ~ClientServer() override = default;

    BufferUdp::Ptr parseMsg(BufferUdp::Ptr const& data,
                            boost::asio::ip::udp::endpoint const& endpoint) override;

    BufferUdp::Ptr replyMsgReceived(boost::asio::ip::udp::endpoint const& senderEndpoint,
                                    LoaderMsg const& inMsg,
                                    int status, std::string const& msgTxt); // TODO shows up in both MasterServer and WorkerServer

    //void keyInsert(std::string const& key, int chunk, int subchunk); &&&
    // KeyValue keyLookup(std::string const& key); &&&

private:
    void _msgRecieved(LoaderMsg const& inMsg, BufferUdp::Ptr const& data,
                      boost::asio::ip::udp::endpoint const& senderEndpoint);


    CentralClient* _centralClient;
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_CLIENTSERVER_H_
