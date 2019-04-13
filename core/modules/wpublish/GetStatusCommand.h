// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
#ifndef LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H
#define LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H

// System headers
#include <string>

// Qserv headers
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    class MsgProcessor;
    class SendChannel;
}}}

// This header declarations
namespace lsst {
namespace qserv {
namespace wpublish {

/**
 * Class GetStatusCommand returns various info on the on-going status of
 * a Qserv worker.
 */
class GetStatusCommand : public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    GetStatusCommand& operator=(GetStatusCommand const&) = delete;
    GetStatusCommand(GetStatusCommand const&) = delete;
    GetStatusCommand() = delete;

    /**
     * @param sendChannel  communication channel for reporting results
     * @param processor    message processor for extracting status info
     */
    explicit GetStatusCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                              std::shared_ptr<wbase::MsgProcessor> const& processor);

    ~GetStatusCommand() override = default;

    void run() override;

private:
    
    std::shared_ptr<wbase::MsgProcessor> const _processor;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_GET_STATUS_COMMAND_H
